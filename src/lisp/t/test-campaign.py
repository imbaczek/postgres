#!/usr/bin/env python3
"""Run serialized strict jobs from frozen source with private reusable FASLs.

Each job gates image identity and suite inventory before executing selected heads.
"""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import time

RAM = Path('/dev/shm') / ('pg-lisp-tests-' + str(os.getuid()))
GENERATED = {'.fasl', '.so', '.o', '.core', '.pyc'}
ENVIRONMENT = ('SBCL', 'SBCL_HOME', 'CL_SOURCE_REGISTRY', 'ASDF_SOURCE_REGISTRY',
               'ASDF_OUTPUT_TRANSLATIONS', 'QUICKLISP_HOME', 'PG_CONFIG', 'PG_LIBPQ_HELPER',
               'PG_LISP_TEST_COMPILE_POLICY')


def job_root(path=None):
    value = Path(path or os.environ['PG_LISP_TEST_RUN_DIR'])
    if value != value.resolve() or value.parent != RAM / 'runs' or not re.fullmatch(r'run\.[A-Za-z0-9]+', value.name):
        raise ValueError('Expected an owned RAM test run directory')
    info = value.stat()
    if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError('RAM test run is not private and owned')
    mount = subprocess.check_output(['findmnt', '-rn', '-M', str(RAM), '-o', 'FSTYPE,OPTIONS'], text=True).split()
    if len(mount) != 2 or mount[0] != 'tmpfs' or 'noswap' not in mount[1].split(','):
        raise ValueError('Test campaign requires the unswappable RAM workspace')
    actual_mount = subprocess.check_output(['findmnt', '-rn', '-T', str(value), '-o', 'TARGET'], text=True).strip()
    if actual_mount != str(RAM):
        raise ValueError('RAM test run was redirected to another mount')
    return value


def source_files(source):
    for directory, names, files in os.walk(source):
        names[:] = sorted(name for name in names if name != '__pycache__')
        for name in names + sorted(files):
            path = Path(directory) / name
            if path.is_symlink():
                raise ValueError('Campaign source contains a symlink: ' + str(path))
        for name in sorted(files):
            path = Path(directory) / name
            if path.suffix not in GENERATED:
                yield path


def fingerprint(source):
    digest = hashlib.sha256()
    count = 0
    for path in source_files(source):
        digest.update(str(path.relative_to(source)).encode() + b'\0')
        digest.update(str(stat.S_IMODE(path.stat().st_mode) & ~0o222).encode() + b'\0')
        digest.update(hashlib.sha256(path.read_bytes()).digest())
        count += 1
    return dict(sha256=digest.hexdigest(), files=count)


def checkout_state(source):
    repo = Path(subprocess.check_output(
        ['git', '-C', str(source), 'rev-parse', '--show-toplevel']).decode().strip())
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args])
    relative = str(source.relative_to(repo))
    patch = git('diff', '--binary', 'HEAD', '--', relative)
    untracked = git('ls-files', '--others', '--exclude-standard', '-z', '--', relative)
    included = {str(path.relative_to(repo)) for path in source_files(source)}
    others = git('ls-files', '--others', '-z', '--', relative).decode().split('\0')[:-1]
    return dict(revision=git('rev-parse', 'HEAD').decode().strip(),
                patch_sha256=hashlib.sha256(patch).hexdigest(),
                source_prefix=relative,
                nontracked_inputs=sorted(name for name in others if name in included),
                untracked=untracked.decode().split('\0')[:-1]), patch


def environment():
    executable = shutil.which(os.environ.get('SBCL', 'sbcl'))
    if not executable:
        raise ValueError('SBCL executable was not found')
    return dict(values={key: os.environ.get(key) for key in ENVIRONMENT},
                executable=executable, launcher_sha256=hashlib.sha256(Path(executable).read_bytes()).hexdigest())


@contextlib.contextmanager
def lock(root, name, exclusive=False, nonblocking=False):
    fd = os.open(root / name, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if info.st_uid != os.getuid() or info.st_nlink != 1 or not stat.S_ISREG(info.st_mode):
            raise ValueError('Invalid campaign lock')
        flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        fcntl.flock(fd, flags | (fcntl.LOCK_NB if nonblocking else 0))
        yield fd
    finally:
        os.close(fd)


@contextlib.contextmanager
def workspace_lease():
    state = RAM.with_name(RAM.name + '-control')
    info = state.lstat()
    if state.is_symlink() or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError('Invalid RAM workspace control directory')
    with lock(state, 'usage.lock') as fd:
        yield fd


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    os.replace(temporary, path)


def create(source):
    root = job_root()
    if (root / 'campaign.json').exists():
        raise ValueError('This run already contains a campaign')
    source = source.resolve()
    before, patch = checkout_state(source)
    image = fingerprint(source)
    frozen = root / 'source' / 'src' / 'lisp'
    frozen.mkdir(parents=True)
    for path in source_files(source):
        target = frozen / path.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    after, _ = checkout_state(source)
    if before != after or image != fingerprint(source) or image != fingerprint(frozen):
        raise ValueError('Source or checkout changed while freezing the campaign')
    for path in sorted(frozen.rglob('*'), reverse=True):
        path.chmod(stat.S_IMODE(path.stat().st_mode) & ~0o222)
    frozen.chmod(stat.S_IMODE(frozen.stat().st_mode) & ~0o222)
    (root / 'source-changes.patch').write_bytes(patch)
    for name in before['nontracked_inputs']:
        suffix = Path(name).relative_to(before['source_prefix'])
        original = frozen / suffix
        if original.is_file():
            destination = root / 'source-additions' / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(original, destination)
    (root / 'parent-fasls').mkdir(mode=0o700)
    (root / 'manifests').mkdir(mode=0o700)
    manifest = dict(format=1, state='open', checkout=str(source), source=str(frozen),
                    image=image, checkout_state=before, environment=environment())
    write_json(root / 'campaign.json', manifest)
    write_json(root / 'logs/campaign-manifest.log', manifest)
    print('TEST-CAMPAIGN ' + str(root), flush=True)


def read_campaign(root):
    data = json.loads((root / 'campaign.json').read_text())
    if data['format'] != 1 or data['source'] != str(root / 'source/src/lisp'):
        raise ValueError('Invalid campaign manifest')
    return data


def fasls(root):
    if root.exists():
        for path in sorted(root.rglob('*')):
            if path.is_symlink():
                raise ValueError('Cache contains a symlink')
            if path.is_file() and path.suffix == '.fasl' and not re.search(r'-tmp[A-Z0-9]+\.fasl$', path.name):
                yield path


def publish_cache(private, shared):
    additions = []
    for path in fasls(private):
        destination = shared / path.relative_to(private)
        if destination.exists():
            if hashlib.sha256(path.read_bytes()).digest() != hashlib.sha256(destination.read_bytes()).digest():
                raise ValueError('Compiled cache collision; start a new campaign: ' + str(path.relative_to(private)))
        else:
            additions.append((path, destination))
    for path, destination in additions:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix('.tmp')
        shutil.copy2(path, temporary)
        os.replace(temporary, destination)
    return len(additions)


def wait_child(command, env, log, lease, run_lock, workspace):
    """Keep all three leases in the child even if the coordinator disappears."""
    process = None
    def interrupted(signum, frame):
        raise InterruptedError('Campaign interrupted by signal ' + str(signum))
    old = {signum: signal.signal(signum, interrupted) for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    try:
        with log.open('w') as output:
            process = subprocess.Popen(command, env=env, cwd=env['PG_LISP_FROZEN_SOURCE'],
                                       stdout=output, stderr=subprocess.STDOUT,
                                       pass_fds=(lease, run_lock, workspace), start_new_session=True)
            try:
                ticks = Path('/proc', str(process.pid), 'stat').read_text().rpartition(')')[2].split()[19]
            except FileNotFoundError:
                ticks = None
            child = dict(pid=process.pid, start_ticks=ticks, lease_fd=lease,
                         run_lock_fd=run_lock, workspace_fd=workspace)
            write_json(log.parent / 'child.log', child)
            print('TEST-CAMPAIGN-CHILD ' + json.dumps(child), flush=True)
            return process.wait()
    finally:
        if process and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        for signum, handler in old.items():
            signal.signal(signum, handler)


def claim_job(job):
    fd = os.open(job / 'campaign-job.claim',
                 os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(fd)


def require_parallel_manifests(root, target):
    for name in ('image', target):
        path = root / 'manifests' / (name + '.log')
        if path.is_symlink() or not path.is_file():
            raise ValueError('Parallel runs require a previously qualified image and target')


def job_cache(job):
    home = Path(os.environ.get('XDG_CACHE_HOME', ''))
    if home != job / 'cache' or home.is_symlink() or not home.is_dir():
        raise ValueError('Campaign cache must be the job private cache directory')
    return home / 'common-lisp'


def campaign_wrapper(root, executable, inherited):
    available = [fd for fd in range(3, 10) if fd not in inherited]
    if len(available) < 2:
        raise ValueError('Campaign wrapper has no free shell lock descriptors')
    lease_fd, workspace_fd = available[:2]
    return ('#!/bin/sh\nset -eu\nexec ' + str(lease_fd) + '>>' + shlex.quote(str(root / 'lease.lock')) +
            '\nflock -s ' + str(lease_fd) + '\nexec ' + str(workspace_fd) + '>>' +
            shlex.quote(str(RAM.with_name(RAM.name + '-control') / 'usage.lock')) +
            '\nflock -s ' + str(workspace_fd) + '\nexec ' + shlex.quote(executable) + ' "$@"\n')


def run(campaign, target, heads, cold, workspace, parallel=False):
    root = job_root(campaign)
    job = job_root()
    if root == job:
        raise ValueError('Each campaign job needs its own RAM workspace')
    claim_job(job)
    with lock(root, 'run.lock', exclusive=not parallel) as run_lock, \
         lock(root, 'lease.lock', exclusive=not parallel) as lease:
        if not parallel:
            fcntl.flock(lease, fcntl.LOCK_SH)
        data = read_campaign(root)
        source = Path(data['source'])
        if data['state'] != 'open' or data['environment'] != environment():
            raise ValueError('Campaign is closed or its runtime environment changed')
        checkout, _ = checkout_state(Path(data['checkout']))
        if data['checkout_state'] != checkout:
            raise ValueError('Campaign checkout revision or dirty state changed; freeze a new campaign')
        if data['image'] != fingerprint(source) or data['image'] != fingerprint(Path(data['checkout'])):
            raise ValueError('Campaign source differs from the current checkout; freeze a new campaign')
        if parallel:
            require_parallel_manifests(root, target)
        cache = job_cache(job)
        if cache.exists():
            raise ValueError('Campaign needs a fresh private parent cache')
        copied = time.monotonic()
        with lock(root, 'cache.lock', exclusive=True):
            if not cold:
                shutil.copytree(root / 'parent-fasls', cache)
        copy_seconds = time.monotonic() - copied
        wrapper = job / 'campaign-sbcl'
        wrapper.write_text(campaign_wrapper(root, data['environment']['executable'],
                                            (lease, run_lock, workspace)))
        wrapper.chmod(0o700)
        env = dict(os.environ, SBCL=str(wrapper), PG_LISP_TEST_CAMPAIGN=str(root),
                   PG_LISP_TEST_CAMPAIGN_SBCL=str(wrapper), PG_LISP_FROZEN_SOURCE=str(source),
                   PG_LISP_TEST_IDENTITY_OUT=str(job / 'logs/image.log'),
                   PG_LISP_TEST_SUITE_OUT=str(job / 'logs/suite.log'))
        for kind, key in (('image', 'PG_LISP_TEST_IDENTITY_EXPECTED'),
                          (target, 'PG_LISP_TEST_SUITE_EXPECTED')):
            reference = root / 'manifests' / (kind + '.log')
            if reference.exists():
                env[key] = str(reference)
        command = [str(wrapper), '--noinform', '--disable-debugger', '--no-sysinit', '--no-userinit',
                   '--non-interactive', '--load', str(source / 't/run-system-tests.lisp'),
                   '--end-toplevel-options', target, '--strict', *heads]
        started = time.monotonic()
        cpu = resource.getrusage(resource.RUSAGE_CHILDREN)
        result = wait_child(command, env, job / 'logs/test.log', lease, run_lock, workspace)
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        if data['image'] != fingerprint(source):
            raise ValueError('Immutable campaign source changed during the job')
        published = 0
        if result == 0 and not parallel:
            with lock(root, 'cache.lock', exclusive=True):
                for kind, name in (('image', 'image'), (target, 'suite')):
                    reference = root / 'manifests' / (kind + '.log')
                    if not reference.exists():
                        shutil.copy2(job / 'logs' / (name + '.log'), reference)
                if not cold:
                    published = publish_cache(cache, root / 'parent-fasls')
        record = dict(format=1, campaign=str(root), source=data['image'], target=target,
                       requested_heads=heads, strict=True, cold=cold, parallel=parallel, exit_code=result,
                      wall_seconds=time.monotonic() - started, copy_seconds=copy_seconds,
                      cpu_seconds=after.ru_utime + after.ru_stime - cpu.ru_utime - cpu.ru_stime,
                      peak_rss_kib=after.ru_maxrss, published_fasls=published,
                      log=str(job / 'logs/test.log'))
        write_json(job / 'logs/campaign-result.json', record)
        print('TEST-CAMPAIGN-RESULT ' + json.dumps(record), flush=True)
        return result


def close(campaign):
    root = job_root(campaign)
    with lock(root, 'lease.lock', exclusive=True, nonblocking=True):
        data = read_campaign(root)
        data['state'] = 'closed'
        write_json(root / 'campaign.json', data)
    print('TEST-CAMPAIGN-CLOSED ' + str(root) + ' (files retained for audited cleanup)', flush=True)


def parse_args(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest='action', required=True)
    create_parser = actions.add_parser('create')
    create_parser.add_argument('--source', type=Path, default=Path(__file__).resolve().parent.parent)
    run_parser = actions.add_parser('run')
    run_parser.add_argument('campaign')
    run_parser.add_argument('target', choices=('unit', 'physical'))
    run_parser.add_argument('--cold', action='store_true', help='Do not read or publish parent FASLs')
    run_parser.add_argument('--parallel', action='store_true',
                            help='Overlap independent jobs on a qualified campaign; never publish cache additions')
    run_parser.add_argument('heads', nargs='*')
    actions.add_parser('close').add_argument('campaign')
    args, extra = parser.parse_known_args(arguments)
    if extra:
        if args.action != 'run' or any(value.startswith('-') for value in extra):
            parser.error('unrecognized arguments: ' + ' '.join(extra))
        args.heads.extend(extra)
    return args


def main():
    os.umask(0o077)
    args = parse_args()
    with workspace_lease() as workspace:
        if args.action == 'create':
            create(args.source)
        elif args.action == 'run':
            return run(args.campaign, args.target, args.heads, args.cold, workspace, args.parallel)
        else:
            close(args.campaign)
    return 0


if __name__ == '__main__':
    sys.exit(main())
