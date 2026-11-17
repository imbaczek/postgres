# Qualification belongs to one frozen campaign and compile profile.  A fresh
# compiler records its log and FASL inventory in a parent-owned sealed checkpoint
# before tests run.  A successful serial job may publish only those unchanged
# files; reuse copies and verifies them in a private job cache.
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile


PASS = 'TEST-STRICT-COMPILE-PASS'
MAXIMUM = 64 * 1024 * 1024
SEALS = fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL


@contextlib.contextmanager
def checkpoint():
    fd = os.memfd_create('pg-strict-compile', os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        yield fd
    finally:
        os.close(fd)


def seal_checkpoint(path, value):
    fd = os.open(path, os.O_RDWR | os.O_CLOEXEC)
    try:
        if os.fstat(fd).st_size or fcntl.fcntl(fd, fcntl.F_GET_SEALS):
            raise ValueError('Compiler checkpoint was already written')
        data = json.dumps(value, sort_keys=True).encode()
        if len(data) > 4 * 1024 * 1024:
            raise ValueError('Compiler checkpoint exceeds its bound')
        with os.fdopen(os.dup(fd), 'wb') as stream:
            stream.write(data)
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS, SEALS)
    finally:
        os.close(fd)


def read_checkpoint(fd):
    if fd is None or fcntl.fcntl(fd, fcntl.F_GET_SEALS) != SEALS:
        raise ValueError('Compiler checkpoint was not sealed before tests')
    size = os.fstat(fd).st_size
    if not 0 < size <= 4 * 1024 * 1024:
        raise ValueError('Invalid compiler checkpoint size')
    return json.loads(os.pread(fd, size, 0))


def directory(path):
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('Compiler cache directory is not private: ' + str(path))
    return info


def private_file(path, maximum=MAXIMUM, contents=False):
    before = path.lstat()
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        held = os.fstat(fd)
        facts = lambda value: (value.st_dev, value.st_ino, value.st_mode, value.st_uid,
                               value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
        if (not stat.S_ISREG(held.st_mode) or held.st_uid != os.getuid()
                or held.st_nlink != 1 or held.st_mode & 0o077
                or facts(before) != facts(held) or held.st_size > maximum):
            raise ValueError('Compiler cache file is not private and bound: ' + str(path))
        digest, pieces, size = hashlib.sha256(), [], 0
        while block := os.read(fd, 1024 * 1024):
            size += len(block)
            if size > maximum:
                raise ValueError('Compiler cache file exceeds its bound')
            digest.update(block)
            if contents:
                pieces.append(block)
        if size != held.st_size or facts(held) != facts(os.fstat(fd)) or facts(held) != facts(path.lstat()):
            raise ValueError('Compiler cache file changed during its read')
        return dict(bytes=size, sha256=digest.hexdigest()), b''.join(pieces)
    finally:
        os.close(fd)


def read_json(path):
    return json.loads(private_file(path, 4 * 1024 * 1024, True)[1])


def write_json(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
    path.chmod(0o600)


def relative_path(value):
    if not isinstance(value, str):
        raise ValueError('Invalid qualified FASL path')
    path = Path(value)
    if (path.is_absolute() or str(path) != value
            or '..' in path.parts or value == '.' or path.suffix != '.fasl'
            or re.search(r'-tmp[A-Z0-9]+\.fasl$', path.name)):
        raise ValueError('Invalid qualified FASL path')
    return path


def listed_file(cache, row):
    path = relative_path(row['path'])
    if type(row['bytes']) is not int or row['bytes'] < 0 or not re.fullmatch('[0-9a-f]{64}', row['sha256']):
        raise ValueError('Invalid qualified FASL facts')
    directory(cache)
    for parent in reversed(path.parents):
        if parent != Path('.'):
            directory(cache / parent)
    actual = private_file(cache / path)[0]
    if actual != {key: row[key] for key in ('bytes', 'sha256')}:
        raise ValueError('Qualified FASL changed: ' + str(path))


def inventory(cache):
    directory(cache)
    rows = []
    for path in sorted(cache.rglob('*')):
        if path.is_symlink():
            raise ValueError('Compiler cache contains a symlink')
        if path.is_dir():
            directory(path)
        else:
            relative = str(path.relative_to(cache))
            relative_path(relative)
            rows.append(dict(path=relative, **private_file(path)[0]))
    if not 1 <= len(rows) <= 10000:
        raise ValueError('Compiler cache has no complete bounded inventory')
    return rows


def verify_files(cache, rows, exact=True):
    if not isinstance(rows, list) or not 1 <= len(rows) <= 10000:
        raise ValueError('Invalid qualified FASL inventory')
    names = [row['path'] for row in rows]
    if names != sorted(set(names)):
        raise ValueError('Qualified FASL inventory repeats or reorders a path')
    for row in rows:
        listed_file(cache, row)
    if exact and inventory(cache) != rows:
        raise ValueError('Qualified FASL inventory has extra files')


def identity(campaign, profile):
    data = read_json(campaign / 'campaign.json')
    expected = data['environment']['values'].get('PG_LISP_TEST_COMPILE_POLICY') or 'default'
    if (data['state'] != 'open' or data['source'] != str(campaign / 'source/src/lisp')
            or profile.lower() != expected):
        raise ValueError('Compiler qualification source or profile changed')
    return {key: data[key] for key in ('source', 'image', 'environment', 'checkout_state')}


def validate(campaign, manifest, profile, image=None):
    if manifest['format'] != 1 or manifest['identity'] != identity(campaign, profile):
        raise ValueError('Compiler qualification identity changed')
    if manifest['profile'] != profile.lower():
        raise ValueError('Compiler qualification profile changed')
    if image is not None and private_file(image)[0]['sha256'] != manifest['image_sha256']:
        raise ValueError('Compiler qualification runtime image changed')


def read_published(campaign, profile, image):
    directory(campaign)
    directory(campaign / 'manifests')
    value = read_json(campaign / 'manifests/strict-compile.json')
    if (value['kind'] != 'published' or value['cache'] != str(campaign / 'plain-strict-cache')
            or value['log'] != str(campaign / 'manifests/strict-compile.log')):
        raise ValueError('Compiler qualification lost its published paths')
    validate(campaign, value, profile, image)
    if private_file(Path(value['log']))[0] != value['log_facts']:
        raise ValueError('Published compiler log changed')
    return value


def owned_path(path, root):
    if not path.is_absolute() or '..' in path.parts or not path.is_relative_to(root):
        raise ValueError('Compiler qualification path escapes its owner')
    directory(root)
    for parent in reversed(path.relative_to(root).parents):
        if parent != Path('.'):
            directory(root / parent)


# Capture the successful compiler output before any test head can add files.
def capture(campaign, job, cache, log, image, profile, output, checkpoint_path):
    directory(job)
    for path, root in ((cache, job / 'tmp'), (log, job / 'tmp'),
                       (image, job / 'logs'), (output, job / 'logs')):
        owned_path(path, root)
    log_facts, text = private_file(log, 16 * 1024 * 1024, True)
    if not text.decode().splitlines() or text.decode().splitlines()[-1] != PASS:
        raise ValueError('Fresh compiler has no final success marker')
    value = dict(format=1, kind='fresh', identity=identity(campaign, profile),
                 profile=profile.lower(), image_sha256=private_file(image)[0]['sha256'],
                 cache=str(cache), log=str(log), log_facts=log_facts, files=inventory(cache))
    write_json(output, value)
    seal_checkpoint(checkpoint_path, value)
    return value


def copy_files(cache, rows, destination):
    verify_files(cache, rows, exact=False)
    destination.mkdir(mode=0o700)
    for row in rows:
        path = destination / relative_path(row['path'])
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        shutil.copy2(cache / row['path'], path)
        path.chmod(0o600)
    verify_files(destination, rows)
    verify_files(cache, rows, exact=False)


# Compare the receipt with the sealed pre-test checkpoint and publish once.
# Extra files made by tests are excluded from the qualified inventory.
def publish(campaign, job, receipt, checkpoint_fd=None):
    if not receipt.exists():
        return 0
    value = read_checkpoint(checkpoint_fd)
    if read_json(receipt) != value:
        raise ValueError('Compiler receipt changed after its pre-test checkpoint')
    directory(campaign)
    directory(campaign / 'manifests')
    owned_path(job / 'logs/image.log', job / 'logs')
    if value['kind'] != 'fresh':
        raise ValueError('Only a fresh compiler can publish qualification')
    validate(campaign, value, value['profile'], job / 'logs/image.log')
    cache, log = Path(value['cache']), Path(value['log'])
    owned_path(cache, job / 'tmp')
    owned_path(log, job / 'tmp')
    if private_file(log)[0] != value['log_facts']:
        raise ValueError('Compiler qualification log changed')
    verify_files(cache, value['files'], exact=False)
    manifest = campaign / 'manifests/strict-compile.json'
    if manifest.exists():
        prior = read_published(campaign, value['profile'], job / 'logs/image.log')
        verify_files(campaign / 'plain-strict-cache', prior['files'])
        return 0
    destination = campaign / 'plain-strict-cache'
    if destination.exists():
        raise ValueError('Interrupted compiler cache publication; use a new campaign')
    with tempfile.TemporaryDirectory(prefix='.strict-cache-', dir=campaign) as temporary:
        stage = Path(temporary) / 'cache'
        copy_files(cache, value['files'], stage)
        # Publication is write-once, including the log. Never follow an existing
        # destination link or replace an interrupted publication's evidence.
        published_log = campaign / 'manifests/strict-compile.log'
        with published_log.open('xb') as output:
            published_log.chmod(0o600)
            with log.open('rb') as source:
                shutil.copyfileobj(source, output)
        if private_file(published_log)[0] != value['log_facts']:
            raise ValueError('Compiler log changed during publication')
        published_log.chmod(0o400)
        stage.rename(destination)
    for path in sorted(destination.rglob('*'), reverse=True):
        path.chmod(0o500 if path.is_dir() else 0o400)
    destination.chmod(0o500)
    value.update(kind='published', cache=str(destination),
                 log=str(campaign / 'manifests/strict-compile.log'))
    write_json(manifest, value)
    manifest.chmod(0o400)
    return len(value['files'])


# Verify the shared qualification and make a private copy for this job.
def prepare(campaign, job, profile):
    value = read_published(campaign, profile, campaign / 'manifests/image.log')
    shared = campaign / 'plain-strict-cache'
    verify_files(shared, value['files'])
    cache, receipt = job / 'plain-strict-cache', job / 'logs/strict-compile-reused.json'
    copy_files(shared, value['files'], cache)
    value.update(kind='reused', cache=str(cache))
    write_json(receipt, value)
    return cache, receipt


# Recheck the private copy and its campaign and image binding before use.
def verify_job(campaign, job, receipt, cache, profile, image):
    value = read_json(receipt)
    expected = read_published(campaign, profile, image)
    expected.update(kind='reused', cache=str(cache))
    if (value['kind'] != 'reused' or cache != job / 'plain-strict-cache'
            or value != expected):
        raise ValueError('Reused compiler cache does not belong to its job')
    verify_files(cache, value['files'])
    return value


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('capture', 'verify'))
    parser.add_argument('cache', type=Path)
    parser.add_argument('profile')
    parser.add_argument('--log', type=Path)
    args = parser.parse_args()
    campaign = Path(os.environ['PG_LISP_TEST_CAMPAIGN'])
    job = Path(os.environ['PG_LISP_TEST_RUN_DIR'])
    image = Path(os.environ['PG_LISP_TEST_IDENTITY_OUT'])
    if args.action == 'capture':
        capture(campaign, job, args.cache, args.log, image, args.profile,
                Path(os.environ['PG_LISP_TEST_STRICT_REPORT_OUT']),
                os.environ['PG_LISP_TEST_STRICT_CHECKPOINT'])
    else:
        verify_job(campaign, job, Path(os.environ['PG_LISP_TEST_STRICT_REUSE_RECEIPT']),
                   args.cache, args.profile, image)


if __name__ == '__main__':
    main()
