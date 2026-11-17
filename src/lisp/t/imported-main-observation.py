"""Check pre-WAL imported proof observations without modifying the certified root."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import time


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def lisp_string(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'


def fixture_image(data, writer):
    image = writer.tree_image(data)
    for relative, row in image.items():
        facts = (data / relative).lstat()
        row.extend((facts.st_uid, facts.st_gid, facts.st_nlink,
                    facts.st_mtime_ns, facts.st_ctime_ns))
    return image


def main():
    directory = Path(__file__).resolve().parent
    campaign = module('campaign', directory / 'test-campaign.py')
    writer = module('writer', directory / 'continuation-native-writer.py')
    root = campaign.job_root(os.environ['PG_LISP_TEST_CAMPAIGN'])
    job = campaign.job_root()
    data = Path(os.environ['PG_NATIVE_ACTIVE_PROOF_DATA']).resolve(strict=True)
    # The certificate binds this earlier fixture's inode; a new job cannot clone it.
    campaign.job_root(data.parents[2])
    identity = (data.stat().st_dev, data.stat().st_ino)
    with campaign.workspace_lease() as workspace, \
            campaign.lock(root, 'run.lock', exclusive=True) as run_lock, \
            campaign.lock(root, 'lease.lock') as lease:
        frozen = campaign.read_campaign(root)
        source = Path(frozen['source'])
        checkout, _ = campaign.checkout_state(Path(frozen['checkout']))
        if (frozen['state'] != 'open' or frozen['environment'] != campaign.environment()
                or frozen['checkout_state'] != checkout
                or frozen['image'] != campaign.fingerprint(source)
                or frozen['image'] != campaign.fingerprint(Path(frozen['checkout']))):
            raise ValueError('Imported observation source or runtime changed')
        with writer.owned_fixture(data, identity):
            before = fixture_image(data, writer)
        campaign.write_json(job / 'logs/original-before.json', before)
        dependencies = lisp_string(source / 't/test-dependencies.lisp')
        setup = ('(let ((cl-user::*pg-test-system* "rove")) '
                 '(declare (special cl-user::*pg-test-system*)) (load ' + dependencies + '))')
        script = lisp_string(source / 't/imported-main-observation.lisp')
        load = ('(handler-bind ((warning (lambda (c) '
                '(unless (typep c \'sb-kernel:redefinition-warning) (error c))))) '
                '(load ' + script + '))')
        environment = dict(os.environ, PG_NATIVE_PROOF_DATA=str(data),
                           PG_LISP_FROZEN_SOURCE=str(source))
        log = job / 'logs/imported-main-observation.log'
        started = time.monotonic()
        status = campaign.wait_child(
            [environment.get('SBCL', 'sbcl'), '--noinform', '--no-sysinit', '--no-userinit',
             '--non-interactive', '--eval',
             '(defparameter *native-imported-main-bootstrap-start* (get-internal-real-time))',
             '--eval', '(require :asdf)', '--eval', setup, '--eval', load],
            environment, log, lease, run_lock, workspace)
        with writer.owned_fixture(data, identity):
            after = fixture_image(data, writer)
        campaign.write_json(job / 'logs/original-after.json', after)
        preserved = before == after
        source_preserved = frozen['image'] == campaign.fingerprint(source)
        text = log.read_text()
        phases = re.findall(r'^IMPORTED-MAIN-OBSERVATION-PHASE (setup|execution) wall=([0-9]+\.[0-9]+)$',
                            text, re.MULTILINE)
        result = dict(exit_code=status, wall_seconds=time.monotonic() - started,
                      source=frozen['image'], original_preserved=preserved,
                      source_preserved=source_preserved,
                      phases={name: float(value) for name, value in phases},
                      original_sha256=hashlib.sha256(
                          json.dumps(before, sort_keys=True).encode()).hexdigest(), log=str(log))
        campaign.write_json(job / 'logs/imported-main-observation-result.json', result)
        if (status or not preserved or not source_preserved
                or 'IMPORTED-MAIN-OBSERVATION PASS' not in text
                or len(phases) != 2 or set(result['phases']) != {'setup', 'execution'}):
            raise RuntimeError('Imported observation failed: ' + str(result))
        print('IMPORTED-MAIN-OBSERVATION-RESULT ' + json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
