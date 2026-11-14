"""Check root descriptor cleanup with real postmaster ownership and close faults."""
import os
from pathlib import Path
import subprocess
import tempfile
import time


def lisp_string(value):
    return '"' + str(value).replace('\\', '\\\\').replace('"', '\\"') + '"'


def run():
    if 'PG_LISP_TEST_RUN_DIR' not in os.environ:
        raise RuntimeError('run this test through t/test-ramdisk.sh run')
    source = Path(__file__).resolve().parents[1]
    data = Path(tempfile.mkdtemp(prefix='root-owner-', dir=os.environ['TMPDIR']))
    log = Path(os.environ['PG_LISP_TEST_LOG_DIR']) / 'root-descriptor.log'
    forms = ['(require :asdf)',
             f'(push (uiop:ensure-directory-pathname {lisp_string(source)}) asdf:*central-registry*)',
             '(asdf:load-system "pg")', '(in-package :pg)',
             '(setf *print-circle* t *print-length* nil *print-level* nil)',
             f'(handler-bind ((warning (lambda (c) (error "fixture warning: ~a" c)))) (load {lisp_string(Path(__file__).with_name("physical-directory-root-native.lisp"))}))',
             f'''(let ((*pgdata* (directory-pathname {lisp_string(data)})))
               (unwind-protect
                    (postmaster-own-data-directory "127.0.0.1" 6543
                      :startup-reconcile (lambda ()
                        (dolist (mode '(:error :throw :normal)) (physical-directory-root-test-close mode))))
                 (when *postmaster-pid-stream* (postmaster-remove-pid-file))
                 (when *postmaster-data-lock*
                   (close *postmaster-data-lock*) (setf *postmaster-data-lock* nil))))''']
    command = [os.environ.get('SBCL', 'sbcl'), '--noinform', '--disable-debugger', '--non-interactive']
    for code in forms:
        command.extend(['--eval', code])
    started = time.monotonic()
    with log.open('w') as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=90)
    assert result.returncode == 0, (result.returncode, log)
    text = log.read_text()
    assert text.count('LOGICAL-INSTALL-ROOT-CLOSE-PASS') == 3, log
    assert not (data / 'postmaster.pid').exists()
    assert sorted(path.name for path in data.iterdir()) == ['.pg_lisp.lock']
    print(f'ROOT-DESCRIPTOR-PASS cases=3 seconds={time.monotonic() - started:.6f}', flush=True)


if __name__ == '__main__':
    run()
