#!/usr/bin/env python3
"""Measure cache trimming around the normal Mage unit-test command."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

DAY = 24 * 60 * 60
CUTOFF = 5 * DAY + 3600


def cache_path():
    path = Path(os.environ['GOCACHE'])
    if not path.is_absolute() or path == Path('/'):
        raise ValueError('expected an absolute Go cache directory')
    return path


def entries(cache):
    return sorted(path for path in cache.glob('[0-9a-f][0-9a-f]/*')
                  if path.name.endswith(('-a', '-d')))


def record(phase, *, stream=None, **fields):
    cache = cache_path()
    observed = time.time()
    ages = [observed - path.stat().st_mtime for path in entries(cache)]
    marker = cache / 'trim.txt'
    print('TASK016 ' + json.dumps({
        'phase': phase,
        'observed_epoch': observed,
        'entries': len(ages),
        'old_entries': sum(age > CUTOFF for age in ages),
        'max_age_seconds': max(ages, default=None),
        'trim_marker': marker.read_text().strip() if marker.is_file() else None,
        **fields,
    }), file=stream, flush=True)


def content_digest(cache):
    digest = hashlib.sha256()
    for path in sorted(path for path in cache.rglob('*') if path.is_file()):
        relative = path.relative_to(cache)
        if relative == Path('trim.txt'):
            continue
        digest.update(os.fsencode(relative) + b'\0')
        file_digest = hashlib.sha256()
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                file_digest.update(chunk)
        digest.update(file_digest.digest())
    return digest.hexdigest()


def isolate():
    cache = cache_path()
    record('runner_cache_before_isolation')
    if cache.exists() or cache.is_symlink():
        # The restore still needs the original absolute cache path.
        backup = Path(tempfile.mkdtemp(prefix='task-016-original-cache-', dir=cache.parent))
        cache.rename(backup / cache.name)


def prepare(case):
    cache = cache_path()
    restored = entries(cache)
    if os.environ['TASK016_CACHE_HIT'] != 'true' or not restored:
        raise ValueError('expected a nonempty exact unit-cache restore')
    record('after_restore', case=case)
    digest = content_digest(cache)
    now = int(time.time())
    mtime = now - (11 * DAY if case == 'aged' else 0)
    for path in restored:
        os.utime(path, (mtime, mtime))
    (cache / 'trim.txt').write_text(str(now - 2 * DAY))
    record('before_startup', case=case, content_sha256=digest)


def go_probe(args):
    command = args[0] if args else ''
    caller = Path(sys.argv[0]).name
    if command not in {'build', 'install', 'list', 'test'}:
        os.execv(os.environ['TASK016_GO'], [os.environ['TASK016_GO'], *args])
    # Mage parses output from Go commands. Emit observations through a separate file.
    with Path(os.environ['TASK016_TRACE']).open('a') as trace:
        record('before_go', stream=trace, caller=caller, command=command,
               test_count_one='-count=1' in args if command == 'test' else None)
        start = time.monotonic()
        result = subprocess.run([os.environ['TASK016_GO'], *args])
        seconds = time.monotonic() - start
        record('after_go', stream=trace, caller=caller, command=command,
               command_seconds=seconds, exit_code=result.returncode)
    return result.returncode


def run():
    go = shutil.which('go')
    if go is None:
        raise ValueError('Go is missing from PATH')
    with tempfile.TemporaryDirectory(prefix='task-016-', dir=os.environ['RUNNER_TEMP']) as temp:
        directory = Path(temp)
        trace = directory / 'go.jsonl'
        for name in ('go', 'mage-go'):
            (directory / name).symlink_to(Path(__file__).resolve())
        env = {
            **os.environ,
            'PATH': str(directory) + os.pathsep + os.environ['PATH'],
            'MAGEFILE_GOCMD': str(directory / 'mage-go'),
            'TASK016_GO': go,
            'TASK016_TRACE': str(trace),
            'ARMADA_UNIT_CACHE_DIAGNOSTIC': '1',
        }
        record('mage_start', image_os=os.getenv('ImageOS'), image_version=os.getenv('ImageVersion'),
               revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip())
        start = time.monotonic()
        result = subprocess.run([go, 'run', 'github.com/magefile/mage@v1.17.2', '-v', 'tests'], env=env)
        elapsed = time.monotonic() - start
        if trace.is_file():
            print(trace.read_text(), end='', flush=True)
        record('mage_end', command_seconds=elapsed, exit_code=result.returncode)
    return result.returncode


def main():
    if Path(sys.argv[0]).name in {'go', 'mage-go'}:
        return go_probe(sys.argv[1:])

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('isolate')
    prep = commands.add_parser('prepare')
    prep.add_argument('case', choices=['fresh', 'aged'])
    commands.add_parser('run')
    args = parser.parse_args()
    if args.command == 'isolate':
        isolate()
    elif args.command == 'prepare':
        prepare(args.case)
    else:
        return run()
    return 0


if __name__ == '__main__':
    sys.exit(main())
