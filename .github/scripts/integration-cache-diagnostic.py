#!/usr/bin/env python3
"""Measure the Go build cache around the Armada integration startup."""

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
        raise ValueError('expected a dedicated absolute Go cache path')
    return path


def cache_entries(path):
    return sorted(entry for entry in path.glob('[0-9a-f][0-9a-f]/*')
                  if entry.name.endswith(('-a', '-d')))


def record(phase, *, stream=None, **fields):
    cache = cache_path()
    observed_at = time.time()
    entries = cache_entries(cache)
    ages = [observed_at - entry.stat().st_mtime for entry in entries]
    marker = cache / 'trim.txt'
    marker_contents = marker.read_text().strip() if marker.exists() else None
    print('TASK015 ' + json.dumps({
        'phase': phase,
        'observed_epoch': observed_at,
        'action_entries': sum(entry.name.endswith('-a') for entry in entries),
        'data_entries': sum(entry.name.endswith('-d') for entry in entries),
        'entries_older_than_cutoff': sum(age > CUTOFF for age in ages),
        'minimum_age_seconds': min(ages, default=None),
        'maximum_age_seconds': max(ages, default=None),
        'trim_marker': marker_contents,
        **fields,
    }), file=stream, flush=True)


def cache_digest(cache):
    digest = hashlib.sha256()
    files = sorted(path for path in cache.rglob('*') if path.is_file())
    for path in files:
        relative = path.relative_to(cache)
        if relative == Path('trim.txt'):
            continue
        digest.update(os.fsencode(relative) + b'\0')
        with path.open('rb') as stream:
            digest.update(hash_file(stream))
    return digest.hexdigest()


def hash_file(stream):
    digest = hashlib.sha256()
    for block in iter(lambda: stream.read(1024 * 1024), b''):
        digest.update(block)
    return digest.digest()


def check_empty(cache):
    if cache_entries(cache) or (cache / 'trim.txt').exists():
        raise ValueError('expected empty diagnostic Go cache')


def isolate_runner_cache():
    cache = cache_path()
    record('runner_cache_before_isolation')
    if cache.exists() or cache.is_symlink():
        # Keep the original path available for actions/cache's matching and extraction.
        backup = Path(tempfile.mkdtemp(prefix='task-015-original-cache-', dir=cache.parent))
        cache.rename(backup / cache.name)
    check_empty(cache)


def prepare(case):
    cache = cache_path()
    record('after_restore', case=case)
    if case == 'empty':
        check_empty(cache)
        record('before_startup', case=case)
        return

    entries = cache_entries(cache)
    if not entries:
        raise ValueError('the restored Go cache is empty')
    digest = cache_digest(cache)
    now = int(time.time())
    mtime = now - 11 * DAY if case == 'aged' else now
    for entry in entries:
        os.utime(entry, (mtime, mtime))
    (cache / 'trim.txt').write_text(str(now - 2 * DAY))
    record('before_startup', case=case, cache_digest=digest)


def run_tool(tool, args):
    start = time.monotonic()
    result = subprocess.run([tool, *args])
    return result.returncode, time.monotonic() - start


def mage_go(args):
    code, seconds = run_tool(os.environ['TASK015_GO'], args)
    # Mage captures Go's stdout for version and environment lookups.
    with Path(os.environ['TASK015_MAGE_TRACE']).open('a') as trace:
        record('after_mage_go', stream=trace, command=args[0], command_seconds=seconds, exit_code=code)
    return code


def goreleaser(args):
    record('before_goreleaser', archive_skipped='--skip=archive' in args)
    start = time.monotonic()
    build_start = None
    build_observed = False
    with subprocess.Popen([os.environ['TASK015_GORELEASER'], *args],
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
        for line in process.stdout:
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
            if build_start is None and b'building binaries' in line:
                build_start = time.monotonic()
            if build_start is not None and not build_observed and b'calculating checksums' in line:
                build_observed = True
                record('after_goreleaser_binaries', marker_interval_seconds=time.monotonic() - build_start)
        code = process.wait()
    record('after_goreleaser', command_seconds=time.monotonic() - start, exit_code=code)
    if code == 0 and (not build_observed or '--skip=archive' not in args):
        raise ValueError('missing expected GoReleaser build boundary or archive skip')
    return code


def run_startup():
    go = shutil.which('go')
    release = shutil.which('goreleaser')
    if go is None or release is None:
        raise ValueError('expected Go and GoReleaser on PATH')
    # Replace only the entry points we observe. GoReleaser still resolves the real Go tool.
    script = Path(__file__).resolve()
    with tempfile.TemporaryDirectory(prefix='task-015-', dir=os.environ['RUNNER_TEMP']) as scratch:
        directory = Path(scratch)
        trace = directory / 'mage.jsonl'
        (directory / 'mage-go').symlink_to(script)
        (directory / 'goreleaser').symlink_to(script)
        env = {
            **os.environ,
            'PATH': str(directory) + os.pathsep + os.environ['PATH'],
            'MAGEFILE_GOCMD': str(directory / 'mage-go'),
            'TASK015_GO': go,
            'TASK015_MAGE_TRACE': str(trace),
            'TASK015_GORELEASER': release,
        }
        start = time.monotonic()
        result = subprocess.run([go, 'run', 'github.com/magefile/mage@v1.17.2', '-v', 'dev:full'], env=env)
        seconds = time.monotonic() - start
        if trace.exists():
            print(trace.read_text(), end='', flush=True)
        record('after_startup', command_seconds=seconds, exit_code=result.returncode)
    return result.returncode


def main():
    program = Path(sys.argv[0]).name
    if program == 'mage-go':
        return mage_go(sys.argv[1:])
    if program == 'goreleaser':
        return goreleaser(sys.argv[1:])

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('isolate')
    preparation = commands.add_parser('prepare')
    preparation.add_argument('case', choices=['fresh', 'aged', 'empty'])
    commands.add_parser('run')
    args = parser.parse_args()
    if args.command == 'isolate':
        isolate_runner_cache()
    elif args.command == 'prepare':
        prepare(args.case)
    else:
        return run_startup()
    return 0


if __name__ == '__main__':
    sys.exit(main())
