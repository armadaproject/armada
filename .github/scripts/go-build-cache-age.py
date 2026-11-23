"""Summarize Go build cache ages for the integration job log."""

import os
from pathlib import Path
import time


cache = Path(os.environ['GOCACHE'])
if not cache.is_dir():
    print('Go build cache directory is absent')
else:
    now = time.time()
    # Go's cmd/go/internal/cache allows an extra hour because a cache hit
    # refreshes the recorded mtime at most once an hour.
    # https://github.com/golang/go/blob/go1.27.4/src/cmd/go/internal/cache/cache.go#L337-L429
    trim_limit = 5 * 24 * 60 * 60
    cutoff = trim_limit + 60 * 60
    ages = [now - entry.stat().st_mtime for entry in cache.glob('[0-9a-f][0-9a-f]/*')
            if entry.name.endswith(('-a', '-d'))]
    past_trim_limit = sum(age > trim_limit for age in ages)
    past_cutoff = sum(age > cutoff for age in ages)
    print(f'Go build cache entries={len(ages)} older_than_5_days={past_trim_limit} '
          f'older_than_cutoff={past_cutoff} '
          f'max_age_seconds={max(ages, default=0):.0f}')
    marker = cache / 'trim.txt'
    if marker.is_file():
        print(f'Go build cache last_trim_epoch={int(marker.read_text())}')
