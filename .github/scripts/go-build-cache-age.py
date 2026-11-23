"""Summarize Go build cache ages for the integration job log."""

import os
from pathlib import Path
import time


cache = Path(os.environ['GOCACHE'])
if not cache.is_dir():
    print('Go build cache directory is absent')
else:
    now = time.time()
    cutoff = 5 * 24 * 60 * 60 + 60 * 60
    ages = [now - entry.stat().st_mtime for entry in cache.glob('[0-9a-f][0-9a-f]/*')
            if entry.name.endswith(('-a', '-d'))]
    old = sum(age > cutoff for age in ages)
    print(f'Go build cache entries={len(ages)} older_than_cutoff={old} '
          f'max_age_seconds={max(ages, default=0):.0f}')
    marker = cache / 'trim.txt'
    if marker.is_file():
        print(f'Go build cache last_trim_epoch={int(marker.read_text())}')
