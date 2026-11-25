#!/usr/bin/env python3
"""Isolate the runner build cache and verify the diagnostic's starting state."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("isolate", "cold", "integration"))
    phase = parser.parse_args().phase
    cache = Path(os.environ["GOCACHE"])
    if not cache.is_absolute() or cache == Path("/"):
        parser.error("expected an absolute Go build-cache directory")

    if phase == "isolate":
        if cache.exists() or cache.is_symlink():
            # GitHub's cache version depends on the original absolute path.
            backup = Path(tempfile.mkdtemp(prefix="unit-cache-backup-", dir=cache.parent))
            cache.rename(backup / cache.name)
        return

    go = json.loads(subprocess.check_output([
        "go", "env", "-json", "GOVERSION", "GOOS", "GOARCH", "GOAMD64",
        "CGO_ENABLED", "GOFLAGS", "GOTOOLCHAIN", "GOMODCACHE",
    ], text=True))
    checkout = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if phase == "cold" and (cache.exists() or cache.is_symlink()):
        parser.error("the cold build-cache path must be absent")

    files = [path.stat() for path in cache.glob("[0-9a-f][0-9a-f]/*")
             if path.name.endswith(("-a", "-d")) and path.is_file()]
    if phase == "integration" and (os.environ.get("CACHE_HIT") != "true" or not files):
        parser.error("expected an exact integration archive restore with Go cache entries")

    now = time.time()
    ages = [now - entry.st_mtime for entry in files]
    trim_marker = cache / "trim.txt"
    print(json.dumps({
        "case": phase,
        "checkout": checkout,
        "image": os.environ.get("ImageVersion"),
        "go": go,
        "observed_epoch": now,
        "entries": len(files),
        "bytes": sum(entry.st_size for entry in files),
        "entries_older_than_five_days": sum(age > 5 * 24 * 60 * 60 for age in ages),
        "oldest_entry_age_seconds": max(ages, default=None),
        "trim_marker": trim_marker.read_text().strip() if trim_marker.is_file() else None,
    }))


if __name__ == "__main__":
    main()
