#!/usr/bin/env python3
from __future__ import annotations
import argparse, shutil, sys, tempfile, zipfile
from pathlib import Path

def main():
    ap = argparse.ArgumentParser(description="Install bundled custom bw2io package into the active Python environment")
    ap.add_argument('--zip', dest='zip_path', type=Path, default=Path(__file__).resolve().with_name('bw2io_custom_snapshot.zip'))
    args = ap.parse_args()
    z = args.zip_path
    if not z.exists():
        raise SystemExit(f"ZIP not found: {z}")
    try:
        import bw2io  # type: ignore
    except Exception as e:
        raise SystemExit(f"bw2io is not importable in this Python env: {e}")
    target = Path(bw2io.__file__).resolve().parent
    site = target.parent
    print(f"Target site-packages bw2io: {target}")
    with tempfile.TemporaryDirectory() as td:
        tdp = Path(td)
        with zipfile.ZipFile(z, 'r') as zf:
            zf.extractall(tdp)
        src = tdp / 'bw2io'
        if not src.exists():
            raise SystemExit('ZIP does not contain top-level bw2io/ folder')
        backup = target.with_name(target.name + '_backup_before_custom')
        if backup.exists():
            shutil.rmtree(backup)
        shutil.copytree(target, backup)
        tmp_new = site / 'bw2io_new_tmp_replace'
        if tmp_new.exists():
            shutil.rmtree(tmp_new)
        shutil.copytree(src, tmp_new)
        shutil.rmtree(target)
        tmp_new.rename(target)
    print('Custom bw2io installed successfully.')
    print(f'Backup created at: {backup}')

if __name__ == '__main__':
    main()
