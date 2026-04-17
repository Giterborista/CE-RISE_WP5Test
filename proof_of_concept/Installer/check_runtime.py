#!/usr/bin/env python3
from __future__ import annotations
import importlib
mods = ['bw2data','bw2calc','bw2io','openpyxl','matplotlib','jsonschema']
for m in mods:
    try:
        mod = importlib.import_module(m)
        print(f'{m}: OK ({getattr(mod, "__version__", "no __version__")})')
    except Exception as e:
        print(f'{m}: ERROR ({e})')
