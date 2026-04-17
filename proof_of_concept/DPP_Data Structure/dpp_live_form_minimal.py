#!/usr/bin/env python3
"""Minimal strict local DPP JSON generator (form <-> single live JSON).

Rules implemented from user requirements:
- BoM is always stage Raw material acquisition (no year/location).
- Non-BoM stages use Activity description + Location + Reference year.
- DPP is a strict data carrier (no chain logic injected in JSON).
- Live JSON is always visible; valid export is blocked when required fields are missing.
- One JSON panel only (no source-form JSON panel).
"""

from __future__ import annotations

import argparse
import json
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
DEFAULT_DICTIONARIES = (
    Path(__file__).resolve().parents[1]
    / "dictionaries"
    / "dpp_dictionaries.json"
)
DEFAULT_BONSAI_INDEX = (
    Path(__file__).resolve().parents[1]
    / "dictionaries"
    / "bonsai_process_index.json"
)


def _load_dictionaries(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(
            f"Dictionary file not found: {path}. "
            "Create it first (JSON file with units/locations/locationMap/bonsaiProcesses/transports)."
        )
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    # Backward compatibility: old key `classifications` -> new user-facing `bonsaiProcesses`
    if "bonsaiProcesses" not in data and "classifications" in data:
        data["bonsaiProcesses"] = list(data.get("classifications") or [])

    required = ["units", "locations", "locationMap", "bonsaiProcesses", "transports"]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Dictionary JSON missing required keys: {', '.join(missing)}")
    for k in ["units", "locations", "bonsaiProcesses", "transports"]:
        if not isinstance(data.get(k), list):
            raise ValueError(f"Dictionary key '{k}' must be a list")
    if not isinstance(data.get("locationMap"), dict):
        raise ValueError("Dictionary key 'locationMap' must be an object")
    return data


def _load_bonsai_index(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(
            f"BONSAI index file not found: {path}. "
            "Generate it first from the live BONSAI database."
        )
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError("BONSAI index must be a JSON object")
    if not isinstance(data.get("entries"), dict):
        raise ValueError("BONSAI index JSON missing required object key 'entries'")
    for label, entry in data["entries"].items():
        if not isinstance(label, str) or not label.strip():
            raise ValueError("BONSAI index entry labels must be non-empty strings")
        if not isinstance(entry, dict):
            raise ValueError(f"BONSAI index entry '{label}' must be an object")
        for key in ["bonsaiProcess", "providerKind", "allowedLocations", "codeByLocation"]:
            if key not in entry:
                raise ValueError(f"BONSAI index entry '{label}' missing required key '{key}'")
        if entry.get("providerKind") not in {"market", "activity"}:
            raise ValueError(
                f"BONSAI index entry '{label}' has invalid providerKind '{entry.get('providerKind')}'"
            )
        if not isinstance(entry.get("allowedLocations"), list):
            raise ValueError(f"BONSAI index entry '{label}' allowedLocations must be a list")
        if not isinstance(entry.get("codeByLocation"), dict):
            raise ValueError(f"BONSAI index entry '{label}' codeByLocation must be an object")
    return data


def _html(dicts: dict, bonsai_index: dict) -> str:
    payload = json.dumps(dicts, ensure_ascii=False)
    bonsai_index_payload = json.dumps(bonsai_index, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>DPP Minimal Generator</title>
  <style>
    :root {{
      --bg:#eef2f7; --card:#fff; --line:#d1d5db; --text:#1f2937; --muted:#6b7280;
      --ok:#047857; --err:#b91c1c; --warn:#92400e; --blue:#1d4ed8;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; }}
    header {{ padding:16px 20px; background:linear-gradient(120deg,#eff6ff,#e0e7ff); border-bottom:1px solid var(--line); }}
    h1 {{ margin:0 0 4px; font-size:28px; }}
    .sub {{ margin:0; color:var(--muted); }}
    .toolbar {{ margin-top:10px; display:flex; gap:8px; flex-wrap:wrap; }}
    .btn {{ border:1px solid var(--line); border-radius:8px; background:#fff; padding:8px 12px; cursor:pointer; font-weight:600; }}
    .btn.primary {{ background:var(--blue); color:#fff; border-color:#1e40af; }}
    .wrap {{ display:grid; grid-template-columns:1.25fr 1fr; gap:12px; padding:12px; align-items:start; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; box-shadow:0 2px 6px rgba(0,0,0,.04); padding:12px; margin-bottom:12px; }}
    .card h2 {{ margin:0 0 8px; font-size:17px; border-bottom:1px solid #e5e7eb; padding-bottom:6px; }}
    .grid2 {{ display:grid; grid-template-columns:1fr 1fr; gap:8px; }}
    .field label {{ display:block; font-size:12px; color:var(--muted); margin-bottom:4px; }}
    input,select {{ width:100%; border:1px solid var(--line); border-radius:8px; padding:7px 8px; font-size:13px; }}
    table {{ width:100%; border-collapse:collapse; font-size:12px; }}
    th,td {{ border:1px solid #e5e7eb; padding:4px; vertical-align:top; }}
    th {{ background:#f8fafc; text-align:left; }}
    .search-select {{ position:relative; min-width:220px; }}
    .search-menu {{ display:none; position:absolute; left:0; right:0; top:calc(100% + 2px); z-index:50; max-height:220px; overflow:auto; background:#fff; border:1px solid #cbd5e1; border-radius:8px; box-shadow:0 8px 20px rgba(15,23,42,.12); }}
    .search-select.open .search-menu {{ display:block; }}
    .search-option {{ padding:6px 8px; cursor:pointer; border-bottom:1px solid #eef2f7; font-size:12px; line-height:1.25; }}
    .search-option:last-child {{ border-bottom:none; }}
    .search-option:hover, .search-option.active {{ background:#e8f0ff; }}
    .search-empty {{ padding:6px 8px; color:#64748b; font-size:12px; }}
    .mini-btn {{ border:1px solid var(--line); background:#fff; border-radius:6px; padding:4px 8px; font-size:12px; cursor:pointer; }}
    .del {{ color:var(--err); }}
    .status {{ font-size:13px; padding:8px; border-radius:8px; border:1px solid #e5e7eb; margin-bottom:8px; }}
    .status.ok {{ color:var(--ok); background:#ecfdf5; border-color:#a7f3d0; }}
    .status.err {{ color:var(--err); background:#fef2f2; border-color:#fecaca; }}
    .status.warn {{ color:var(--warn); background:#fffbeb; border-color:#fde68a; }}
    .errors {{ margin:0; padding-left:18px; color:var(--err); font-size:12px; }}
    pre {{ margin:0; background:#0b1220; color:#dbeafe; border:1px solid #111827; border-radius:10px; padding:10px; font-size:12px; max-height:78vh; overflow:auto; }}
    .pill {{ display:inline-block; border:1px solid #cbd5e1; border-radius:999px; padding:2px 8px; margin-right:4px; font-size:11px; color:#334155; background:#f8fafc; }}
    @media (max-width:1200px) {{ .wrap {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <header>
    <h1>DPP JSON Generator</h1>
    <div class="toolbar">
      <button class="btn" id="uploadBtn">Upload DPP JSON</button>
      <input id="uploadInput" type="file" accept=".json,application/json" style="display:none" />
      <button class="btn primary" id="refreshBtn">Refresh JSON now</button>
      <button class="btn" id="copyBtn">Copy JSON</button>
      <button class="btn" id="downloadBtn">Download JSON</button>
    </div>
  </header>

  <div class="wrap">
    <div>
      <section class="card">
        <h2>Product context</h2>
        <div class="grid2">
          <div class="field"><label>DPP code (auto)</label><input id="dppCode" readonly /></div>
          <div class="field"><label>Product name*</label><input id="productName" /></div>
          <div class="field"><label>Reference flow number*</label><input id="refNumber" type="number" step="any" value="1" /></div>
          <div class="field"><label>Declared unit*</label><select id="declaredUnit"></select></div>
        </div>
      </section>

      <section class="card">
        <h2>BoM activity (always Raw material acquisition)</h2>
        <div class="pill">No location</div><div class="pill">No reference year</div>
        <table>
          <thead>
            <tr><th>Description*</th><th>Quantity*</th><th>Unit*</th><th>Upstream</th><th>BONSAI process*</th><th>Source location*</th><th>Evidence</th><th>Notes</th><th></th></tr>
          </thead>
          <tbody id="bomRows"></tbody>
        </table>
        <button class="mini-btn" data-add="bomRows">+ Add row</button>
        <h3>Activity transport inputs (optional)</h3>
        <table><thead><tr><th>Transport mode*</th><th>Cost (MEUR)*</th><th>Source location*</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="bomTrRows"></tbody></table>
        <button class="mini-btn" data-add="bomTrRows">+ Add row</button>
      </section>

      <section class="card">
        <h2>Manufacturing (required)</h2>
        <div class="grid2">
          <div class="field"><label>Activity description*</label><input id="mDesc" /></div>
          <div class="field"><label>Reference year*</label><input id="mYear" type="number" /></div>
          <div class="field"><label>Location*</label><select id="mLoc"></select></div>
        </div>
        <h3>Activity inputs</h3>
        <table><thead><tr><th>Flow type*</th><th>Description*</th><th>Quantity*</th><th>Unit*</th><th>Upstream</th><th>BONSAI process*</th><th>Source location</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="mInRows"></tbody></table>
        <button class="mini-btn" data-add="mInRows">+ Add row</button>
        <h3>Activity outputs</h3>
        <table><thead><tr><th>Flow type*</th><th>Description*</th><th>Quantity*</th><th>Unit*</th><th>Upstream</th><th>BONSAI process</th><th>Source location</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="mOutRows"></tbody></table>
        <button class="mini-btn" data-add="mOutRows">+ Add row</button>
        <h3>Activity transport inputs (optional)</h3>
        <table><thead><tr><th>Transport mode*</th><th>Cost (MEUR)*</th><th>Source location*</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="mTrRows"></tbody></table>
        <button class="mini-btn" data-add="mTrRows">+ Add row</button>
      </section>

      <section class="card">
        <h2>Installation/distribution/retail (optional, transport-only)</h2>
        <div class="grid2">
          <div class="field"><label>Activity description</label><input id="iDesc" /></div>
          <div class="field"><label>Reference year</label><input id="iYear" type="number" /></div>
          <div class="field"><label>Location</label><select id="iLoc"></select></div>
          <div class="field"><label>Transport mode</label><select id="iMode"></select></div>
          <div class="field"><label>Transport cost (MEUR)</label><input id="iCost" type="number" step="any" /></div>
        </div>
      </section>

      <section class="card">
        <h2>Use (optional)</h2>
        <div class="grid2">
          <div class="field"><label>Activity description</label><input id="uDesc" /></div>
          <div class="field"><label>Reference year</label><input id="uYear" type="number" /></div>
          <div class="field"><label>Location</label><select id="uLoc"></select></div>
        </div>
        <h3>Activity inputs</h3>
        <table><thead><tr><th>Flow type*</th><th>Description*</th><th>Quantity*</th><th>Unit*</th><th>Upstream</th><th>BONSAI process*</th><th>Source location</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="uInRows"></tbody></table>
        <button class="mini-btn" data-add="uInRows">+ Add row</button>
        <h3>Activity outputs</h3>
        <table><thead><tr><th>Flow type*</th><th>Description*</th><th>Quantity*</th><th>Unit*</th><th>Upstream</th><th>BONSAI process</th><th>Source location</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="uOutRows"></tbody></table>
        <button class="mini-btn" data-add="uOutRows">+ Add row</button>
        <h3>Activity transport inputs (optional)</h3>
        <table><thead><tr><th>Transport mode*</th><th>Cost (MEUR)*</th><th>Source location*</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="uTrRows"></tbody></table>
        <button class="mini-btn" data-add="uTrRows">+ Add row</button>
      </section>

      <section class="card">
        <h2>Maintenance, repair, refurbishment (optional)</h2>
        <div class="grid2">
          <div class="field"><label>Activity description</label><input id="rDesc" /></div>
          <div class="field"><label>Reference year</label><input id="rYear" type="number" /></div>
          <div class="field"><label>Location</label><select id="rLoc"></select></div>
        </div>
        <h3>Activity inputs</h3>
        <table><thead><tr><th>Flow type*</th><th>Description*</th><th>Quantity*</th><th>Unit*</th><th>Upstream</th><th>BONSAI process*</th><th>Source location</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="rInRows"></tbody></table>
        <button class="mini-btn" data-add="rInRows">+ Add row</button>
        <h3>Activity outputs</h3>
        <table><thead><tr><th>Flow type*</th><th>Description*</th><th>Quantity*</th><th>Unit*</th><th>Upstream</th><th>BONSAI process</th><th>Source location</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="rOutRows"></tbody></table>
        <button class="mini-btn" data-add="rOutRows">+ Add row</button>
        <h3>Activity transport inputs (optional)</h3>
        <table><thead><tr><th>Transport mode*</th><th>Cost (MEUR)*</th><th>Source location*</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="rTrRows"></tbody></table>
        <button class="mini-btn" data-add="rTrRows">+ Add row</button>
      </section>

      <section class="card">
        <h2>End-of-life (optional)</h2>
        <div class="grid2">
          <div class="field"><label>Activity description</label><input id="eDesc" /></div>
          <div class="field"><label>Reference year</label><input id="eYear" type="number" /></div>
          <div class="field"><label>Location</label><select id="eLoc"></select></div>
        </div>
        <h3>Activity inputs</h3>
        <table><thead><tr><th>Flow type*</th><th>Description*</th><th>Quantity*</th><th>Unit*</th><th>Upstream</th><th>BONSAI process</th><th>Source location</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="eInRows"></tbody></table>
        <button class="mini-btn" data-add="eInRows">+ Add row</button>
        <h3>Activity outputs</h3>
        <table><thead><tr><th>Flow type*</th><th>Description*</th><th>Quantity*</th><th>Unit*</th><th>Upstream</th><th>BONSAI process</th><th>Source location</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="eOutRows"></tbody></table>
        <button class="mini-btn" data-add="eOutRows">+ Add row</button>
        <h3>Activity transport inputs (optional)</h3>
        <table><thead><tr><th>Transport mode*</th><th>Cost (MEUR)*</th><th>Source location*</th><th>Evidence</th><th>Notes</th><th></th></tr></thead><tbody id="eTrRows"></tbody></table>
        <button class="mini-btn" data-add="eTrRows">+ Add row</button>
      </section>
    </div>

    <div>
      <section class="card">
        <h2>Status</h2>
        <div id="status" class="status warn">Start typing. JSON updates live.</div>
        <ul id="errors" class="errors"></ul>
      </section>
      <section class="card">
        <h2>DPP JSON</h2>
        <pre id="jsonOut">{{}}</pre>
      </section>
    </div>
  </div>

  <script>
    const DICT = {payload};
    const BONSAI_INDEX = {bonsai_index_payload};
    const BONSAI_ENTRIES = BONSAI_INDEX.entries || {{}};
    const EVIDENCE = ["", "measured", "estimated", "calculated"];
    const FLOW_TYPES_INPUT = ["BONSAI process", "Transport mode"];
    const FLOW_TYPES_OUTPUT = ["BONSAI process", "Transport mode"];
    const UNIT_LABELS = {{
      C62: "piece",
      KGM: "kilogram",
      TNE: "tonne",
      KWH: "kilowatt-hour",
      TJ: "terajoule",
      KMT: "kilometre",
      TKM: "tonne-kilometre",
      LTR: "litre",
      MTQ: "cubic metre",
      GRM: "gram",
      MTR: "metre",
      MTK: "square metre",
      MJ: "megajoule"
    }};

    function norm(s) {{
      return String(s || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
    }}
    function simpleHash(s) {{
      let h = 0;
      const str = String(s || "");
      for (let i = 0; i < str.length; i += 1) {{
        h = ((h << 5) - h) + str.charCodeAt(i);
        h |= 0;
      }}
      return Math.abs(h);
    }}
    function autoDppCode(productName) {{
      const base = norm(productName) || "product";
      const code = String(simpleHash(base) % 10000).padStart(4, "0");
      return `DPP_${{base}}_${{code}}`;
    }}
    function toNum(v) {{
      if (v === null || v === undefined || String(v).trim() === "") return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    }}
    function normalizeLocationCode(code) {{
      const raw = String(code || "").trim().toUpperCase();
      if (!raw) return "";
      if (raw === "GLOBAL") return "GLO";
      return raw;
    }}
    function isoFromLocation(name) {{
      const s = String(name || "").trim();
      if (!s) return "";
      if ((s.length === 2 || s.length === 3) && /^[A-Za-z]+$/.test(s)) return normalizeLocationCode(s);
      const mapped = (DICT.locationMap && DICT.locationMap[s]) ? DICT.locationMap[s] : s.toUpperCase();
      return normalizeLocationCode(mapped);
    }}
    function hasAny(v) {{
      return !(v === null || v === undefined || String(v).trim() === "");
    }}
    function unitLabel(code) {{
      const c = String(code || "").trim().toUpperCase();
      return UNIT_LABELS[c] || c || "";
    }}
    function unitOptionObjects() {{
      return (DICT.units || []).map(u => ({{
        value: u,
        label: unitLabel(u)
      }}));
    }}
    function escHtml(s) {{
      return String(s || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }}
    function selectHtml(options, val) {{
      const opts = [...(options || [])];
      const isObj = opts.some(o => o && typeof o === "object" && "value" in o);
      if (String(val || "").trim()) {{
        const exists = isObj
          ? opts.some(o => String((o && o.value) || "") === String(val))
          : opts.some(o => String(o) === String(val));
        if (!exists) {{
          opts.push(isObj ? {{ value: val, label: unitLabel(val) }} : val);
        }}
      }}
      return opts.map(o => {{
        if (o && typeof o === "object" && "value" in o) {{
          const ov = String(o.value || "");
          const ol = String(o.label || ov);
          const sel = ov === String(val || "") ? "selected" : "";
          return `<option value="${{ov}}" ${{sel}}>${{ol}}</option>`;
        }}
        const sv = String(o);
        const sel = sv === String(val || "") ? "selected" : "";
        return `<option value="${{sv}}" ${{sel}}>${{sv}}</option>`;
      }}).join("");
    }}
    function uniqStrings(values) {{
      const out = [];
      const seen = new Set();
      for (const v of (values || [])) {{
        const s = String(v || "").trim();
        if (!s) continue;
        const k = s.toLowerCase();
        if (seen.has(k)) continue;
        seen.add(k);
        out.push(s);
      }}
      return out;
    }}
    function stripProcessDisplayLabel(value) {{
      return String(value || "").trim().replace(/\\s+\\((market for|activity)\\)\\s*$/i, "").trim();
    }}
    function processDisplayKind(value) {{
      const m = String(value || "").trim().match(/\\((market for|activity)\\)\\s*$/i);
      return m ? String(m[1] || "").toLowerCase() : "";
    }}
    function processKindFromCode(code) {{
      const token = String(code || "").trim().split("|", 1)[0].toUpperCase();
      if (!token) return "";
      return token.startsWith("M_") ? "market for" : "activity";
    }}
    function listHasProcessValue(list, value) {{
      const raw = stripProcessDisplayLabel(value).toLowerCase();
      if (!raw) return false;
      return uniqStrings(list || []).some(o => stripProcessDisplayLabel(o).toLowerCase() === raw);
    }}
    function pickDisplayProcessValue(rawValue, options, preferredKind = "") {{
      const raw = stripProcessDisplayLabel(rawValue);
      if (!raw) return "";
      const opts = uniqStrings(options || []);
      const exact = opts.find(o => String(o || "").trim().toLowerCase() === String(rawValue || "").trim().toLowerCase());
      if (exact) return exact;
      const matches = opts.filter(o => stripProcessDisplayLabel(o).toLowerCase() === raw.toLowerCase());
      if (!matches.length) return raw;
      if (preferredKind) {{
        const preferred = matches.find(o => processDisplayKind(o) === preferredKind);
        if (preferred) return preferred;
      }}
      const activity = matches.find(o => processDisplayKind(o) === "activity");
      if (activity) return activity;
      const market = matches.find(o => processDisplayKind(o) === "market for");
      if (market) return market;
      return matches[0];
    }}
    function bonsaiEntryForLabel(label) {{
      const key = String(label || "").trim();
      return key ? (BONSAI_ENTRIES[key] || null) : null;
    }}
    function bonsaiProcessOptionsForType(ft, tableId = "") {{
      const t = String(ft || "").trim();
      if (t === "Transport mode") return DICT.transports || [];
      const base = Array.isArray(DICT.bonsaiProcesses) ? DICT.bonsaiProcesses : [];
      const isActivityOutput = ["mOutRows", "uOutRows", "rOutRows", "eOutRows"].includes(String(tableId || "").trim());
      if (!isActivityOutput) return base;
      return uniqStrings([...(DICT.bonsaiProcesses || []), ...(DICT.eolScenarios || [])]);
    }}
    function preferredProcessKind(rawValue, ft, tableId = "") {{
      const t = String(ft || "").trim();
      if (t === "Transport mode") return "activity";
      const isActivityOutput = ["mOutRows", "uOutRows", "rOutRows", "eOutRows"].includes(String(tableId || "").trim());
      if (isActivityOutput && listHasProcessValue(DICT.eolScenarios || [], rawValue)) return "activity";
      return "";
    }}
    function displayProcessValue(rawValue, ft, tableId = "", bonsaiCode = "") {{
      const opts = bonsaiProcessOptionsForType(ft, tableId);
      const codeKind = processKindFromCode(bonsaiCode);
      return pickDisplayProcessValue(rawValue, opts, codeKind || preferredProcessKind(rawValue, ft, tableId));
    }}
    function displayTransportValue(rawValue, bonsaiCode = "") {{
      const codeKind = processKindFromCode(bonsaiCode);
      return pickDisplayProcessValue(rawValue, DICT.transports || [], codeKind || "activity");
    }}
    function locationNameFromIso(code) {{
      const c = normalizeLocationCode(code);
      if (!c) return "";
      for (const [name, iso] of Object.entries(DICT.locationMap || {{}})) {{
        if (normalizeLocationCode(iso) === c) return name;
      }}
      return c;
    }}
    function locationOptionObjectsForCodes(codes) {{
      return (codes || []).map(code => {{
        const normalized = normalizeLocationCode(code);
        return {{
          value: locationNameFromIso(normalized),
          label: locationNameFromIso(normalized),
        }};
      }});
    }}
    function resolvedSelectionForLabel(displayLabel, sourceLocationValue) {{
      const entry = bonsaiEntryForLabel(displayLabel);
      if (!entry) return null;
      const src = isoFromLocation(sourceLocationValue);
      const code = src ? String((entry.codeByLocation || {{}})[src] || "").trim() : "";
      return {{
        entry,
        bonsaiProcess: String(entry.bonsaiProcess || "").trim(),
        sourceLocation: src,
        bonsaiCode: code,
      }};
    }}
    function setSelectOptions(el, optionObjects, selectedValue = "") {{
      if (!el) return;
      const opts = [""].concat(optionObjects || []);
      el.innerHTML = selectHtml(opts, selectedValue || "");
    }}
    function currentRowDisplayValue(tr) {{
      if (!tr) return "";
      return String(
        tr.querySelector('[data-k="classif"]')?.value
        || tr.querySelector('[data-k="mode"]')?.value
        || ""
      ).trim();
    }}
    function syncRowBonsaiState(tr, tableId) {{
      if (!tr) return;
      const srcEl = tr.querySelector('[data-k="srcLoc"]');
      if (!srcEl) return;
      const entry = bonsaiEntryForLabel(currentRowDisplayValue(tr));
      if (!entry) {{
        tr.dataset.bonsaiCode = "";
        srcEl.disabled = true;
        setSelectOptions(srcEl, [], "");
        return;
      }}
      const allowed = Array.isArray(entry.allowedLocations) ? entry.allowedLocations : [];
      const currentIso = isoFromLocation(srcEl.value);
      const nextIso = allowed.includes(currentIso) ? currentIso : "";
      setSelectOptions(srcEl, locationOptionObjectsForCodes(allowed), locationNameFromIso(nextIso));
      srcEl.disabled = allowed.length === 0;
      tr.dataset.bonsaiCode = nextIso ? String((entry.codeByLocation || {{}})[nextIso] || "").trim() : "";
    }}
    function syncInstallationTransportState() {{
      const modeEl = g("iMode");
      const locEl = g("iLoc");
      if (!modeEl || !locEl) return;
      const entry = bonsaiEntryForLabel(modeEl.value);
      if (!entry) {{
        locEl.disabled = true;
        setSelectOptions(locEl, [], "");
        return;
      }}
      const allowed = Array.isArray(entry.allowedLocations) ? entry.allowedLocations : [];
      const currentIso = isoFromLocation(locEl.value);
      const nextIso = allowed.includes(currentIso) ? currentIso : "";
      setSelectOptions(locEl, locationOptionObjectsForCodes(allowed), locationNameFromIso(nextIso));
      locEl.disabled = allowed.length === 0;
    }}
    function rowTypeOptions(tableId) {{
      return tableId.endsWith("OutRows") ? FLOW_TYPES_OUTPUT : FLOW_TYPES_INPUT;
    }}
    function setClassifSelectOptions(tr, tableId) {{
      if (!tr) return;
      const ftEl = tr.querySelector('[data-k="ft"]');
      const clEl = tr.querySelector('[data-k="classif"]');
      if (!clEl) return;
      const ft = ftEl ? ftEl.value : "BONSAI process";
      const opts = bonsaiProcessOptionsForType(ft, tableId);
      if (clEl.tagName === "INPUT" && clEl.dataset.searchInput === "1") {{
        const wrap = clEl.closest('[data-search-select="1"]');
        setSearchMenuOptions(wrap, opts, clEl.value || "", clEl.value || "");
      }} else {{
        const oldVal = clEl.value || "";
        clEl.innerHTML = selectHtml(opts, oldVal);
      }}
    }}
    function setSearchMenuOptions(wrap, options, currentVal = "", filterText = "") {{
      if (!wrap) return;
      const menu = wrap.querySelector('[data-search-menu="1"]');
      if (!menu) return;
      const needle = String(filterText || "").trim().toLowerCase();
      const opts = uniqStrings(options || []);
      const filtered = needle ? opts.filter(o => String(o).toLowerCase().includes(needle)) : opts;
      if (!filtered.length) {{
        menu.innerHTML = `<div class="search-empty">No matching process</div>`;
        return;
      }}
      menu.innerHTML = filtered.map(o => {{
        const active = String(o) === String(currentVal || "") ? " active" : "";
        return `<div class="search-option${{active}}" data-search-option="1" data-value="${{escHtml(o)}}">${{escHtml(o)}}</div>`;
      }}).join("");
    }}
    function buildCell(spec, val) {{
      if (spec.t === "datalist") {{
        const current = String(val ?? spec.d ?? "");
        return `<div class="search-select" data-search-select="1"><input data-k="${{spec.k}}" data-search-input="1" type="search" value="${{escHtml(current)}}" placeholder="${{escHtml(spec.ph || "Search...")}}" autocomplete="off" /><div class="search-menu" data-search-menu="1"></div></div>`;
      }}
      if (spec.t === "select") {{
        return `<select data-k="${{spec.k}}">${{selectHtml(spec.o, val ?? spec.d ?? "")}}</select>`;
      }}
      if (spec.t === "num") {{
        return `<input data-k="${{spec.k}}" type="number" step="any" value="${{val ?? spec.d ?? ""}}" />`;
      }}
      return `<input data-k="${{spec.k}}" type="text" value="${{String(val ?? spec.d ?? "").replace(/"/g, "&quot;")}}" />`;
    }}

    const TABLE = {{
      bomRows: [
        {{k:"desc",t:"text"}}, {{k:"qty",t:"num"}}, {{k:"unit",t:"select",o:unitOptionObjects(),d:"KGM"}},
        {{k:"upstream",t:"select",o:["No"],d:"No"}}, {{k:"classif",t:"datalist",o:DICT.bonsaiProcesses,ph:"Search BONSAI process"}},
        {{k:"srcLoc",t:"select",o:[]}}, {{k:"evidence",t:"select",o:EVIDENCE,d:"measured"}}, {{k:"notes",t:"text"}}
      ],
      bomTrRows: [
        {{k:"mode",t:"select",o:DICT.transports}}, {{k:"cost",t:"num"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE,d:"measured"}}, {{k:"notes",t:"text"}}
      ],
      mInRows: [
        {{k:"ft",t:"select",o:FLOW_TYPES_INPUT,d:"BONSAI process"}}, {{k:"desc",t:"text"}}, {{k:"qty",t:"num"}},
        {{k:"unit",t:"select",o:unitOptionObjects(),d:"KWH"}}, {{k:"upstream",t:"select",o:["No"],d:"No"}},
        {{k:"classif",t:"datalist",o:DICT.bonsaiProcesses,ph:"Search BONSAI process"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE}}, {{k:"notes",t:"text"}}
      ],
      mOutRows: [
        {{k:"ft",t:"select",o:FLOW_TYPES_OUTPUT,d:"BONSAI process"}}, {{k:"desc",t:"text"}}, {{k:"qty",t:"num"}},
        {{k:"unit",t:"select",o:unitOptionObjects(),d:"KGM"}}, {{k:"upstream",t:"select",o:["No"],d:"No"}},
        {{k:"classif",t:"datalist",o:uniqStrings([...(DICT.bonsaiProcesses || []), ...(DICT.eolScenarios || [])]),ph:"Search BONSAI process / EoL scenario"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE}}, {{k:"notes",t:"text"}}
      ],
      mTrRows: [
        {{k:"mode",t:"select",o:DICT.transports}}, {{k:"cost",t:"num"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE,d:"measured"}}, {{k:"notes",t:"text"}}
      ],
      uInRows: [
        {{k:"ft",t:"select",o:FLOW_TYPES_INPUT,d:"BONSAI process"}}, {{k:"desc",t:"text"}}, {{k:"qty",t:"num"}},
        {{k:"unit",t:"select",o:unitOptionObjects(),d:"KWH"}}, {{k:"upstream",t:"select",o:["No"],d:"No"}},
        {{k:"classif",t:"datalist",o:DICT.bonsaiProcesses,ph:"Search BONSAI process"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE}}, {{k:"notes",t:"text"}}
      ],
      uOutRows: [
        {{k:"ft",t:"select",o:FLOW_TYPES_OUTPUT,d:"BONSAI process"}}, {{k:"desc",t:"text"}}, {{k:"qty",t:"num"}},
        {{k:"unit",t:"select",o:unitOptionObjects(),d:"KGM"}}, {{k:"upstream",t:"select",o:["No"],d:"No"}},
        {{k:"classif",t:"datalist",o:uniqStrings([...(DICT.bonsaiProcesses || []), ...(DICT.eolScenarios || [])]),ph:"Search BONSAI process / EoL scenario"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE}}, {{k:"notes",t:"text"}}
      ],
      uTrRows: [
        {{k:"mode",t:"select",o:DICT.transports}}, {{k:"cost",t:"num"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE,d:"measured"}}, {{k:"notes",t:"text"}}
      ],
      rInRows: [
        {{k:"ft",t:"select",o:FLOW_TYPES_INPUT,d:"BONSAI process"}}, {{k:"desc",t:"text"}}, {{k:"qty",t:"num"}},
        {{k:"unit",t:"select",o:unitOptionObjects(),d:"KGM"}}, {{k:"upstream",t:"select",o:["No"],d:"No"}},
        {{k:"classif",t:"datalist",o:DICT.bonsaiProcesses,ph:"Search BONSAI process"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE}}, {{k:"notes",t:"text"}}
      ],
      rOutRows: [
        {{k:"ft",t:"select",o:FLOW_TYPES_OUTPUT,d:"BONSAI process"}}, {{k:"desc",t:"text"}}, {{k:"qty",t:"num"}},
        {{k:"unit",t:"select",o:unitOptionObjects(),d:"KGM"}}, {{k:"upstream",t:"select",o:["No"],d:"No"}},
        {{k:"classif",t:"datalist",o:uniqStrings([...(DICT.bonsaiProcesses || []), ...(DICT.eolScenarios || [])]),ph:"Search BONSAI process / EoL scenario"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE}}, {{k:"notes",t:"text"}}
      ],
      rTrRows: [
        {{k:"mode",t:"select",o:DICT.transports}}, {{k:"cost",t:"num"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE,d:"measured"}}, {{k:"notes",t:"text"}}
      ],
      eInRows: [
        {{k:"ft",t:"select",o:FLOW_TYPES_INPUT,d:"BONSAI process"}}, {{k:"desc",t:"text"}}, {{k:"qty",t:"num"}},
        {{k:"unit",t:"select",o:unitOptionObjects(),d:"KGM"}}, {{k:"upstream",t:"select",o:["No"],d:"No"}},
        {{k:"classif",t:"datalist",o:DICT.bonsaiProcesses,ph:"Search BONSAI process"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE}}, {{k:"notes",t:"text"}}
      ],
      eOutRows: [
        {{k:"ft",t:"select",o:FLOW_TYPES_OUTPUT,d:"BONSAI process"}}, {{k:"desc",t:"text"}}, {{k:"qty",t:"num"}},
        {{k:"unit",t:"select",o:unitOptionObjects(),d:"KGM"}}, {{k:"upstream",t:"select",o:["No"],d:"No"}},
        {{k:"classif",t:"datalist",o:uniqStrings([...(DICT.bonsaiProcesses || []), ...(DICT.eolScenarios || [])]),ph:"Search BONSAI process / EoL scenario"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE}}, {{k:"notes",t:"text"}}
      ],
      eTrRows: [
        {{k:"mode",t:"select",o:DICT.transports}}, {{k:"cost",t:"num"}}, {{k:"srcLoc",t:"select",o:[]}},
        {{k:"evidence",t:"select",o:EVIDENCE,d:"measured"}}, {{k:"notes",t:"text"}}
      ],
    }};

    function addRow(tableId, data = null) {{
      const schema = TABLE[tableId];
      const tb = document.getElementById(tableId);
      if (!schema || !tb) return;
      const tr = document.createElement("tr");
      tr.innerHTML = schema.map(s => `<td>${{buildCell(s, data ? data[s.k] : undefined)}}</td>`).join("")
        + `<td><button class="mini-btn del" data-del="1">✕</button></td>`;
      tb.appendChild(tr);
      if (data && data.bonsaiCode) tr.dataset.bonsaiCode = String(data.bonsaiCode);
      setClassifSelectOptions(tr, tableId);
      syncRowBonsaiState(tr, tableId);
    }}
    function clearRows(tableId) {{
      const tb = document.getElementById(tableId);
      if (tb) tb.innerHTML = "";
    }}
    function setValue(id, value) {{
      const el = g(id);
      if (!el) return;
      el.value = (value === null || value === undefined) ? "" : String(value);
      if (el.tagName === "SELECT") {{
        const has = Array.from(el.options).some(o => String(o.value) === String(el.value));
        if (!has && String(el.value).trim()) {{
          const opt = document.createElement("option");
          opt.value = String(el.value);
          opt.textContent = String(el.value);
          el.appendChild(opt);
        }}
      }}
    }}
    function inferFlowType(flow, fo) {{
      const proc = String((flow.bonsaiProcess || flow.classification || fo.bonsaiProcess || fo.classification || "")).trim();
      if (listHasProcessValue(DICT.transports || [], proc)) return "Transport mode";
      return "BONSAI process";
    }}
    function inferTransportRows(flows, detId, fos) {{
      const rows = [];
      for (const f of (flows || [])) {{
        if (!f || f.flowId === detId) continue;
        const fo = fos[f.flowObjectId] || {{}};
        const ft = inferFlowType(f, fo);
        if (ft !== "Transport mode") continue;
        rows.push({{
          mode: displayTransportValue(
            f.bonsaiProcess || f.classification || fo.bonsaiProcess || fo.classification || fo.name || "",
            f.bonsaiCode || "",
          ),
          cost: f.amount ?? "",
          srcLoc: locationNameFromIso(f.sourceLocation || ""),
          bonsaiCode: f.bonsaiCode || "",
          evidence: f.evidenceMethod || "measured",
          notes: f.notes || "",
        }});
      }}
      return rows;
    }}
    function rowFromFlow(flow, fo, tableId = "") {{
      const ft = inferFlowType(flow, fo);
      const rawProcess = flow.bonsaiProcess || flow.classification || fo.bonsaiProcess || fo.classification || "";
      return {{
        ft,
        desc: fo.name || "",
        qty: flow.amount ?? "",
        unit: flow.unit || "",
        upstream: "No",
        classif: displayProcessValue(rawProcess, ft, tableId, flow.bonsaiCode || ""),
        srcLoc: locationNameFromIso(flow.sourceLocation || ""),
        bonsaiCode: flow.bonsaiCode || "",
        evidence: flow.evidenceMethod || "",
        notes: flow.notes || "",
      }};
    }}
    function populateRows(tableId, rows) {{
      clearRows(tableId);
      for (const r of rows || []) addRow(tableId, r);
    }}
    function readRows(tableId) {{
      const schema = TABLE[tableId];
      const tb = document.getElementById(tableId);
      if (!schema || !tb) return [];
      const out = [];
      for (const tr of tb.querySelectorAll("tr")) {{
        const row = {{}};
        let any = false;
        for (const s of schema) {{
          const el = tr.querySelector(`[data-k="${{s.k}}"]`);
          if (!el) continue;
          let v = el.value;
          if (s.t === "num") v = (v === "") ? null : Number(v);
          row[s.k] = v;
          if (!(v === null || v === "")) any = true;
        }}
        if (any) {{
          row.bonsaiCode = String(tr.dataset.bonsaiCode || "").trim();
          out.push(row);
        }}
      }}
      return out;
    }}
    function g(id) {{ return document.getElementById(id); }}
    function gv(id) {{ return g(id)?.value ?? ""; }}

    function collect() {{
      return {{
        product: {{
          name: gv("productName"),
          refNum: toNum(gv("refNumber")),
          unit: gv("declaredUnit"),
        }},
        bom: readRows("bomRows"),
        bomTransport: readRows("bomTrRows"),
        manufacturing: {{
          desc: gv("mDesc"), year: toNum(gv("mYear")), loc: gv("mLoc"),
          inputs: readRows("mInRows"), outputs: readRows("mOutRows"), transport: readRows("mTrRows"),
        }},
        installation: {{
          desc: gv("iDesc"), year: toNum(gv("iYear")), loc: gv("iLoc"),
          mode: gv("iMode"), cost: toNum(gv("iCost"))
        }},
        use: {{
          desc: gv("uDesc"), year: toNum(gv("uYear")), loc: gv("uLoc"),
          inputs: readRows("uInRows"), outputs: readRows("uOutRows"), transport: readRows("uTrRows"),
        }},
        maint: {{
          desc: gv("rDesc"), year: toNum(gv("rYear")), loc: gv("rLoc"),
          inputs: readRows("rInRows"), outputs: readRows("rOutRows"), transport: readRows("rTrRows"),
        }},
        eol: {{
          desc: gv("eDesc"), year: toNum(gv("eYear")), loc: gv("eLoc"),
          inputs: readRows("eInRows"), outputs: readRows("eOutRows"), transport: readRows("eTrRows"),
        }},
      }};
    }}
    function parseActivityBase(act) {{
      const stage = String(act.LCStage || "");
      const at = String(act.ActivityType || "").trim();
      let desc = at;
      if (!desc) {{
        const a = String(act.Activity || "").trim();
        if (stage === "Raw material acquisition" && / - BoM$/i.test(a)) {{
          desc = a.replace(/ - BoM$/i, "").trim() + " - BoM";
        }} else if (a) {{
          const m = a.match(/^(.*),\\s*[^,]+ in \\d{{4}}$/);
          desc = m ? m[1].trim() : a;
        }}
      }}
      return {{
        desc,
        year: act.ReferenceYear ?? "",
        loc: locationNameFromIso(act.Place || ""),
      }};
    }}
    function loadDppToForm(dpp) {{
      const fos = {{}};
      for (const fo of (dpp.flowObjects || [])) {{
        if (fo && fo.flowObjectId) fos[fo.flowObjectId] = fo;
      }}

      setValue("dppCode", dpp.dppId || "");
      const acts = Array.isArray(dpp.activities) ? dpp.activities : [];
      const byStage = {{}};
      for (const a of acts) {{
        const st = String(a.LCStage || "");
        if (!byStage[st]) byStage[st] = a;
      }}
      const bom = byStage["Raw material acquisition"] || null;
      const manuf = byStage["Manufacturing"] || null;
      const inst = byStage["Installation/distribution/retail"] || null;
      const use = byStage["Use"] || null;
      const maint = byStage["Maintenance, repair, refurbishment"] || null;
      const eol = byStage["End-of-life"] || null;

      if (bom) {{
        const pname = String(bom.Activity || "").replace(/ - BoM$/i, "").trim();
        setValue("productName", pname || "");
        const det = (bom.flows || []).find(f => f.flowId === bom.determiningFlowId);
        setValue("refNumber", det?.amount ?? 1);
        setValue("declaredUnit", det?.unit || "C62");
        const bomRows = (bom.flows || [])
          .filter(f => f.flowId !== bom.determiningFlowId && f.direction === "input")
          .map(f => rowFromFlow(f, fos[f.flowObjectId] || {{}}, "bomRows"));
        populateRows("bomRows", bomRows.filter(r => r.ft !== "Transport mode"));
        populateRows("bomTrRows", inferTransportRows(bom.flows || [], bom.determiningFlowId, fos));
      }} else {{
        populateRows("bomRows", []);
        populateRows("bomTrRows", []);
      }}

      if (manuf) {{
        const b = parseActivityBase(manuf);
        setValue("mDesc", b.desc);
        setValue("mYear", b.year);
        setValue("mLoc", b.loc);
        const ins = (manuf.flows || [])
          .filter(f => f.flowId !== manuf.determiningFlowId && f.direction === "input")
          .map(f => rowFromFlow(f, fos[f.flowObjectId] || {{}}, "mInRows"));
        const outs = (manuf.flows || [])
          .filter(f => f.flowId !== manuf.determiningFlowId && f.direction === "output")
          .map(f => rowFromFlow(f, fos[f.flowObjectId] || {{}}, "mOutRows"));
        populateRows("mInRows", ins.filter(r => r.ft !== "Transport mode"));
        populateRows("mOutRows", outs.filter(r => r.ft !== "Transport mode"));
        populateRows("mTrRows", inferTransportRows(manuf.flows || [], manuf.determiningFlowId, fos));
      }} else {{
        setValue("mDesc", "");
        setValue("mYear", "");
        setValue("mLoc", "");
        populateRows("mInRows", []);
        populateRows("mOutRows", []);
        populateRows("mTrRows", []);
      }}

      if (inst) {{
        const b = parseActivityBase(inst);
        setValue("iDesc", b.desc);
        setValue("iYear", b.year);
        const tf = (inst.flows || []).find(f => f.flowId !== inst.determiningFlowId && f.direction === "input");
        if (tf) {{
          const fo = fos[tf.flowObjectId] || {{}};
          setValue("iMode", displayTransportValue(fo.name || tf.bonsaiProcess || tf.classification || "", tf.bonsaiCode || ""));
          syncInstallationTransportState();
          setValue("iLoc", b.loc);
          syncInstallationTransportState();
          setValue("iCost", tf.amount ?? "");
        }} else {{
          setValue("iMode", "");
          syncInstallationTransportState();
          setValue("iLoc", b.loc);
          syncInstallationTransportState();
          setValue("iCost", "");
        }}
      }} else {{
        setValue("iDesc", "");
        setValue("iYear", "");
        setValue("iMode", "");
        syncInstallationTransportState();
        setValue("iLoc", "");
        syncInstallationTransportState();
        setValue("iCost", "");
      }}

      if (use) {{
        const b = parseActivityBase(use);
        setValue("uDesc", b.desc);
        setValue("uYear", b.year);
        setValue("uLoc", b.loc);
        const rows = (use.flows || [])
          .filter(f => f.flowId !== use.determiningFlowId && f.direction === "input")
          .map(f => rowFromFlow(f, fos[f.flowObjectId] || {{}}, "uInRows"));
        const outRows = (use.flows || [])
          .filter(f => f.flowId !== use.determiningFlowId && f.direction === "output")
          .map(f => rowFromFlow(f, fos[f.flowObjectId] || {{}}, "uOutRows"));
        populateRows("uInRows", rows.filter(r => r.ft !== "Transport mode"));
        populateRows("uOutRows", outRows.filter(r => r.ft !== "Transport mode"));
        populateRows("uTrRows", inferTransportRows(use.flows || [], use.determiningFlowId, fos));
      }} else {{
        setValue("uDesc", "");
        setValue("uYear", "");
        setValue("uLoc", "");
        populateRows("uInRows", []);
        populateRows("uOutRows", []);
        populateRows("uTrRows", []);
      }}

      if (maint) {{
        const b = parseActivityBase(maint);
        setValue("rDesc", b.desc);
        setValue("rYear", b.year);
        setValue("rLoc", b.loc);
        const ins = (maint.flows || [])
          .filter(f => f.flowId !== maint.determiningFlowId && f.direction === "input")
          .map(f => rowFromFlow(f, fos[f.flowObjectId] || {{}}, "rInRows"));
        const outs = (maint.flows || [])
          .filter(f => f.flowId !== maint.determiningFlowId && f.direction === "output")
          .map(f => rowFromFlow(f, fos[f.flowObjectId] || {{}}, "rOutRows"));
        populateRows("rInRows", ins.filter(r => r.ft !== "Transport mode"));
        populateRows("rOutRows", outs.filter(r => r.ft !== "Transport mode"));
        populateRows("rTrRows", inferTransportRows(maint.flows || [], maint.determiningFlowId, fos));
      }} else {{
        setValue("rDesc", "");
        setValue("rYear", "");
        setValue("rLoc", "");
        populateRows("rInRows", []);
        populateRows("rOutRows", []);
        populateRows("rTrRows", []);
      }}

      if (eol) {{
        const b = parseActivityBase(eol);
        setValue("eDesc", b.desc);
        setValue("eYear", b.year);
        setValue("eLoc", b.loc);
        const eInRows = (eol.flows || [])
          .filter(f => f.flowId !== eol.determiningFlowId && f.direction === "input")
          .map(f => rowFromFlow(f, fos[f.flowObjectId] || {{}}, "eInRows"));
        const eOutRows = (eol.flows || [])
          .filter(f => f.flowId !== eol.determiningFlowId && f.direction === "output")
          .map(f => rowFromFlow(f, fos[f.flowObjectId] || {{}}, "eOutRows"));
        populateRows("eInRows", eInRows.filter(r => r.ft !== "Transport mode"));
        populateRows("eOutRows", eOutRows.filter(r => r.ft !== "Transport mode"));
        populateRows("eTrRows", inferTransportRows(eol.flows || [], eol.determiningFlowId, fos));
      }} else {{
        setValue("eDesc", "");
        setValue("eYear", "");
        setValue("eLoc", "");
        populateRows("eInRows", []);
        populateRows("eOutRows", []);
        populateRows("eTrRows", []);
      }}
    }}

    function buildDpp(state) {{
      const errors = [];
      const flowObjects = {{}};
      const activities = [];
      const usedActIds = new Set();

      const pname = String(state.product.name || "").trim();
      const refNum = state.product.refNum;
      const unit = String(state.product.unit || "").trim();
      const m = state.manufacturing;
      const i = state.installation;
      const u = state.use;
      const r = state.maint;
      const e = state.eol;
      const mStarted = hasAny(m.desc) || m.year !== null || hasAny(m.loc) || m.inputs.length > 0 || m.outputs.length > 0 || m.transport.length > 0;
      const iActive = hasAny(i.mode) || i.cost !== null || hasAny(i.loc) || hasAny(i.year);
      const uActive = u.inputs.length > 0 || u.outputs.length > 0 || u.transport.length > 0 || hasAny(u.loc) || u.year !== null;
      const rActive = r.inputs.length > 0 || r.outputs.length > 0 || r.transport.length > 0 || hasAny(r.loc) || r.year !== null;
      const eActive = e.inputs.length > 0 || e.outputs.length > 0 || e.transport.length > 0 || hasAny(e.loc) || e.year !== null;
      const hasAnyInput = hasAny(pname) || state.bom.length > 0 || state.bomTransport.length > 0 || mStarted || iActive || uActive || rActive || eActive;

      if (!hasAnyInput) {{
        return {{ errors: [], dpp: {{}} }};
      }}

      if (!pname) errors.push("Product name is required.");
      if (!(refNum > 0)) errors.push("Reference flow number must be > 0.");
      if (!unit) errors.push("Declared unit is required.");

      function ensureFO(name, objectClass, suffix = "") {{
        let id = "";
        if (objectClass === "primary" && suffix) {{
          id = `fo_p_${{norm(suffix)}}`;
        }} else {{
          const base = `fo_${{objectClass}}_${{norm(name) || "item"}}`;
          id = suffix ? `${{base}}_${{norm(suffix)}}` : base;
        }}
        if (!flowObjects[id]) {{
          flowObjects[id] = {{ flowObjectId: id, name, objectClass }};
        }}
        return id;
      }}
      function makeActId(fullName) {{
        const base = `act_${{norm(fullName) || "activity"}}`;
        if (!usedActIds.has(base)) {{ usedActIds.add(base); return base; }}
        let i = 2;
        while (usedActIds.has(`${{base}}_${{i}}`)) i++;
        const id = `${{base}}_${{i}}`;
        usedActIds.add(id);
        return id;
      }}
      function mkActivity(desc, stage, loc, year, noContext = false) {{
        if (!desc) errors.push(`${{stage}}: Activity description is required.`);
        let full = desc || stage;
        let place = "";
        let refYear = null;
        if (!noContext) {{
          if (!loc) errors.push(`${{stage}}: Location is required.`);
          if (!(year > 0)) errors.push(`${{stage}}: Reference year is required.`);
          full = `${{desc}}, ${{loc}} in ${{year}}`;
          place = isoFromLocation(loc);
          refYear = year;
        }}
        const activityId = makeActId(full);
        const primaryName = `1 C62 output from ${{stage}}`;
        const pfo = ensureFO(primaryName, "primary", activityId);
        const flowPrefix = `fl_${{activityId.replace(/^act_/, "")}}`;
        const detFlowId = `${{flowPrefix}}_001`;
        const activity = {{
          activityId,
          Activity: full,
          ActivityType: desc || stage,
          LCStage: stage,
          flows: [{{
            flowId: detFlowId,
            flowObjectId: pfo,
            direction: "output",
            amount: 1,
            unit: "C62",
            isDetermining: true,
            notes: "Determining flow (primary output) for this activity"
          }}],
          determiningFlowId: detFlowId,
          _cnt: 1,
          _flowPrefix: flowPrefix,
          _pfo: pfo,
        }};
        if (!noContext && place) activity.Place = place;
        if (!noContext && refYear !== null) activity.ReferenceYear = refYear;
        return activity;
      }}
      function nextFlowId(a) {{
        a._cnt += 1;
        return `${{a._flowPrefix}}_${{String(a._cnt).padStart(3, "0")}}`;
      }}
      function resolveIndexedProcess(displayLabel, srcLoc, section) {{
        const label = String(displayLabel || "").trim();
        if (!label) {{
          errors.push(`${{section}}: BONSAI process is required.`);
          return null;
        }}
        const resolved = resolvedSelectionForLabel(label, srcLoc);
        if (!resolved || !resolved.entry) {{
          errors.push(`${{section}}: selected BONSAI process '${{label}}' is not available in the BONSAI index.`);
          return null;
        }}
        if (!resolved.sourceLocation) {{
          errors.push(`${{section}}: source location is required.`);
          return null;
        }}
        if (!resolved.bonsaiCode) {{
          errors.push(
            `${{section}}: source location '${{resolved.sourceLocation}}' is not available for '${{label}}'.`
          );
          return null;
        }}
        return resolved;
      }}
      function addRowFlows(a, rows, direction, section, requireClass = false) {{
        for (const r of rows || []) {{
          const desc = String(r.desc || "").trim();
          const qty = toNum(r.qty);
          const u = String(r.unit || "").trim();
          if (!desc && qty === null && !u) continue;
          if (!desc || qty === null || !u) {{
            errors.push(`${{section}}: each row requires Description, Quantity, Unit.`);
            continue;
          }}
          const classif = String(r.classif || "").trim();
          const ft = String(r.ft || "").trim();
          if (requireClass && !classif) {{
            errors.push(`${{section}}: BONSAI process is required.`);
            continue;
          }}
          // BONSAI-process rows remain technosphere-like flows in this strict minimal tool.
          // Do not auto-promote output rows to elementary; that breaks round-trip imports.
          let oc = "secondary";
          const resolved = resolveIndexedProcess(classif, r.srcLoc, section);
          if (!resolved) continue;
          const fo = ensureFO(desc, oc);
          const f = {{
            flowId: nextFlowId(a),
            flowObjectId: fo,
            direction,
            amount: qty,
            unit: u.toUpperCase(),
            bonsaiProcess: resolved.bonsaiProcess,
            sourceLocation: resolved.sourceLocation,
            bonsaiCode: resolved.bonsaiCode,
          }};
          if (hasAny(r.evidence)) f.evidenceMethod = String(r.evidence).toLowerCase();
          if (hasAny(r.notes)) f.notes = String(r.notes);
          a.flows.push(f);
        }}
      }}
      function addTransportFlows(a, rows, section, defaultLoc = "") {{
        for (const r of rows || []) {{
          const mode = String(r.mode || "").trim();
          const modeRaw = stripProcessDisplayLabel(mode);
          const cost = toNum(r.cost);
          if (!modeRaw && cost === null) continue;
          if (!modeRaw || cost === null) {{
            errors.push(`${{section}}: each transport row requires Transport mode and Cost (MEUR).`);
            continue;
          }}
          const src = isoFromLocation(String(r.srcLoc || "").trim() || defaultLoc || "");
          if (!src) {{
            errors.push(`${{section}}: source location is required for transport rows.`);
            continue;
          }}
          const resolved = resolveIndexedProcess(mode, src, section);
          if (!resolved) continue;
          const fo = ensureFO(modeRaw, "secondary");
          const f = {{
            flowId: nextFlowId(a),
            flowObjectId: fo,
            direction: "input",
            amount: cost,
            unit: "MEUR",
            bonsaiProcess: resolved.bonsaiProcess,
            sourceLocation: resolved.sourceLocation,
            bonsaiCode: resolved.bonsaiCode,
            evidenceMethod: String(r.evidence || "measured").toLowerCase(),
          }};
          if (hasAny(r.notes)) f.notes = String(r.notes);
          a.flows.push(f);
        }}
      }}

      // BoM (required in final valid JSON)
      let bom = null;
      if (!state.bom.length) {{
        errors.push("BoM: at least one row is required.");
      }} else {{
        bom = mkActivity(`${{pname || "Product"}} - BoM`, "Raw material acquisition", "", null, true);
        addRowFlows(bom, state.bom, "input", "BoM", true);
        addTransportFlows(bom, state.bomTransport, "BoM transport");
        activities.push(bom);
      }}

      // Manufacturing (required in final valid JSON)
      let manuf = null;
      if (!mStarted) {{
        errors.push("Manufacturing: activity information is required.");
      }} else {{
        if (!(m.inputs.length || m.outputs.length)) {{
          errors.push("Manufacturing: at least one input/output row is required.");
        }}
        manuf = mkActivity(m.desc, "Manufacturing", m.loc, m.year, false);
        addRowFlows(manuf, m.inputs, "input", "Manufacturing inputs", true);
        addRowFlows(manuf, m.outputs, "output", "Manufacturing outputs", false);
        addTransportFlows(manuf, m.transport, "Manufacturing transport", m.loc);
        activities.push(manuf);
      }}
      // Installation/distribution/retail (optional transport-only)
      if (iActive) {{
        if (!hasAny(i.mode) || i.cost === null || !hasAny(i.loc) || !(i.year > 0) || !hasAny(i.desc)) {{
          errors.push("Installation/distribution/retail: description, year, location, transport mode, and cost (MEUR) are all required when active.");
        }} else {{
          const act = mkActivity(i.desc, "Installation/distribution/retail", i.loc, i.year, false);
          addTransportFlows(act, [{{ mode: i.mode, cost: i.cost, srcLoc: i.loc, evidence: "measured", notes: "Installation transport" }}], "Installation transport", i.loc);
          activities.push(act);
        }}
      }}

      // Use (optional)
      if (uActive) {{
        if (!hasAny(u.desc) || !hasAny(u.loc) || !(u.year > 0)) {{
          errors.push("Use: description, year, location are required when active.");
        }} else {{
          const act = mkActivity(u.desc, "Use", u.loc, u.year, false);
          addRowFlows(act, u.inputs, "input", "Use inputs", true);
          addRowFlows(act, u.outputs, "output", "Use outputs", false);
          addTransportFlows(act, u.transport, "Use transport", u.loc);
          activities.push(act);
        }}
      }}

      // Maintenance (optional)
      if (rActive) {{
        if (!hasAny(r.desc) || !hasAny(r.loc) || !(r.year > 0)) {{
          errors.push("Maintenance/refurbishment: description, year, location are required when active.");
        }} else {{
          const act = mkActivity(r.desc, "Maintenance, repair, refurbishment", r.loc, r.year, false);
          addRowFlows(act, r.inputs, "input", "Maintenance inputs", true);
          addRowFlows(act, r.outputs, "output", "Maintenance outputs", false);
          addTransportFlows(act, r.transport, "Maintenance transport", r.loc);
          activities.push(act);
        }}
      }}

      // End-of-life (optional)
      if (eActive) {{
        if (!hasAny(e.desc) || !hasAny(e.loc) || !(e.year > 0)) {{
          errors.push("End-of-life: description, year, and location are required when active.");
        }} else {{
          const act = mkActivity(e.desc, "End-of-life", e.loc, e.year, false);
          addRowFlows(act, e.inputs, "input", "End-of-life inputs", false);
          addRowFlows(act, e.outputs, "output", "End-of-life outputs", false);
          addTransportFlows(act, e.transport, "End-of-life transport", e.loc);
          activities.push(act);
        }}
      }}

      for (const a of activities) {{
        delete a._cnt;
        delete a._flowPrefix;
        delete a._pfo;
      }}

      const generatedDppCode = autoDppCode(pname);
      const dpp = {{
        schemaVersion: "1.0.0",
        updatedAt: new Date().toISOString(),
        dppId: generatedDppCode,
        flowObjects: Object.values(flowObjects),
        activities
      }};

      setValue("dppCode", generatedDppCode);
      return {{ errors, dpp }};
    }}

    function setStatus(kind, txt) {{
      const el = g("status");
      el.className = "status " + kind;
      el.textContent = txt;
    }}
    function renderErrors(list) {{
      const ul = g("errors");
      ul.innerHTML = "";
      for (const e of list || []) {{
        const li = document.createElement("li");
        li.textContent = e;
        ul.appendChild(li);
      }}
    }}

    function refreshJson() {{
      const state = collect();
      setValue("dppCode", autoDppCode(state.product.name || ""));
      const out = buildDpp(state);
      g("jsonOut").textContent = JSON.stringify(out.dpp, null, 2);
      if (!out.errors.length && Object.keys(out.dpp || {{}}).length === 0) {{
        setStatus("warn", "No input yet. Start filling fields.");
        renderErrors([]);
        window.__VALID_DPP = null;
      }} else if (out.errors.length) {{
        setStatus("err", `Blocked: ${{out.errors.length}} issue(s).`);
        renderErrors(out.errors);
        window.__VALID_DPP = null;
      }} else {{
        setStatus("ok", "JSON generated.");
        renderErrors([]);
        window.__VALID_DPP = out.dpp;
      }}
    }}

    function initSelect(id, values, defaultVal = "") {{
      const el = g(id);
      if (!el) return;
      const opts = ["", ...(values || [])];
      el.innerHTML = selectHtml(opts, defaultVal);
    }}

    function init() {{
      initSelect("declaredUnit", unitOptionObjects(), "C62");
      initSelect("mLoc", DICT.locations, "");
      initSelect("uLoc", DICT.locations, "");
      initSelect("rLoc", DICT.locations, "");
      initSelect("eLoc", DICT.locations, "");
      initSelect("iMode", DICT.transports, "");
      syncInstallationTransportState();
    }}

    const triggerRefresh = (() => {{
      let t = null;
      return () => {{
        if (t) clearTimeout(t);
        t = setTimeout(refreshJson, 120);
      }};
    }})();

    document.addEventListener("click", (ev) => {{
      if (ev.target?.matches?.('[data-search-input="1"]')) {{
        const input = ev.target;
        const wrap = input.closest('[data-search-select="1"]');
        const tr = input.closest("tr");
        const tb = input.closest("tbody");
        document.querySelectorAll('.search-select.open').forEach(el => {{ if (el !== wrap) el.classList.remove('open'); }});
        if (tr && tb && tb.id) setClassifSelectOptions(tr, tb.id);
        if (wrap) {{
          const ft = tr?.querySelector('[data-k="ft"]')?.value || "BONSAI process";
          setSearchMenuOptions(wrap, bonsaiProcessOptionsForType(ft, tb?.id || ""), input.value || "", "");
          wrap.classList.add("open");
        }}
        return;
      }}
      if (ev.target?.matches?.('[data-search-option="1"]')) {{
        const opt = ev.target;
        const wrap = opt.closest('[data-search-select="1"]');
        const input = wrap?.querySelector('[data-search-input="1"]');
        if (input) input.value = opt.getAttribute("data-value") || "";
        const tr = opt.closest("tr");
        const tb = opt.closest("tbody");
        if (tr && tb && tb.id) syncRowBonsaiState(tr, tb.id);
        if (wrap) wrap.classList.remove("open");
        triggerRefresh();
        return;
      }}
      const add = ev.target?.getAttribute?.("data-add");
      if (add) {{
        addRow(add);
        triggerRefresh();
        return;
      }}
      const del = ev.target?.getAttribute?.("data-del");
      if (del) {{
        const tr = ev.target.closest("tr");
        if (tr) tr.remove();
        triggerRefresh();
        return;
      }}
      document.querySelectorAll('.search-select.open').forEach(el => el.classList.remove('open'));
    }});
    document.addEventListener("input", (ev) => {{
      const el = ev.target;
      if (el && (el.tagName === "INPUT" || el.tagName === "SELECT" || el.tagName === "TEXTAREA")) {{
        if (el.matches('[data-search-input="1"]')) {{
          const wrap = el.closest('[data-search-select="1"]');
          const tr = el.closest("tr");
          const tb = el.closest("tbody");
          const ft = tr?.querySelector('[data-k="ft"]')?.value || "BONSAI process";
          setSearchMenuOptions(wrap, bonsaiProcessOptionsForType(ft, tb?.id || ""), el.value || "", el.value || "");
          if (wrap) wrap.classList.add("open");
          if (tr && tb && tb.id) syncRowBonsaiState(tr, tb.id);
        }}
        triggerRefresh();
      }}
    }});
    document.addEventListener("change", (ev) => {{
      const el = ev.target;
      if (el && (el.tagName === "INPUT" || el.tagName === "SELECT" || el.tagName === "TEXTAREA")) {{
        if (el.matches('select[data-k="ft"]')) {{
          const tr = el.closest("tr");
          const tb = el.closest("tbody");
          if (tr && tb && tb.id) {{
            setClassifSelectOptions(tr, tb.id);
            syncRowBonsaiState(tr, tb.id);
          }}
        }} else if (el.matches('select[data-k="srcLoc"], select[data-k="mode"]')) {{
          const tr = el.closest("tr");
          const tb = el.closest("tbody");
          if (tr && tb && tb.id) syncRowBonsaiState(tr, tb.id);
        }} else if (el.id === "iMode" || el.id === "iLoc") {{
          syncInstallationTransportState();
        }}
        triggerRefresh();
      }}
    }});
    document.getElementById("refreshBtn").addEventListener("click", refreshJson);
    document.getElementById("uploadBtn").addEventListener("click", () => {{
      document.getElementById("uploadInput").click();
    }});
    document.getElementById("uploadInput").addEventListener("change", async (ev) => {{
      const f = ev.target?.files?.[0];
      if (!f) return;
      try {{
        const txt = await f.text();
        const dpp = JSON.parse(txt);
        loadDppToForm(dpp);
        refreshJson();
        setStatus("ok", "DPP JSON uploaded and form populated.");
      }} catch (e) {{
        setStatus("err", "Upload failed: invalid JSON.");
        renderErrors([String(e?.message || e)]);
      }} finally {{
        ev.target.value = "";
      }}
    }});
    document.getElementById("copyBtn").addEventListener("click", async () => {{
      const txt = g("jsonOut").textContent || "";
      try {{
        await navigator.clipboard.writeText(txt);
        setStatus("ok", "JSON copied to clipboard.");
      }} catch (_) {{
        setStatus("warn", "Clipboard blocked by browser. Copy manually from JSON panel.");
      }}
    }});
    document.getElementById("downloadBtn").addEventListener("click", () => {{
      if (!window.__VALID_DPP) {{
        alert("JSON is currently blocked. Fix errors first.");
        return;
      }}
      const blob = new Blob([JSON.stringify(window.__VALID_DPP, null, 2)], {{ type: "application/json" }});
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "dpp_converted.json";
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    }});

    init();
    refreshJson();
  </script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    html = ""

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/" or self.path.startswith("/index"):
            body = self.html.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()

    def log_message(self, fmt: str, *args) -> None:
        _ = (fmt, args)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run minimal strict DPP local form generator")
    parser.add_argument("--dictionaries", type=Path, default=DEFAULT_DICTIONARIES)
    parser.add_argument("--bonsai-index", type=Path, default=DEFAULT_BONSAI_INDEX)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8780)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    dicts = _load_dictionaries(args.dictionaries)
    bonsai_index = _load_bonsai_index(args.bonsai_index)
    Handler.html = _html(dicts, bonsai_index)

    server = None
    chosen = None
    for p in [args.port] + list(range(args.port + 1, args.port + 12)):
        try:
            server = HTTPServer((args.host, p), Handler)
            chosen = p
            break
        except OSError:
            continue
    if server is None or chosen is None:
        raise RuntimeError(
            f"Could not bind server on {args.host}:{args.port}..{args.port+11}"
        )

    url = f"http://{args.host}:{chosen}"
    if chosen != args.port:
        print(f"Requested port {args.port} busy; using {chosen}")
    print(f"Minimal DPP generator running at: {url}")
    if not args.no_browser:
        try:
            webbrowser.open(url)
        except Exception:
            pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
