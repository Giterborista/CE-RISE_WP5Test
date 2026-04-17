#!/usr/bin/env python3
"""Online DPP impact calculator (strict) with BW2 + BONSAI and graph visualizers.

Design choices (from user decisions):
- DPP is a strict data carrier (no auto activity-chain links injected).
- Strict mapping, strict unresolved handling.
- Project can be selected or created from UI.
- BONSAI import can be requested from UI if missing.
- Method list is read live from current project (EF v3.1 subset).
- Stop point is selected from activities actually present in DPP.
- Scaling uses reference-flow target amount/unit only.
- Output includes table + two PNG visualizations:
  1) per-activity impact + total bar
  2) full DPP IO graph (activity-flow relations)
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import re
import threading
import time
import traceback
import uuid
import webbrowser
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def _runtime_default_bw_dir() -> str:
    explicit = os.environ.get("CERISE_BW_DIR") or os.environ.get("BW2_DIR") or os.environ.get("BRIGHTWAY2_DIR")
    if explicit:
        return explicit
    # Render persistent disk mount (if configured in Render dashboard).
    if Path("/var/data").exists():
        return "/var/data/brightway"
    return ""


_AUTO_BW_DIR = _runtime_default_bw_dir()
if _AUTO_BW_DIR:
    os.environ.setdefault("BRIGHTWAY2_DIR", _AUTO_BW_DIR)
    os.environ.setdefault("BW2_DIR", _AUTO_BW_DIR)


try:
    import bw2calc as bc
    import bw2data as bd
except Exception as exc:  # pragma: no cover
    raise RuntimeError("bw2data/bw2calc are required in this environment") from exc

from cerise_brightway import (
    DEFAULT_BIOSPHERE_DB,
    DEFAULT_BONSAI_DB,
    DEFAULT_BW_DIR,
    DEFAULT_METHOD,
    import_bonsai_if_missing,
    set_brightway_context,
)

ISO_RE = re.compile(r"^[A-Z]{2,3}$")
ALNUM_RE = re.compile(r"[^A-Za-z0-9]+")
BONSAI_CODE_RE = re.compile(r"^[A-Za-z0-9_]+\|[^|]+$")
PROJECT_SAFE_RE = re.compile(r"[^a-z0-9._-]+")
PROJECT_ALLOWED_RE = re.compile(r"^[A-Za-z0-9._-]+$")
REPO_ROOT = Path(__file__).resolve().parents[2]
DPPLCA_OUTPUT_NS = "http://example.org/dpplca_output#"
DPPLCA_LCSTAGES_NS = "http://example.org/dpplca_lcstages#"
EF31_LCIAMETHODS_NS = "http://example.org/ef31_lciamethods#"
EF31_IMPACTCATEGORIES_NS = "http://example.org/ef31_impactcategories#"
EF31_IMPACTINDICATORS_NS = "http://example.org/ef31_impactindicators#"
DPP_RUNTIME_NS = "http://example.org/dpp#"
ALLOWED_STAGES = {
    "Raw material acquisition",
    "Manufacturing",
    "Installation/distribution/retail",
    "Use",
    "Maintenance, repair, refurbishment",
    "End-of-life",
}
_METHOD_ONTOLOGY_CACHE: Optional[Dict[str, Dict[str, str]]] = None
_JOBS: Dict[str, Dict[str, Any]] = {}
_JOBS_LOCK = threading.Lock()
_BW_LOCK = threading.Lock()
_JOBS_DIR: Optional[Path] = None
CERISE_FIXED_PROJECT_RAW = os.environ.get("CERISE_FIXED_PROJECT", "").strip()
CERISE_DISABLE_BOOTSTRAP = os.environ.get("CERISE_DISABLE_BOOTSTRAP", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (s or "").strip().lower()).strip("-")


def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").strip().lower())


def _normalize_location_code(code: str) -> str:
    raw = str(code or "").strip().upper()
    if not raw:
        return ""
    if raw == "GLOBAL":
        return "GLO"
    return raw


def _safe_project_name(name: str) -> str:
    base = str(name or "").strip().lower()
    if not base:
        return ""
    base = PROJECT_SAFE_RE.sub("-", base)
    base = re.sub(r"-{2,}", "-", base).strip("-.")
    return base or "project"


def _coerce_project_name(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    if s.startswith("Project: "):
        s = s.replace("Project: ", "", 1).strip()
    m = re.match(r"^<Project:\s*(.+?)>$", s)
    if m:
        s = m.group(1).strip()
    m = re.match(r"^Project\(name=['\"](.+?)['\"].*\)$", s)
    if m:
        s = m.group(1).strip()
    return s


def _fixed_project_name() -> str:
    return _coerce_project_name(CERISE_FIXED_PROJECT_RAW)


def _effective_project_name(requested: Any) -> str:
    fixed = _fixed_project_name()
    if fixed:
        return fixed
    return _coerce_project_name(requested)


def _round_sig(value: float, sig: int = 6) -> float:
    if value == 0:
        return 0.0
    from math import floor, log10

    return round(value, sig - int(floor(log10(abs(value)))) - 1)


def _fmt_sig(value: float, sig: int = 6) -> str:
    return f"{_round_sig(float(value), sig):.{sig}g}"


def _json_response(handler: BaseHTTPRequestHandler, payload: Dict[str, Any], status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _text_response(handler: BaseHTTPRequestHandler, text: str, status: int = 200) -> None:
    body = text.encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/plain; charset=utf-8")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _job_path(job_id: str) -> Optional[Path]:
    if _JOBS_DIR is None:
        return None
    return _JOBS_DIR / f"{job_id}.json"


def _job_persist(job: Dict[str, Any]) -> None:
    p = _job_path(str(job.get("id", "")))
    if p is None:
        return
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(job, ensure_ascii=False), encoding="utf-8")
        tmp.replace(p)
    except Exception:
        # Persistence is best-effort; in-memory flow still works.
        pass


def _job_load(job_id: str) -> Optional[Dict[str, Any]]:
    p = _job_path(job_id)
    if p is None or not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("id") == job_id:
            return data
    except Exception:
        return None
    return None


def _job_create(kind: str, payload: Dict[str, Any]) -> str:
    job_id = uuid.uuid4().hex
    now = time.time()
    job = {
        "id": job_id,
        "kind": kind,
        "status": "queued",
        "payload": payload,
        "createdAt": now,
        "updatedAt": now,
    }
    with _JOBS_LOCK:
        _JOBS[job_id] = job
    _job_persist(job)
    return job_id


def _job_update(job_id: str, **fields: Any) -> None:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            job = _job_load(job_id)
            if job is None:
                return
            _JOBS[job_id] = job
        job.update(fields)
        job["updatedAt"] = time.time()
    _job_persist(job)


def _job_snapshot(job_id: str) -> Optional[Dict[str, Any]]:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
    if job is None:
        job = _job_load(job_id)
        if job is None:
            return None
        with _JOBS_LOCK:
            _JOBS[job_id] = job
        # Do not echo full payloads back to UI.
    return {
        "id": job["id"],
        "kind": job["kind"],
        "status": job["status"],
        "createdAt": job["createdAt"],
        "updatedAt": job["updatedAt"],
        "result": job.get("result"),
        "error": job.get("error"),
        "trace": job.get("trace"),
    }


def _run_setup_job(job_id: str, payload: Dict[str, Any], bw_dir: Path) -> None:
    _job_update(job_id, status="running")
    try:
        with _BW_LOCK:
            project = _effective_project_name(payload.get("project", ""))
            setup = _ensure_project_and_bonsai(
                project=project,
                create_project=bool(payload.get("createProject", False)),
                import_bonsai=bool(payload.get("importBonsaiIfMissing", True)),
                bw_dir=bw_dir,
            )
            try:
                projects = _project_names()
            except Exception:
                projects = [setup.get("project", "")]
            result = {"projectInfo": setup, "projects": projects}
        _job_update(job_id, status="done", result=result)
    except Exception as exc:
        _job_update(job_id, status="error", error=str(exc), trace=traceback.format_exc(limit=8))


def _run_calc_job(job_id: str, payload: Dict[str, Any], bw_dir: Path) -> None:
    _job_update(job_id, status="running")
    try:
        with _BW_LOCK:
            result = _handle_calculation(payload=payload, bw_dir=bw_dir)
        _job_update(job_id, status="done", result=result)
    except Exception as exc:
        _job_update(job_id, status="error", error=str(exc), trace=traceback.format_exc(limit=8))


def _project_names() -> List[str]:
    names: List[str] = []
    try:
        projects_iter = list(bd.projects)
    except Exception:
        projects_iter = []
    for p in projects_iter:
        try:
            name = getattr(p, "name", None)
            if not name:
                name = _coerce_project_name(p)
            else:
                name = _coerce_project_name(name)
            if not name:
                continue
            if not PROJECT_ALLOWED_RE.match(name):
                safe = _safe_project_name(name)
                if not safe:
                    continue
                name = safe
            names.append(name)
        except Exception:
            # Skip malformed project entries instead of failing setup/init.
            continue
    if not names:
        try:
            cur = getattr(bd.projects.current, "name", "") if bd.projects.current else ""
            cur = _coerce_project_name(cur)
            if cur:
                names.append(cur)
        except Exception:
            pass
    return sorted(set(names), key=str.lower)


def _method_label(m: Tuple[str, ...]) -> str:
    return " | ".join(m)


def _ef_methods() -> List[Tuple[str, ...]]:
    methods = [m for m in bd.methods if isinstance(m, tuple) and m and m[0] == "EF v3.1"]
    return sorted(methods)


def _default_method_label(labels: List[str]) -> str:
    target = _method_label(DEFAULT_METHOD)
    for l in labels:
        if l == target:
            return l
    for l in labels:
        if "climate change" in l.lower() and "gwp100" in l.lower():
            return l
    return labels[0] if labels else ""


def _ensure_project_and_bonsai(
    project: str,
    create_project: bool,
    import_bonsai: bool,
    bw_dir: Path,
) -> Dict[str, Any]:
    requested_project = _coerce_project_name(project)
    fixed_project = _fixed_project_name()
    project = fixed_project or requested_project

    if fixed_project:
        create_project = False
        import_bonsai = False

    if not project:
        raise ValueError("Project name is required")
    if not PROJECT_ALLOWED_RE.match(project):
        project = _safe_project_name(project)
        if not project:
            raise ValueError("Project name is invalid")

    os.environ["BRIGHTWAY2_DIR"] = str(bw_dir)
    os.environ["BW2_DIR"] = str(bw_dir)

    existing = set(_project_names())
    if project not in existing:
        if not create_project:
            raise ValueError(f"Project '{project}' not found. Enable 'Create if missing'.")
        try:
            bd.projects.set_current(project)
        except Exception as exc:
            # Some BW2 environments enforce strict name patterns.
            # If the requested name fails, retry once with a safe slug.
            msg = str(exc) or exc.__class__.__name__
            if "expected pattern" in msg.lower():
                safe_name = _safe_project_name(project)
                if safe_name != project:
                    try:
                        bd.projects.set_current(safe_name)
                        project = safe_name
                    except Exception as exc2:
                        raise ValueError(
                            f"Could not create project '{project}' (fallback '{safe_name}' also failed): {exc2}"
                        ) from exc2
                else:
                    raise ValueError(f"Could not create project '{project}': {msg}") from exc
            else:
                raise ValueError(f"Could not create project '{project}': {msg}") from exc
    else:
        try:
            set_brightway_context(project=project, bw_dir=bw_dir)
        except Exception as exc:
            msg = str(exc) or exc.__class__.__name__
            if "expected pattern" in msg.lower():
                safe_name = _safe_project_name(project)
                if safe_name and safe_name != project:
                    set_brightway_context(project=safe_name, bw_dir=bw_dir)
                    project = safe_name
                else:
                    raise ValueError(f"Could not switch to project '{project}': {msg}") from exc
            else:
                raise ValueError(f"Could not switch to project '{project}': {msg}") from exc

    try:
        set_brightway_context(project=project, bw_dir=bw_dir)
    except Exception as exc:
        raise ValueError(f"Could not activate project '{project}': {exc}") from exc

    imported = False
    if DEFAULT_BONSAI_DB not in bd.databases:
        if import_bonsai:
            try:
                imported = import_bonsai_if_missing(project=project, bw_dir=bw_dir)
            except Exception as exc:
                raise RuntimeError(
                    f"BONSAI import failed for project '{project}': {exc}"
                ) from exc
        else:
            raise ValueError(
                f"BONSAI database '{DEFAULT_BONSAI_DB}' not found in project '{project}'. "
                "Enable 'Import BONSAI if missing'."
            )

    if DEFAULT_BIOSPHERE_DB not in bd.databases:
        raise ValueError(
            f"Biosphere database '{DEFAULT_BIOSPHERE_DB}' not found in project '{project}'."
        )

    methods = _ef_methods()
    method_labels = [_method_label(m) for m in methods]

    return {
        "project": project,
        "requestedProject": requested_project,
        "fixedProject": fixed_project,
        "bonsaiImported": imported,
        "methodLabels": method_labels,
        "defaultMethod": _default_method_label(method_labels),
    }


def _validate_dpp_strict(dpp: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    if not isinstance(dpp, dict):
        return ["DPP must be a JSON object."]

    if not dpp.get("dppId"):
        errors.append("dppId is mandatory.")

    if not isinstance(dpp.get("flowObjects"), list):
        errors.append("flowObjects must be a list.")
    if not isinstance(dpp.get("activities"), list):
        errors.append("activities must be a list.")

    flow_objects = {fo.get("flowObjectId"): fo for fo in dpp.get("flowObjects", []) if isinstance(fo, dict)}

    for fo in dpp.get("flowObjects", []):
        if not isinstance(fo, dict):
            errors.append("flowObjects entries must be objects.")
            continue
        if not fo.get("flowObjectId"):
            errors.append("flowObjectId is required for each flow object.")
        if fo.get("objectClass") not in {"primary", "secondary", "elementary"}:
            errors.append(f"FlowObject '{fo.get('flowObjectId')}' has invalid objectClass.")

    acts = dpp.get("activities", [])
    if not acts:
        errors.append("At least one activity is required.")

    seen_act: set[str] = set()
    for i, a in enumerate(acts):
        if not isinstance(a, dict):
            errors.append(f"activities[{i}] must be an object.")
            continue

        aid = a.get("activityId")
        if not aid:
            errors.append(f"activities[{i}].activityId is required.")
        elif aid in seen_act:
            errors.append(f"Duplicate activityId: {aid}")
        else:
            seen_act.add(aid)

        if not a.get("Activity"):
            errors.append(f"Activity '{aid or i}': Activity is required.")
        if not a.get("ActivityType"):
            errors.append(f"Activity '{aid or i}': ActivityType is required.")

        stage = a.get("LCStage")
        if stage not in ALLOWED_STAGES:
            errors.append(f"Activity '{aid or i}': invalid LCStage '{stage}'.")

        if stage == "Raw material acquisition":
            if "Place" in a and a.get("Place") not in {None, ""}:
                errors.append(f"Activity '{aid or i}': BoM must not include Place.")
            if "ReferenceYear" in a and a.get("ReferenceYear") not in {None, ""}:
                errors.append(f"Activity '{aid or i}': BoM must not include ReferenceYear.")
        else:
            place = a.get("Place")
            year = a.get("ReferenceYear")
            if not isinstance(place, str) or not ISO_RE.match(place.strip().upper()):
                errors.append(f"Activity '{aid or i}': Place must be ISO code (2-3 letters).")
            if not isinstance(year, int) or year <= 0:
                errors.append(f"Activity '{aid or i}': ReferenceYear is required and must be positive integer.")

        flows = a.get("flows")
        if not isinstance(flows, list) or not flows:
            errors.append(f"Activity '{aid or i}': flows must be a non-empty list.")
            continue

        det_id = a.get("determiningFlowId")
        if not det_id:
            errors.append(f"Activity '{aid or i}': determiningFlowId is required.")
            continue

        by_flow_id = {}
        for f in flows:
            if isinstance(f, dict) and f.get("flowId"):
                by_flow_id[f["flowId"]] = f

        det_flow = by_flow_id.get(det_id)
        if not det_flow:
            errors.append(f"Activity '{aid or i}': determiningFlowId not found in flows.")
            continue

        if det_flow.get("direction") != "output":
            errors.append(f"Activity '{aid or i}': determining flow must be output.")

        det_obj = flow_objects.get(det_flow.get("flowObjectId"), {})
        if det_obj.get("objectClass") != "primary":
            errors.append(f"Activity '{aid or i}': determining flow object must be primary.")

        for f in flows:
            if not isinstance(f, dict):
                errors.append(f"Activity '{aid or i}': each flow must be an object.")
                continue
            fid = f.get("flowId")
            if not fid:
                errors.append(f"Activity '{aid or i}': each flow needs flowId.")
            if f.get("flowObjectId") not in flow_objects:
                errors.append(f"Activity '{aid or i}' flow '{fid}': flowObjectId not found in flowObjects.")
                continue
            if f.get("direction") not in {"input", "output"}:
                errors.append(f"Activity '{aid or i}' flow '{fid}': direction must be input/output.")
            if not isinstance(f.get("amount"), (int, float)):
                errors.append(f"Activity '{aid or i}' flow '{fid}': amount must be numeric.")
            if not isinstance(f.get("unit"), str) or not f.get("unit").strip():
                errors.append(f"Activity '{aid or i}' flow '{fid}': unit is required.")

            is_det = fid == det_id
            if not is_det:
                if not isinstance(f.get("evidenceMethod"), str) or not f.get("evidenceMethod").strip():
                    errors.append(f"Activity '{aid or i}' flow '{fid}': evidenceMethod is mandatory.")

                fobj = flow_objects[f.get("flowObjectId")]
                cls = fobj.get("objectClass")
                if cls == "secondary":
                    if not isinstance(f.get("bonsaiProcess"), str) or not f.get("bonsaiProcess").strip():
                        errors.append(
                            f"Activity '{aid or i}' flow '{fid}': secondary flows require bonsaiProcess."
                        )
                    src = _normalize_location_code(f.get("sourceLocation", ""))
                    if not ISO_RE.match(src):
                        errors.append(
                            f"Activity '{aid or i}' flow '{fid}': secondary flows require sourceLocation ISO code."
                        )
                    code = str(f.get("bonsaiCode", "")).strip()
                    if not BONSAI_CODE_RE.match(code):
                        errors.append(
                            f"Activity '{aid or i}' flow '{fid}': secondary flows require bonsaiCode '<TOKEN>|<LOCATION>'."
                        )
                    elif _normalize_location_code(code.split("|", 1)[1]) != src:
                        errors.append(
                            f"Activity '{aid or i}' flow '{fid}': bonsaiCode location must match sourceLocation."
                        )

    return errors


def _db_name_from_dpp_id(dpp_id: str) -> str:
    raw = ALNUM_RE.sub("", dpp_id or "")
    if not raw:
        raw = "DPP"
    raw = raw[:40]
    return f"DPP{raw}_database"


@dataclass
class ResolutionError:
    activity_id: str
    flow_id: str
    reason: str


def _resolve_secondary_key(
    flow_bonsai_code: str,
    flow_bonsai_process: str,
    source_location: str,
    bonsai_meta_by_code: Dict[str, Dict[str, str]],
) -> str:
    code = str(flow_bonsai_code or "").strip()
    if not BONSAI_CODE_RE.match(code):
        raise ValueError("secondary flows require bonsaiCode in '<TOKEN>|<LOCATION>' form")

    loc = _normalize_location_code(source_location)
    if not ISO_RE.match(loc):
        raise ValueError("invalid sourceLocation ISO code")

    meta = bonsai_meta_by_code.get(code)
    if not meta:
        raise ValueError(f"BONSAI code '{code}' not found")

    code_loc = meta.get("location", "")
    if code_loc != loc:
        raise ValueError(
            f"bonsaiCode location '{code_loc}' mismatches sourceLocation '{loc}'"
        )

    bonsai_process = str(flow_bonsai_process or "").strip()
    if not bonsai_process:
        raise ValueError("secondary flows require bonsaiProcess")

    proc_norm = _norm_text(bonsai_process)
    name_norm = _norm_text(meta.get("name", ""))
    ref_norm = _norm_text(meta.get("reference_product", ""))
    if proc_norm not in {name_norm, ref_norm}:
        raise ValueError(
            f"bonsaiCode '{code}' does not match bonsaiProcess '{bonsai_process}'"
        )

    return code


def _resolve_elementary_key(
    flow_obj_name: str,
    biosphere_db: str,
    by_name_index: Dict[str, List[Tuple[str, str]]],
) -> Tuple[str, str]:
    name = str(flow_obj_name or "").strip()
    if not name:
        raise ValueError("empty elementary flow object name")

    candidates = by_name_index.get(name, [])
    if not candidates:
        raise ValueError(f"elementary flow '{name}' not found in '{biosphere_db}'")
    if len(candidates) > 1:
        raise ValueError(f"elementary flow '{name}' is ambiguous ({len(candidates)} matches)")
    return candidates[0]


def _norm_unit(u: str) -> str:
    return (u or "").strip().lower()


def _convert_dpp_to_supplier_unit(
    amount: float,
    dpp_unit: str,
    supplier_unit: str,
) -> Tuple[float, str, str]:
    """Strict unit conversion for secondary BONSAI exchanges.

    Implemented only for requested pairs:
    - kg/KGM -> tonne
    - kWh/KWH -> TJ
    """
    du = _norm_unit(dpp_unit)
    su = _norm_unit(supplier_unit)

    # Same unit (case-insensitive)
    if du == su:
        return amount, supplier_unit, "no_conversion"

    # kg -> tonne
    if du in {"kg", "kgm"} and su == "tonne":
        return amount / 1000.0, supplier_unit, "kg_to_tonne"

    # kWh -> TJ
    if du in {"kwh"} and su == "tj":
        return amount * 3.6e-6, supplier_unit, "kwh_to_tj"

    raise ValueError(
        f"unit mismatch: DPP unit '{dpp_unit}' not compatible with supplier unit '{supplier_unit}'. "
        "Supported conversions in strict mode: kg->tonne, kWh->TJ."
    )


def _build_foreground_payload_strict(
    dpp: Dict[str, Any],
    activities_subset: List[Dict[str, Any]],
    db_name: str,
    bonsai_db: str,
    biosphere_db: str,
    cumulative_mode: bool = False,
) -> Tuple[Dict[Tuple[str, str], Dict[str, Any]], List[ResolutionError]]:
    flow_map = {fo["flowObjectId"]: fo for fo in dpp.get("flowObjects", [])}
    payload: Dict[Tuple[str, str], Dict[str, Any]] = {}
    errors: List[ResolutionError] = []

    bonsai_meta_by_code: Dict[str, Dict[str, str]] = {}
    for ds in bd.Database(bonsai_db):
        code = ds["code"]
        raw_loc = code.split("|", 1)[1] if "|" in code else ""
        bonsai_meta_by_code[code] = {
            "location": _normalize_location_code(raw_loc),
            "name": str(ds.get("name") or "").strip(),
            "reference_product": str(ds.get("reference product") or "").strip(),
            "unit": str(ds.get("unit") or "").strip(),
        }

    by_name_index: Dict[str, List[Tuple[str, str]]] = {}
    for ds in bd.Database(biosphere_db):
        nm = ds.get("name")
        if not nm:
            continue
        by_name_index.setdefault(nm, []).append((biosphere_db, ds["code"]))

    for act in activities_subset:
        aid = act["activityId"]
        flows = act.get("flows", [])
        det_id = act.get("determiningFlowId")
        det = next((f for f in flows if f.get("flowId") == det_id), None)
        if det is None:
            errors.append(ResolutionError(aid, "<det>", "determiningFlowId not found"))
            continue

        ds = {
            "name": act.get("Activity", aid),
            "reference product": act.get("ActivityType", act.get("Activity", aid)),
            "unit": det.get("unit", "unit"),
            "location": act.get("Place") or "GLO",
            "exchanges": [
                {
                    "input": (db_name, aid),
                    "amount": float(det.get("amount", 1.0)),
                    "type": "production",
                    "unit": det.get("unit", "unit"),
                }
            ],
        }

        for fl in flows:
            fid = fl.get("flowId", "")
            if fid == det_id:
                continue
            fo = flow_map.get(fl.get("flowObjectId"))
            if fo is None:
                errors.append(ResolutionError(aid, fid, "flowObjectId not found"))
                continue

            cls = fo.get("objectClass")
            direction = fl.get("direction")
            amount = float(fl.get("amount", 0.0))
            unit = fl.get("unit", "")

            try:
                if cls == "secondary":
                    if direction not in {"input", "output"}:
                        raise ValueError("secondary flows must be input or output")
                    code = _resolve_secondary_key(
                        flow_bonsai_code=fl.get("bonsaiCode", ""),
                        flow_bonsai_process=fl.get("bonsaiProcess", ""),
                        source_location=fl.get("sourceLocation", ""),
                        bonsai_meta_by_code=bonsai_meta_by_code,
                    )
                    supplier_unit = bonsai_meta_by_code.get(code, {}).get("unit", "")
                    conv_amount, conv_unit, conv_note = _convert_dpp_to_supplier_unit(
                        amount=amount,
                        dpp_unit=unit,
                        supplier_unit=supplier_unit,
                    )
                    model_note = (
                        "secondary_output_modeled_as_treatment_exchange"
                        if direction == "output"
                        else "secondary_input_exchange"
                    )
                    ds["exchanges"].append(
                        {
                            "input": (bonsai_db, code),
                            "amount": conv_amount,
                            "type": "technosphere",
                            "unit": conv_unit,
                            "comment": (
                                f"evidenceMethod={fl.get('evidenceMethod','')}; "
                                f"dppDirection={direction}; modeling={model_note}; "
                                f"unitConversion={conv_note}; dppUnit={unit}; supplierUnit={supplier_unit}"
                            ),
                        }
                    )
                elif cls == "elementary":
                    if direction != "output":
                        raise ValueError("elementary flows are supported only as output in strict mode")
                    bio_key = _resolve_elementary_key(
                        flow_obj_name=fo.get("name", ""),
                        biosphere_db=biosphere_db,
                        by_name_index=by_name_index,
                    )
                    ds["exchanges"].append(
                        {
                            "input": bio_key,
                            "amount": amount,
                            "type": "biosphere",
                            "unit": unit,
                            "comment": f"evidenceMethod={fl.get('evidenceMethod','')}",
                        }
                    )
                elif cls == "primary":
                    raise ValueError("primary flows other than determining flow are not supported in strict mode")
                else:
                    raise ValueError(f"unsupported objectClass '{cls}'")
            except Exception as exc:
                errors.append(ResolutionError(aid, fid, str(exc)))

        payload[(db_name, aid)] = ds

    # Optional cumulative chaining: each activity consumes the determining output
    # of the previous activity (1 unit), creating a linked foreground chain.
    if cumulative_mode and not errors and len(activities_subset) > 1:
        for idx in range(1, len(activities_subset)):
            prev = activities_subset[idx - 1]
            curr = activities_subset[idx]
            prev_aid = prev.get("activityId")
            curr_aid = curr.get("activityId")
            if not prev_aid or not curr_aid:
                continue
            prev_det_id = prev.get("determiningFlowId")
            prev_det = next((f for f in prev.get("flows", []) if f.get("flowId") == prev_det_id), None)
            prev_unit = str(prev_det.get("unit", "unit")) if isinstance(prev_det, dict) else "unit"
            curr_ds = payload.get((db_name, curr_aid))
            if not curr_ds:
                continue
            curr_ds.setdefault("exchanges", []).append(
                {
                    "input": (db_name, prev_aid),
                    "amount": 1.0,
                    "type": "technosphere",
                    "unit": prev_unit,
                    "comment": "cumulative_chain_link=true",
                }
            )

    return payload, errors


def _write_foreground(payload: Dict[Tuple[str, str], Dict[str, Any]], db_name: str) -> None:
    if db_name in bd.databases:
        bd.Database(db_name).delete(warn=False)
    bd.Database(db_name).write(payload)


def _method_from_label(label: str) -> Tuple[str, ...]:
    parts = [p.strip() for p in (label or "").split("|")]
    return tuple(parts)


def _load_method_ontology_cache() -> Dict[str, Dict[str, str]]:
    global _METHOD_ONTOLOGY_CACHE
    if _METHOD_ONTOLOGY_CACHE is not None:
        return _METHOD_ONTOLOGY_CACHE

    ttl_path = REPO_ROOT / "ontology" / "elementary_flows" / "ef31_lciamethods.ttl"
    cache: Dict[str, Dict[str, str]] = {}
    if not ttl_path.exists():
        _METHOD_ONTOLOGY_CACHE = cache
        return cache

    text = ttl_path.read_text(encoding="utf-8")
    block_re = re.compile(
        r"^:(?P<local>[A-Za-z0-9\-]+)\s+a\s+:LCIAMethod\s*;\s*(?P<body>.*?)(?=^:[A-Za-z0-9\-]+\s+a\s+:LCIAMethod\s*;|\Z)",
        re.M | re.S,
    )
    label_re = re.compile(r'rdfs:label\s+"(?P<label>[^"]+)"')
    category_re = re.compile(r":forImpactCategory\s+efic:(?P<local>[A-Za-z0-9_]+)")
    indicator_re = re.compile(r":usesImpactIndicator\s+efii:(?P<local>[A-Za-z0-9_]+)")

    for match in block_re.finditer(text):
        local = match.group("local")
        body = match.group("body")
        label_match = label_re.search(body)
        category_match = category_re.search(body)
        indicator_match = indicator_re.search(body)
        label = label_match.group("label") if label_match else local
        category_local = category_match.group("local") if category_match else ""
        indicator_local = indicator_match.group("local") if indicator_match else ""
        info = {
            "methodUri": f"{EF31_LCIAMETHODS_NS}{local}",
            "methodLabel": label,
            "impactCategoryUri": f"{EF31_IMPACTCATEGORIES_NS}{category_local}" if category_local else "",
            "impactCategoryLocal": category_local,
            "impactIndicatorUri": f"{EF31_IMPACTINDICATORS_NS}{indicator_local}" if indicator_local else "",
            "impactIndicatorLocal": indicator_local,
        }
        for key in {
            _norm_key(label),
            _norm_key(category_local),
        }:
            if key:
                cache[key] = info

    _METHOD_ONTOLOGY_CACHE = cache
    return cache


def _method_ontology_info(method: Tuple[str, ...]) -> Dict[str, str]:
    cache = _load_method_ontology_cache()
    keys = []
    if len(method) > 1:
        keys.append(_norm_key(method[1]))
    if len(method) > 2:
        keys.append(_norm_key(method[2]))
    for key in keys:
        if key and key in cache:
            return cache[key]
    return {
        "methodUri": "",
        "methodLabel": "",
        "impactCategoryUri": "",
        "impactCategoryLocal": "",
        "impactIndicatorUri": "",
        "impactIndicatorLocal": "",
    }


def _impact_category_from_method(method: Tuple[str, ...]) -> str:
    return str(method[1]).strip() if len(method) > 1 else ""


def _impact_indicator_from_method(method: Tuple[str, ...]) -> str:
    return str(method[2]).strip() if len(method) > 2 else ""


def _derive_system_boundary(activities: List[Dict[str, Any]]) -> str:
    stages = {str(a.get("LCStage", "")).strip() for a in activities if str(a.get("LCStage", "")).strip()}
    grave_stages = {
        "Use",
        "Maintenance, repair, refurbishment",
        "End-of-life",
    }
    if stages & grave_stages:
        return "cradle-to-grave"
    return "cradle-to-gate"


def _system_boundary_uri(system_boundary: str) -> Optional[str]:
    mapping = {
        "cradle-to-grave": f"{DPPLCA_OUTPUT_NS}CradleToGrave",
        "cradle-to-gate": f"{DPPLCA_OUTPUT_NS}CradleToGate",
    }
    return mapping.get(str(system_boundary or "").strip().lower())


def _stage_reference(stage_label: str) -> Dict[str, Any]:
    source_label = str(stage_label or "").strip()
    mapping = {
        "Raw material acquisition": (
            f"{DPPLCA_LCSTAGES_NS}RawMaterialAcquisition",
            "Raw material acquisition and pre-processing (including production of parts and components)",
        ),
        "Manufacturing": (
            f"{DPPLCA_LCSTAGES_NS}Manufacturing",
            "Manufacturing (production of the main product)",
        ),
        "Installation/distribution/retail": (
            f"{DPPLCA_LCSTAGES_NS}Distribution",
            "Distribution",
        ),
        "Use": (
            f"{DPPLCA_LCSTAGES_NS}Use",
            "Use",
        ),
        # The DPPLCA stage ontology folds maintenance under the use stage.
        "Maintenance, repair, refurbishment": (
            f"{DPPLCA_LCSTAGES_NS}Use",
            "Use",
        ),
        "End-of-life": (
            f"{DPPLCA_LCSTAGES_NS}EndOfLife",
            "End of life (including product recovery and recycling)",
        ),
    }
    uri, label = mapping.get(source_label, (None, source_label))
    return {
        "uri": uri,
        "label": label,
        "sourceLabel": source_label,
    }


def _safe_fragment(text: str) -> str:
    frag = re.sub(r"[^A-Za-z0-9._-]+", "-", str(text or "").strip()).strip("-")
    return frag or "unknown"


def _result_uri(dpp_id: str, stage_ref: Dict[str, Any]) -> str:
    stage_token = stage_ref.get("uri") or stage_ref.get("sourceLabel") or stage_ref.get("label") or "unknown"
    return f"{DPP_RUNTIME_NS}{_safe_fragment(dpp_id)}#lcaresult-{_safe_fragment(str(stage_token))}"


def _flow_uri(dpp_id: str, flow_id: str) -> str:
    return f"{DPP_RUNTIME_NS}{_safe_fragment(dpp_id)}#flow-{_safe_fragment(flow_id)}"


def _ontology_reference_flow(
    dpp_id: str,
    calculation_mode: str,
    target_amount: float,
    target_unit: str,
    stop_activity: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if calculation_mode != "single" or not stop_activity:
        return None
    det = _det_flow(stop_activity)
    return {
        "uri": _flow_uri(dpp_id, str(det.get("flowId", ""))),
        "activityId": str(stop_activity.get("activityId", "")),
        "flowId": str(det.get("flowId", "")),
        "amount": _round_sig(float(target_amount), 8),
        "unit": str(target_unit or "").strip(),
    }


def _aggregate_stage_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        stage_ref = _stage_reference(str(row.get("LCStage", "")).strip())
        stage_key = str(stage_ref.get("uri") or stage_ref.get("label") or "unclassified")
        bucket = grouped.setdefault(
            stage_key,
            {
                "stageRef": stage_ref,
                "value": 0.0,
                "unit": str(row.get("unit", "")).strip(),
                "activityCount": 0,
                "sourceStages": [],
            },
        )
        bucket["value"] += float(row.get("incrementalScore", row.get("score", 0.0)) or 0.0)
        bucket["activityCount"] += 1
        source_stage = stage_ref.get("sourceLabel")
        if source_stage and source_stage not in bucket["sourceStages"]:
            bucket["sourceStages"].append(source_stage)
        if not bucket["unit"]:
            bucket["unit"] = str(row.get("unit", "")).strip()
    return list(grouped.values())


def _ontology_result_rows(
    dpp_id: str,
    rows: List[Dict[str, Any]],
    method: Tuple[str, ...],
    system_boundary: str,
    secondary_data_source: str,
) -> List[Dict[str, Any]]:
    impact_category = _impact_category_from_method(method)
    impact_indicator = _impact_indicator_from_method(method)
    method_info = _method_ontology_info(method)
    boundary_uri = _system_boundary_uri(system_boundary)
    out: List[Dict[str, Any]] = []
    for idx, stage_row in enumerate(_aggregate_stage_rows(rows), 1):
        stage_ref = stage_row["stageRef"]
        out.append(
            {
                "resultId": f"lcaresult_stage_{idx}",
                "uri": _result_uri(dpp_id, stage_ref),
                "forLCStage": stage_ref,
                "forImpactCategory": {
                    "uri": method_info.get("impactCategoryUri") or None,
                    "label": impact_category,
                },
                "calculatedWithMethod": {
                    "uri": method_info.get("methodUri") or None,
                    "label": _method_label(method),
                },
                "hasSystemBoundary": {
                    "uri": boundary_uri,
                    "label": system_boundary,
                },
                "usesSecondaryDataFrom": secondary_data_source,
                "hasImpactValue": {
                    "uri": method_info.get("impactIndicatorUri") or None,
                    "label": impact_indicator,
                    "value": _round_sig(float(stage_row["value"]), 6),
                    "unit": stage_row["unit"],
                },
                "supplementary": {
                    "activityCount": stage_row["activityCount"],
                    "sourceStages": stage_row["sourceStages"],
                    "aggregationBasis": "sum of activity-level incremental scores",
                },
            }
        )
    return out


def _functional_unit_amount_literal(fu: Dict[str, Any]) -> str:
    quantity = str(fu.get("HowMuchQuantity", "")).strip()
    unit = str(fu.get("HowMuchUnit", "")).strip()
    return " ".join(part for part in (quantity, unit) if part).strip()


def _compute_scaling(
    activity: Dict[str, Any],
    target_amount: float,
    target_unit: str,
) -> float:
    det_id = activity.get("determiningFlowId")
    det = next((f for f in activity.get("flows", []) if f.get("flowId") == det_id), None)
    if det is None:
        raise ValueError("Selected stop activity has no determining flow")
    det_amount = float(det.get("amount", 0.0))
    det_unit = str(det.get("unit", "")).strip().upper()
    target_unit_u = str(target_unit or "").strip().upper()

    if det_amount <= 0:
        raise ValueError("Determining flow amount must be > 0")
    if det_unit != target_unit_u:
        raise ValueError(
            f"Reference flow unit mismatch: stop activity determining unit is '{det_unit}' but target is '{target_unit_u}'"
        )
    if target_amount <= 0:
        raise ValueError("Reference flow amount must be > 0")

    return target_amount / det_amount


def _det_flow(activity: Dict[str, Any]) -> Dict[str, Any]:
    det_id = activity.get("determiningFlowId")
    det = next((f for f in activity.get("flows", []) if f.get("flowId") == det_id), None)
    if not isinstance(det, dict):
        raise ValueError(f"Activity '{activity.get('activityId','')}' has no valid determining flow")
    return det


def _score_demand(demand: Dict[Tuple[str, str], float], method: Tuple[str, ...]) -> Tuple[float, str]:
    if method not in bd.methods:
        raise ValueError(f"Method not found in project: {method}")
    cleaned = {k: float(v) for k, v in demand.items() if float(v) != 0.0}
    if not cleaned:
        return 0.0, bd.methods[method].get("unit", "")
    lca = bc.LCA(cleaned, method)
    lca.lci()
    lca.lcia()
    return _round_sig(float(lca.score), 6), bd.methods[method].get("unit", "")


def _run_activity_lca_scores(
    db_name: str,
    activities_subset: List[Dict[str, Any]],
    scaling: float,
    method: Tuple[str, ...],
) -> List[Dict[str, Any]]:
    if method not in bd.methods:
        raise ValueError(f"Method not found in project: {method}")

    rows: List[Dict[str, Any]] = []
    for act in activities_subset:
        aid = act["activityId"]
        demand = {(db_name, aid): scaling}
        lca = bc.LCA(demand, method)
        lca.lci()
        lca.lcia()
        det = _det_flow(act)
        score = _round_sig(float(lca.score), 6)
        rows.append(
            {
                "activityId": aid,
                "Activity": act.get("Activity", aid),
                "LCStage": act.get("LCStage", ""),
                "requestedAmount": _round_sig(float(scaling), 8),
                "requestedUnit": str(det.get("unit", "")),
                "activityUnitsDemand": _round_sig(float(scaling), 8),
                "incrementalScore": score,
                "cumulativeScore": score,
                "score": score,
                "unit": bd.methods[method].get("unit", ""),
            }
        )
    return rows


def _build_per_activity_demand_vector(
    activities: List[Dict[str, Any]],
    raw_demands: Any,
    db_name: str,
) -> Tuple[List[Dict[str, Any]], Dict[Tuple[str, str], float], List[str]]:
    """Build strict per-activity demand vector from UI payload.

    Returns:
      rows_meta (DPP-order rows with requested amounts),
      demand_map (BW demand),
      errors (validation errors)
    """
    errors: List[str] = []
    if raw_demands is None:
        raw_demands = []
    if not isinstance(raw_demands, list):
        return [], {}, ["perActivityDemands must be a list."]

    by_input: Dict[str, Dict[str, Any]] = {}
    for i, item in enumerate(raw_demands):
        if not isinstance(item, dict):
            errors.append(f"perActivityDemands[{i}] must be an object.")
            continue
        aid = str(item.get("activityId", "")).strip()
        if not aid:
            errors.append(f"perActivityDemands[{i}].activityId is required.")
            continue
        if aid in by_input:
            errors.append(f"Duplicate per-activity demand entry for '{aid}'.")
            continue
        by_input[aid] = item

    rows_meta: List[Dict[str, Any]] = []
    demand_map: Dict[Tuple[str, str], float] = {}
    known_ids = {str(a.get("activityId", "")).strip() for a in activities}
    for aid in by_input:
        if aid not in known_ids:
            errors.append(f"Unknown activityId in perActivityDemands: '{aid}'.")

    for act in activities:
        aid = str(act.get("activityId", "")).strip()
        det = _det_flow(act)
        det_amount = float(det.get("amount", 0.0))
        det_unit = str(det.get("unit", "")).strip().upper()
        if det_amount <= 0:
            errors.append(f"Activity '{aid}': determining flow amount must be > 0.")
            det_amount = 1.0
        item = by_input.get(aid, {})
        enabled = bool(item.get("enabled", True))
        amount = item.get("amount", 1.0)
        try:
            amount = float(amount)
        except Exception:
            errors.append(f"Activity '{aid}': per-activity amount must be numeric.")
            amount = 0.0
        if amount < 0:
            errors.append(f"Activity '{aid}': per-activity amount must be >= 0.")
        unit = str(item.get("unit", det_unit)).strip().upper()
        if unit and det_unit and unit != det_unit:
            errors.append(
                f"Activity '{aid}': per-activity unit '{unit}' must match determining flow unit '{det_unit}'."
            )
        requested_amount = amount if enabled else 0.0
        activity_units_demand = (requested_amount / det_amount) if requested_amount > 0 else 0.0
        rows_meta.append(
            {
                "activityId": aid,
                "Activity": act.get("Activity", aid),
                "LCStage": act.get("LCStage", ""),
                "requestedAmount": _round_sig(float(requested_amount), 8),
                "requestedUnit": det_unit,
                "enabled": enabled,
                "determiningAmountInDpp": _round_sig(det_amount, 8),
                "activityUnitsDemand": _round_sig(activity_units_demand, 8),
            }
        )
        if enabled and requested_amount > 0:
            demand_map[(db_name, aid)] = activity_units_demand

    return rows_meta, demand_map, errors


def _run_per_activity_vector_scores(
    db_name: str,
    activities: List[Dict[str, Any]],
    demand_rows: List[Dict[str, Any]],
    method: Tuple[str, ...],
    cumulative_mode: bool,
) -> List[Dict[str, Any]]:
    """Compute incremental and cumulative scores for a strict activity demand vector."""
    rows: List[Dict[str, Any]] = []
    cumulative_demand: Dict[Tuple[str, str], float] = {}
    impact_unit = bd.methods[method].get("unit", "")

    demand_rows_by_id = {r["activityId"]: r for r in demand_rows}

    for act in activities:
        aid = str(act.get("activityId", "")).strip()
        meta = demand_rows_by_id.get(aid, {})
        requested_output_amt = float(meta.get("requestedAmount", 0.0) or 0.0)
        amt = float(meta.get("activityUnitsDemand", 0.0) or 0.0)

        incremental_demand = {(db_name, aid): amt} if amt > 0 else {}
        inc_score, _ = _score_demand(incremental_demand, method)

        cumulative_demand = dict(cumulative_demand)
        if amt > 0:
            cumulative_demand[(db_name, aid)] = cumulative_demand.get((db_name, aid), 0.0) + amt
        cum_score, _ = _score_demand(cumulative_demand, method)

        display_score = cum_score if cumulative_mode else inc_score
        rows.append(
            {
                "activityId": aid,
                "Activity": act.get("Activity", aid),
                "LCStage": act.get("LCStage", ""),
                "requestedAmount": _round_sig(requested_output_amt, 8),
                "requestedUnit": str(meta.get("requestedUnit", "")),
                "activityUnitsDemand": _round_sig(amt, 8),
                "incrementalScore": _round_sig(inc_score, 6),
                "cumulativeScore": _round_sig(cum_score, 6),
                "score": _round_sig(display_score, 6),
                "unit": impact_unit,
                "enabled": bool(meta.get("enabled", True)),
            }
        )
    return rows


def _fig_to_base64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=170, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("ascii")


def _make_impact_chart(rows: List[Dict[str, Any]], cumulative_mode: bool = False) -> str:
    labels = [r["activityId"] for r in rows]
    inc_values = [float(r.get("incrementalScore", r.get("score", 0.0)) or 0.0) for r in rows]
    cum_values = [float(r.get("cumulativeScore", r.get("score", 0.0)) or 0.0) for r in rows]

    if cumulative_mode:
        total = _round_sig(cum_values[-1], 6) if cum_values else 0.0
        labels2 = labels + ["Total"]
        bar_values = inc_values + [total]
        colors = ["#2d6a4f"] * len(inc_values) + ["#1b4332"]
        fig, ax = plt.subplots(figsize=(max(8, len(labels2) * 0.7), 4.8))
        bars = ax.bar(range(len(labels2)), bar_values, color=colors, alpha=0.85, label="Incremental impact")
        ax.plot(range(len(labels)), cum_values, color="#1d4ed8", marker="o", linewidth=2, label="Cumulative impact")
        ax.set_xticks(range(len(labels2)))
        ax.set_xticklabels(labels2, rotation=35, ha="right", fontsize=8)
        ax.set_title("Cumulative mode: bars = incremental, line = cumulative")
        for b, v in zip(bars, bar_values):
            ax.text(
                b.get_x() + b.get_width() / 2.0,
                b.get_height(),
                _fmt_sig(v, 6),
                ha="center",
                va="bottom",
                fontsize=8,
            )
        for x, y in zip(range(len(labels)), cum_values):
            ax.text(x, y, _fmt_sig(y, 6), ha="center", va="bottom", fontsize=7, color="#1d4ed8")
        ax.legend(loc="best", fontsize=8)
    else:
        total = _round_sig(sum(inc_values), 6)
        labels2 = labels + ["Total"]
        values2 = inc_values + [total]
        colors = ["#2d6a4f"] * len(inc_values) + ["#1b4332"]
        fig, ax = plt.subplots(figsize=(max(8, len(labels2) * 0.7), 4.5))
        bars = ax.bar(range(len(labels2)), values2, color=colors)
        ax.set_xticks(range(len(labels2)))
        ax.set_xticklabels(labels2, rotation=35, ha="right", fontsize=8)
        ax.set_title("DPP impacts per activity (scaled) + total")
        for b, v in zip(bars, values2):
            ax.text(
                b.get_x() + b.get_width() / 2.0,
                b.get_height(),
                _fmt_sig(v, 6),
                ha="center",
                va="bottom",
                fontsize=8,
            )
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    return _fig_to_base64(fig)


def _make_scenario_comparison_chart(
    baseline_total: float,
    current_total: float,
    unit: str,
    cumulative_mode: bool = False,
) -> str:
    """Compact comparison chart: baseline (all activities = 1) vs current scenario."""
    labels = ["Baseline (all = 1)", "Current scenario"]
    values = [float(baseline_total or 0.0), float(current_total or 0.0)]
    colors = ["#94a3b8", "#1b4332"]
    delta = values[1] - values[0]
    ratio = (values[1] / values[0]) if values[0] else None

    fig, ax = plt.subplots(figsize=(6.2, 3.8))
    bars = ax.bar(range(2), values, color=colors, width=0.55)
    ax.set_xticks(range(2))
    ax.set_xticklabels(labels, rotation=8, ha="right", fontsize=9)
    title = "Scenario comparison (cumulative total)" if cumulative_mode else "Scenario comparison (total impact)"
    ax.set_title(title, fontsize=11)
    ax.set_ylabel(unit or "")
    ymax = max(values) if values else 1.0
    ax.set_ylim(0, ymax * 1.22 if ymax > 0 else 1.0)
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            _fmt_sig(val, 6),
            ha="center",
            va="bottom",
            fontsize=9,
        )
    delta_txt = f"Delta: {_fmt_sig(delta, 6)} {unit or ''}"
    if ratio is not None:
        delta_txt += f"  ({_fmt_sig(ratio, 4)}x baseline)"
    ax.text(0.02, 0.98, delta_txt, transform=ax.transAxes, ha="left", va="top", fontsize=8, color="#334155")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    return _fig_to_base64(fig)


def _make_io_graph(dpp: Dict[str, Any], cumulative_mode: bool = False) -> str:
    acts = dpp.get("activities", [])
    flow_objs = dpp.get("flowObjects", [])
    fmap = {fo["flowObjectId"]: fo for fo in flow_objs if isinstance(fo, dict)}
    color_map = {"primary": "#2563eb", "secondary": "#16a34a", "elementary": "#dc2626"}

    # Build lane-based layout: input list -> activity box -> output list
    lane_heights = []
    lane_data = []
    for a in acts:
        flows = a.get("flows", [])
        inputs = [f for f in flows if f.get("direction") == "input"]
        outputs = [f for f in flows if f.get("direction") == "output"]
        rows = max(1, len(inputs), len(outputs))
        lane_heights.append(rows)
        lane_data.append((a, inputs, outputs, rows))

    total_rows = sum(lane_heights) + max(0, len(lane_heights) - 1) * 1.2
    fig_h = max(6.0, total_rows * 0.45)
    fig, ax = plt.subplots(figsize=(16, fig_h))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, total_rows + 1)
    ax.axis("off")

    y_cursor = total_rows
    centers: List[Tuple[float, float]] = []
    for (a, inputs, outputs, rows) in lane_data:
        lane_top = y_cursor
        lane_bottom = y_cursor - rows
        center_y = (lane_top + lane_bottom) / 2.0

        # Activity box
        ax.add_patch(plt.Rectangle((0.40, lane_bottom - 0.15), 0.20, rows + 0.30, fill=False, lw=1.0, ec="#334155"))
        ax.text(
            0.50,
            center_y,
            f"{a.get('activityId','')}\n{a.get('LCStage','')}",
            ha="center",
            va="center",
            fontsize=8,
        )
        centers.append((0.50, center_y))

        # Row labels
        ax.text(0.12, lane_top + 0.05, "Inputs", fontsize=8, weight="bold")
        ax.text(0.83, lane_top + 0.05, "Outputs", fontsize=8, weight="bold")

        # Inputs
        for i, fl in enumerate(inputs):
            y = lane_top - i - 0.5
            fo = fmap.get(fl.get("flowObjectId"), {})
            cls = fo.get("objectClass", "secondary")
            amt = _fmt_sig(float(fl.get("amount", 0.0)), 4)
            txt = f"{fo.get('name', fl.get('flowObjectId',''))} ({amt} {fl.get('unit','')})"
            ax.text(0.02, y, txt, ha="left", va="center", fontsize=7, color=color_map.get(cls, "#6b7280"))
            ax.annotate("", xy=(0.40, y), xytext=(0.30, y), arrowprops=dict(arrowstyle="->", lw=0.9, color="#64748b"))

        # Outputs
        for i, fl in enumerate(outputs):
            y = lane_top - i - 0.5
            fo = fmap.get(fl.get("flowObjectId"), {})
            cls = fo.get("objectClass", "secondary")
            amt = _fmt_sig(float(fl.get("amount", 0.0)), 4)
            txt = f"{fo.get('name', fl.get('flowObjectId',''))} ({amt} {fl.get('unit','')})"
            ax.text(0.62, y, txt, ha="left", va="center", fontsize=7, color=color_map.get(cls, "#6b7280"))
            ax.annotate("", xy=(0.62, y), xytext=(0.60, y), arrowprops=dict(arrowstyle="->", lw=0.9, color="#64748b"))

        # Lane separator
        ax.hlines(lane_bottom - 0.5, 0.01, 0.99, colors="#cbd5e1", linewidth=0.8, linestyles="--")
        y_cursor = lane_bottom - 1.2

    if cumulative_mode and len(centers) > 1:
        for i in range(1, len(centers)):
            x0, y0 = centers[i - 1]
            x1, y1 = centers[i]
            ax.annotate(
                "",
                xy=(x1 - 0.11, y1),
                xytext=(x0 - 0.11, y0),
                arrowprops=dict(arrowstyle="->", lw=1.3, color="#0f766e"),
            )
        ax.text(0.01, 0.75, "Cumulative mode: activities are chained in sequence", fontsize=8, color="#0f766e")

    ax.text(0.01, 0.20, "Colors: primary=blue, secondary=green, elementary=red", fontsize=8, color="#334155")
    ax.set_title("DPP IO graph (lane view by activity)", fontsize=11, pad=10)
    fig.tight_layout()
    return _fig_to_base64(fig)


def _activities_until_stop(dpp: Dict[str, Any], stop_activity_id: str) -> List[Dict[str, Any]]:
    acts = dpp.get("activities", [])
    idx = next((i for i, a in enumerate(acts) if a.get("activityId") == stop_activity_id), None)
    if idx is None:
        raise ValueError(f"Stop activity '{stop_activity_id}' not found in DPP.")
    return acts[: idx + 1]


def _handle_calculation(payload: Dict[str, Any], bw_dir: Path) -> Dict[str, Any]:
    project = _effective_project_name(payload.get("project", ""))
    create_project = bool(payload.get("createProject", False))
    import_bonsai = bool(payload.get("importBonsaiIfMissing", True))
    if _fixed_project_name():
        create_project = False
        import_bonsai = False
    method_label = str(payload.get("methodLabel", "")).strip()
    cumulative_mode = bool(payload.get("cumulativeMode", False))
    calculation_mode = str(payload.get("calculationMode", "single") or "single").strip().lower()
    if calculation_mode not in {"single", "per-activity"}:
        raise ValueError("calculationMode must be 'single' or 'per-activity'.")
    try:
        # Calculation path is strict/no-bootstrap: project seeding and BONSAI import
        # must be done in setup ("Refresh methods"), not during a calc job.
        setup = _ensure_project_and_bonsai(
            project=project,
            create_project=False,
            import_bonsai=False,
            bw_dir=bw_dir,
        )
    except Exception as exc:
        if CERISE_DISABLE_BOOTSTRAP or _fixed_project_name():
            raise ValueError(
                "Project is not ready in this runtime. Click 'Refresh methods' first, then run again. "
                f"Details: {exc}"
            ) from exc
        # Runtime can restart on Render; recover once using the requested bootstrap flags.
        try:
            setup = _ensure_project_and_bonsai(
                project=project,
                create_project=create_project,
                import_bonsai=import_bonsai,
                bw_dir=bw_dir,
            )
        except Exception as exc2:
            raise ValueError(
                "Project is not ready in this runtime. Click 'Refresh methods' first, then run again. "
                f"Details: {exc2}"
            ) from exc2

    method_labels = setup["methodLabels"]
    if not method_labels:
        raise ValueError("No EF v3.1 methods found in the selected project.")

    if not method_label:
        method_label = setup["defaultMethod"]
    if method_label not in method_labels:
        raise ValueError("Selected method is not available in current project.")

    dpp = payload.get("dpp")
    if not isinstance(dpp, dict):
        raise ValueError("DPP JSON must be provided as an object.")

    errors = _validate_dpp_strict(dpp)
    if errors:
        return {
            "ok": False,
            "errors": errors,
            "projectInfo": setup,
            "activities": [
                {"activityId": a.get("activityId", ""), "Activity": a.get("Activity", "")} for a in dpp.get("activities", [])
            ],
        }

    fu = payload.get("functionalUnit", {}) if isinstance(payload.get("functionalUnit"), dict) else {}
    stop_activity_id = str(payload.get("stopActivityId", "")).strip()
    target_amount = float(payload.get("referenceFlowAmount", 0.0))
    target_unit = str(payload.get("referenceFlowUnit", "")).strip().upper()
    baseline_total = None
    stop_activity: Optional[Dict[str, Any]] = None

    if calculation_mode == "single":
        if not stop_activity_id:
            raise ValueError("Please select a stop activity.")
        if not target_unit:
            raise ValueError("Reference flow unit is required.")
        subset = _activities_until_stop(dpp, stop_activity_id)
        stop_activity = subset[-1]
        scaling = _compute_scaling(stop_activity, target_amount, target_unit)
        demand_rows_meta = None
    else:
        subset = list(dpp.get("activities", []))
        if not subset:
            raise ValueError("No activities found in DPP.")
        stop_activity_id = ""
        scaling = 1.0
        demand_rows_meta, demand_map, demand_errors = _build_per_activity_demand_vector(
            activities=subset,
            raw_demands=payload.get("perActivityDemands"),
            db_name=_db_name_from_dpp_id(str(dpp.get("dppId", ""))),
        )
        if demand_errors:
            return {
                "ok": False,
                "errors": demand_errors,
                "projectInfo": setup,
                "activities": [
                    {"activityId": a.get("activityId", ""), "Activity": a.get("Activity", "")}
                    for a in dpp.get("activities", [])
                ],
            }
        # Baseline scenario for comparison: all DPP activities set to requested output = 1.
        baseline_raw_demands = []
        for a in subset:
            det = _det_flow(a)
            baseline_raw_demands.append(
                {
                    "activityId": a.get("activityId", ""),
                    "enabled": True,
                    "amount": 1.0,
                    "unit": str(det.get("unit", "")),
                }
            )
        baseline_rows_meta, _baseline_demand_map_unused, baseline_demand_errors = _build_per_activity_demand_vector(
            activities=subset,
            raw_demands=baseline_raw_demands,
            db_name=_db_name_from_dpp_id(str(dpp.get("dppId", ""))),
        )
        if baseline_demand_errors:
            return {
                "ok": False,
                "errors": [f"Baseline comparison setup failed: {e}" for e in baseline_demand_errors],
                "projectInfo": setup,
                "activities": [
                    {"activityId": a.get("activityId", ""), "Activity": a.get("Activity", "")}
                    for a in dpp.get("activities", [])
                ],
            }

    db_name = _db_name_from_dpp_id(str(dpp.get("dppId", "")))
    fg_payload, resolve_errors = _build_foreground_payload_strict(
        dpp=dpp,
        activities_subset=subset,
        db_name=db_name,
        bonsai_db=DEFAULT_BONSAI_DB,
        biosphere_db=DEFAULT_BIOSPHERE_DB,
        cumulative_mode=cumulative_mode,
    )
    if resolve_errors:
        return {
            "ok": False,
            "errors": [
                f"{e.activity_id} / {e.flow_id}: {e.reason}" for e in resolve_errors
            ],
            "projectInfo": setup,
            "activities": [
                {"activityId": a.get("activityId", ""), "Activity": a.get("Activity", "")} for a in dpp.get("activities", [])
            ],
            "dbName": db_name,
        }

    _write_foreground(fg_payload, db_name)

    method = _method_from_label(method_label)
    if calculation_mode == "single":
        rows = _run_activity_lca_scores(db_name=db_name, activities_subset=subset, scaling=scaling, method=method)
        # In single+cumulative mode, rows are cumulative by step because the foreground is chained.
        if cumulative_mode:
            prev = 0.0
            for r in rows:
                curr = float(r.get("score", 0.0) or 0.0)
                r["cumulativeScore"] = _round_sig(curr, 6)
                r["incrementalScore"] = _round_sig(curr - prev, 6)
                prev = curr
            total = _round_sig(rows[-1]["cumulativeScore"], 6) if rows else 0.0
        else:
            running = 0.0
            for r in rows:
                inc = float(r.get("score", 0.0) or 0.0)
                running += inc
                r["incrementalScore"] = _round_sig(inc, 6)
                r["cumulativeScore"] = _round_sig(running, 6)
            total = _round_sig(sum(r["incrementalScore"] for r in rows), 6)
    else:
        rows = _run_per_activity_vector_scores(
            db_name=db_name,
            activities=subset,
            demand_rows=demand_rows_meta or [],
            method=method,
            cumulative_mode=cumulative_mode,
        )
        baseline_rows = _run_per_activity_vector_scores(
            db_name=db_name,
            activities=subset,
            demand_rows=baseline_rows_meta or [],
            method=method,
            cumulative_mode=cumulative_mode,
        )
        if cumulative_mode:
            total = _round_sig(rows[-1]["cumulativeScore"], 6) if rows else 0.0
            baseline_total = _round_sig(baseline_rows[-1]["cumulativeScore"], 6) if baseline_rows else 0.0
        else:
            total = _round_sig(sum(float(r.get("incrementalScore", 0.0) or 0.0) for r in rows), 6)
            baseline_total = _round_sig(sum(float(r.get("incrementalScore", 0.0) or 0.0) for r in baseline_rows), 6)

    impact_activity_png = _make_impact_chart(rows, cumulative_mode=cumulative_mode)
    impact_comparison_png = None
    if calculation_mode == "per-activity":
        impact_comparison_png = _make_scenario_comparison_chart(
            baseline_total=baseline_total,
            current_total=total,
            unit=rows[0]["unit"] if rows else "",
            cumulative_mode=cumulative_mode,
        )
        impact_png = impact_comparison_png
    else:
        impact_png = impact_activity_png
    io_png = _make_io_graph(dpp, cumulative_mode=cumulative_mode)
    system_boundary = _derive_system_boundary(subset)
    impact_category = _impact_category_from_method(method)
    impact_indicator = _impact_indicator_from_method(method)
    secondary_data_source = "BONSAI"
    method_info = _method_ontology_info(method)
    ontology_reference_flow = _ontology_reference_flow(
        dpp_id=str(dpp.get("dppId", "")),
        calculation_mode=calculation_mode,
        target_amount=target_amount,
        target_unit=target_unit,
        stop_activity=stop_activity,
    )
    ontology_results = _ontology_result_rows(
        dpp_id=str(dpp.get("dppId", "")),
        rows=rows,
        method=method,
        system_boundary=system_boundary,
        secondary_data_source=secondary_data_source,
    )

    return {
        "ok": True,
        "errors": [],
        "projectInfo": setup,
        "dbName": db_name,
        "methodLabel": method_label,
        "impactCategory": impact_category,
        "impactIndicator": impact_indicator,
        "systemBoundary": system_boundary,
        "secondaryDataSource": secondary_data_source,
        "calculationMode": calculation_mode,
        "cumulativeMode": cumulative_mode,
        "scalingFactor": _round_sig(scaling, 8),
        "functionalUnit": {
            "What": str(fu.get("What", "")).strip(),
            "HowMuch": {
                "quantity": str(fu.get("HowMuchQuantity", "")).strip(),
                "unit": str(fu.get("HowMuchUnit", "")).strip(),
            },
            "HowLong": str(fu.get("HowLong", "")).strip(),
            "HowWell": str(fu.get("HowWell", "")).strip(),
        },
        "referenceFlow": {"amount": target_amount, "unit": target_unit} if calculation_mode == "single" else None,
        "stopActivityId": stop_activity_id if calculation_mode == "single" else None,
        "table": rows,
        "total": total,
        "unit": rows[0]["unit"] if rows else "",
        "baselineTotal": baseline_total if calculation_mode == "per-activity" else None,
        "activities": [
            {"activityId": a.get("activityId", ""), "Activity": a.get("Activity", "")} for a in dpp.get("activities", [])
        ],
        "impactPng": impact_png,
        "impactActivityPng": impact_activity_png,
        "impactComparisonPng": impact_comparison_png,
        "ioPng": io_png,
        "ontologyResultView": {
            "ontologyFile": "ontology/DPPLCA/dpplca_output.ttl",
            "ontologyIri": "http://example.org/dpplca_output",
            "resultsGranularity": "LCStage",
            "functionalUnit": {
                "whatProvided": str(fu.get("What", "")).strip(),
                "howMuchProvided": _functional_unit_amount_literal(fu),
                "howLongProvided": str(fu.get("HowLong", "")).strip(),
                "howWellProvided": str(fu.get("HowWell", "")).strip(),
                "hasReferenceFlow": ontology_reference_flow,
            },
            "results": ontology_results,
            "summary": {
                "forImpactCategory": {
                    "uri": method_info.get("impactCategoryUri") or None,
                    "label": impact_category,
                },
                "calculatedWithMethod": {
                    "uri": method_info.get("methodUri") or None,
                    "label": method_label,
                },
                "hasSystemBoundary": {
                    "uri": _system_boundary_uri(system_boundary),
                    "label": system_boundary,
                },
                "usesSecondaryDataFrom": secondary_data_source,
                "totalImpactValue": {
                    "uri": method_info.get("impactIndicatorUri") or None,
                    "label": impact_indicator,
                    "value": total,
                    "unit": rows[0]["unit"] if rows else "",
                },
                "baselineTotalImpactValue": (
                    {
                        "uri": method_info.get("impactIndicatorUri") or None,
                        "label": impact_indicator,
                        "value": baseline_total,
                        "unit": rows[0]["unit"] if rows else "",
                    }
                    if calculation_mode == "per-activity" and baseline_total is not None
                    else None
                ),
            },
        },
    }


HTML = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>DPP Impact Calculator (BW2)</title>
  <style>
    :root {
      --bg:#eef2ff; --card:#ffffff; --line:#cbd5e1; --txt:#0f172a; --muted:#64748b;
      --ok:#15803d; --err:#b91c1c; --warn:#92400e; --brand:#1d4ed8;
    }
    * { box-sizing: border-box; }
    body { margin:0; font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",Roboto,Helvetica,Arial,sans-serif; background:var(--bg); color:var(--txt); }
    header { padding:14px 18px; border-bottom:1px solid var(--line); background:linear-gradient(120deg,#dbeafe,#e0e7ff); }
    h1 { margin:0; font-size:24px; }
    .sub { margin:4px 0 0; color:var(--muted); }
    .wrap { display:grid; grid-template-columns: 430px 1fr; gap:12px; padding:12px; align-items:start; }
    .card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; box-shadow:0 2px 8px rgba(15,23,42,.05); }
    .card h2 { margin:0 0 8px; font-size:16px; border-bottom:1px solid #e2e8f0; padding-bottom:6px; }
    label { display:block; font-size:12px; color:var(--muted); margin-bottom:4px; }
    input, select, textarea { width:100%; border:1px solid var(--line); border-radius:8px; padding:8px; font-size:13px; }
    textarea { min-height:260px; font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; }
    .row2 { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
    .btn { border:1px solid #1e40af; background:var(--brand); color:#fff; border-radius:8px; padding:8px 12px; cursor:pointer; font-weight:600; }
    .btn.secondary { background:#fff; color:#1e293b; border-color:var(--line); }
    .status { padding:8px; border-radius:8px; border:1px solid #e2e8f0; font-size:13px; margin-bottom:8px; }
    .status.ok { background:#ecfdf5; color:var(--ok); border-color:#bbf7d0; }
    .status.err { background:#fef2f2; color:var(--err); border-color:#fecaca; }
    .status.warn { background:#fffbeb; color:var(--warn); border-color:#fde68a; }
    .errors { margin:0; padding-left:18px; color:var(--err); font-size:12px; }
    .tbl { width:100%; border-collapse:collapse; font-size:12px; }
    .tbl th,.tbl td { border:1px solid #e2e8f0; padding:6px; }
    .tbl th { background:#f8fafc; text-align:left; }
    .pill { display:inline-block; background:#eff6ff; border:1px solid #bfdbfe; color:#1e3a8a; border-radius:999px; padding:2px 8px; font-size:11px; margin-right:5px; }
    img { width:100%; border:1px solid #e2e8f0; border-radius:10px; background:#fff; }
    .graph-grid { display:grid; grid-template-columns:1fr; gap:10px; }
    .graph-sub { border:1px solid #e2e8f0; border-radius:10px; padding:8px; background:#f8fafc; }
    .graph-sub h3 { margin:0 0 6px; font-size:13px; color:#334155; }
    .graph-img { width:auto; max-width:100%; max-height:320px; object-fit:contain; display:block; margin:0 auto; }
    #impactGraphGrid.per-activity { grid-template-columns: minmax(260px, 0.95fr) minmax(320px, 1.25fr); }
    #impactGraphGrid.single-mode { grid-template-columns:1fr; }
    .muted { color:var(--muted); font-size:12px; }
    .compact td,.compact th { font-size:11px; padding:4px; }
    .compact input[type="number"] { padding:5px 6px; font-size:12px; }
    .compact input[type="checkbox"] { width:auto; transform:scale(1.05); }
    .hidden { display:none; }
    @media (max-width: 1200px) { .wrap { grid-template-columns: 1fr; } }
    @media (max-width: 980px) { #impactGraphGrid.per-activity { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <header>
    <h1>DPP Impact Calculator (Strict, BW2)</h1>
    <p class=\"sub\">Upload or paste DPP JSON, choose project/method/stop activity, define FU + reference flow, then run.</p>
  </header>
  <div class=\"wrap\">
    <div>
      <section class=\"card\">
        <h2>Brightway setup</h2>
        <div class=\"row2\">
          <div>
            <label>Existing project</label>
            <select id=\"projectSelect\"></select>
          </div>
          <div>
            <label>Or new project name</label>
            <input id=\"newProject\" placeholder=\"Type to create/use\" />
          </div>
        </div>
        <button class=\"btn\" id=\"connectBtn\">Refresh methods</button>
      </section>

      <section class=\"card\">
        <h2>DPP input</h2>
        <div class=\"row2\" style=\"margin-bottom:8px;\">
          <button class=\"btn secondary\" id=\"uploadDppBtn\">Upload DPP JSON</button>
          <button class=\"btn secondary\" id=\"pasteDppBtn\">Paste DPP</button>
          <input id=\"uploadDppInput\" type=\"file\" accept=\".json,application/json\" style=\"display:none\" />
        </div>
        <label>DPP JSON</label>
        <textarea id=\"dppJson\" placeholder=\"Paste DPP JSON here...\"></textarea>
        <div class=\"muted\">Strict mode: no fallback mapping; unresolved flows block calculation.</div>
      </section>

      <section class=\"card\">
        <h2>Calculation controls</h2>
        <div class=\"row2\">
          <div>
            <label>EF v3.1 method</label>
            <select id=\"methodSelect\"></select>
          </div>
          <div>
            <label>Stop at activity</label>
            <select id=\"stopActivity\"></select>
          </div>
        </div>
        <div class=\"row2\" style=\"margin-top:8px;\">
          <div>
            <label>Calculation mode</label>
            <select id=\"calcMode\">
              <option value=\"single\">Single reference flow (current behavior)</option>
              <option value=\"per-activity\">Per-activity reference flows (strict)</option>
            </select>
          </div>
        </div>
        <h2 style=\"margin-top:12px;\">Functional Unit + Reference Flow</h2>
        <div class=\"row2\" style=\"margin-top:8px;\">
          <div>
            <label>FU - What</label>
            <input id=\"fuWhat\" placeholder=\"e.g., one black printer cartridge\" />
          </div>
          <div>
            <label>FU - How much (quantity, free text)</label>
            <input id=\"fuHowMuchQuantity\" placeholder=\"e.g., 1 cartridge / 10,000 pages\" />
          </div>
        </div>
        <div class=\"row2\" style=\"margin-top:8px;\">
          <div>
            <label>FU - How much (unit, free text)</label>
            <input id=\"fuHowMuchUnit\" placeholder=\"e.g., cartridge / page\" />
          </div>
          <div>
            <label>FU - How long</label>
            <input id=\"fuHowLong\" placeholder=\"e.g., 1 year\" />
          </div>
          <div>
            <label>FU - How well</label>
            <input id=\"fuHowWell\" placeholder=\"e.g., prints as specified\" />
          </div>
        </div>
        <div class=\"row2\" style=\"margin-top:8px;\">
          <div>
            <label>Reference flow amount</label>
            <input id=\"refAmount\" type=\"number\" step=\"any\" value=\"1\" />
          </div>
          <div>
            <label>Reference flow unit</label>
            <select id=\"refUnit\"></select>
          </div>
        </div>
        <div class=\"row2\" style=\"margin-top:8px;\">
          <label><input id=\"cumulativeMode\" type=\"checkbox\" /> Cumulative chaining (connect each activity output to next activity input)</label>
        </div>
        <div class=\"row2\" style=\"margin-top:8px;\">
          <div></div>
          <button class=\"btn secondary\" id=\"runBtn\">Run now</button>
        </div>
      </section>

      <section class=\"card\" id=\"perActCard\">
        <h2>Per-activity Reference Flows (strict)</h2>
        <div class=\"muted\">Generated from the uploaded/pasted DPP only. Default requested output = 1 for each activity. Unit is read-only (activity determining flow unit). In DPP rules mode, this table is disabled to avoid double counting.</div>
        <div class=\"row2\" style=\"margin:8px 0;\">
          <button class=\"btn secondary\" id=\"perActResetBtn\">Reset All Amounts to 1</button>
          <div class=\"muted\" id=\"perActCount\"></div>
        </div>
        <table class=\"tbl compact\" id=\"perActTable\">
          <thead>
            <tr>
              <th>Use</th>
              <th>Activity ID</th>
              <th>Activity</th>
              <th>Stage</th>
              <th>Req. output amount</th>
              <th>Unit</th>
              <th>DPP det. amount</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </section>
    </div>

    <div>
      <section class=\"card\">
        <h2>Status</h2>
        <div id=\"status\" class=\"status warn\">Waiting for input...</div>
        <ul id=\"errors\" class=\"errors\"></ul>
        <div id=\"meta\" class=\"muted\"></div>
      </section>

      <section class=\"card\">
        <h2>Impact table</h2>
        <table class=\"tbl\" id=\"impactTable\">
          <thead><tr><th>Activity ID</th><th>Activity</th><th>Stage</th><th>Req. output</th><th>RF unit</th><th>Activity units</th><th>Incremental impact</th><th>Cumulative impact</th><th>Impact unit</th></tr></thead>
          <tbody></tbody>
        </table>
        <div id=\"total\" style=\"margin-top:8px; font-weight:700;\"></div>
        <div class=\"row2\" style=\"margin-top:8px;\">
          <button class=\"btn secondary\" id=\"downloadCsvBtn\">Download results (CSV)</button>
        </div>
      </section>

      <section class=\"card\">
        <h2>Impact graph</h2>
        <div id=\"impactGraphGrid\" class=\"graph-grid single-mode\">
          <div id=\"impactComparisonWrap\" class=\"graph-sub hidden\">
            <h3>Scenario comparison (total)</h3>
            <img id=\"impactComparisonImg\" class=\"graph-img\" alt=\"Scenario comparison graph\" />
          </div>
          <div id=\"impactActivityWrap\" class=\"graph-sub\">
            <h3>Activity detail</h3>
            <img id=\"impactActivityImg\" class=\"graph-img\" alt=\"Activity impact graph\" />
          </div>
        </div>
      </section>

      <section class=\"card\">
        <h2>DPP IO graph (full DPP)</h2>
        <span class=\"pill\">primary: blue</span>
        <span class=\"pill\">secondary: green</span>
        <span class=\"pill\">elementary: red</span>
        <img id=\"ioImg\" alt=\"IO graph\" />
      </section>
    </div>
  </div>

<script>
const S = {
  methods: [],
  projects: [],
  timer: null,
  lastResult: null,
  fixedProject: '',
};
const UNIT_LABELS = {
  C62: 'piece',
  KGM: 'kilogram',
  TNE: 'tonne',
  KWH: 'kilowatt-hour',
  TJ: 'terajoule',
  KMT: 'kilometre',
  TKM: 'tonne-kilometre',
  LTR: 'litre',
  MTQ: 'cubic metre',
  GRM: 'gram',
  MTR: 'metre',
  MTK: 'square metre',
  MJ: 'megajoule'
};

function q(id){ return document.getElementById(id); }

function setStatus(kind, txt){
  const el = q('status');
  el.className = 'status ' + kind;
  el.textContent = txt;
}

function renderErrors(arr){
  const ul = q('errors');
  ul.innerHTML = '';
  (arr || []).forEach(e => {
    const li = document.createElement('li');
    li.textContent = e;
    ul.appendChild(li);
  });
}

function optionize(el, values, selected=''){
  el.innerHTML = '';
  (values || []).forEach(v => {
    const o = document.createElement('option');
    o.value = v;
    o.textContent = v;
    if (String(v) === String(selected)) o.selected = true;
    el.appendChild(o);
  });
}

function formatSig(v, sig=6){
  const n = Number(v);
  if (!Number.isFinite(n)) return '';
  return Number.parseFloat(n.toPrecision(sig)).toString();
}
function unitLabel(code){
  const c = String(code || '').trim().toUpperCase();
  return UNIT_LABELS[c] || c || '';
}
function unitOptionHtml(code){
  const c = String(code || '').trim().toUpperCase();
  const label = unitLabel(c);
  return `${label}`;
}

function setupProjectValue(){
  if (S.fixedProject) return S.fixedProject;
  const newProject = q('newProject').value.trim();
  const selected = (q('projectSelect').value || '').trim();
  return newProject || selected;
}

function calcProjectValue(){
  if (S.fixedProject) return S.fixedProject;
  // For calculations, prefer the currently selected existing project.
  // This avoids stale typed project names after a backend restart.
  const selected = (q('projectSelect').value || '').trim();
  const newProject = q('newProject').value.trim();
  return selected || newProject;
}

function setupOptions(){
  if (S.fixedProject){
    return {
      createProject: false,
      importBonsaiIfMissing: false,
      creatingNew: false,
    };
  }
  const newProject = q('newProject').value.trim();
  const creatingNew = !!newProject;
  return {
    createProject: true,
    importBonsaiIfMissing: true,
    creatingNew,
  };
}

function collectPayload(){
  const project = calcProjectValue();
  const setup = setupOptions();
  let dpp = null;
  const txt = q('dppJson').value.trim();
  if (txt){
    dpp = JSON.parse(txt);
  }
  return {
    project,
    createProject: setup.createProject,
    importBonsaiIfMissing: setup.importBonsaiIfMissing,
    methodLabel: q('methodSelect').value,
    calculationMode: q('calcMode').value,
    stopActivityId: q('stopActivity').value,
    cumulativeMode: q('cumulativeMode').checked,
    functionalUnit: {
      What: q('fuWhat').value,
      HowMuchQuantity: q('fuHowMuchQuantity').value,
      HowMuchUnit: q('fuHowMuchUnit').value,
      HowLong: q('fuHowLong').value,
      HowWell: q('fuHowWell').value,
    },
    referenceFlowAmount: Number(q('refAmount').value),
    referenceFlowUnit: q('refUnit').value,
    perActivityDemands: readPerActivityDemandRows(),
    dpp,
  };
}

async function postJson(url, payload){
  const r = await fetch(url, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify(payload),
  });
  const raw = await r.text();
  let j = null;
  try {
    j = raw ? JSON.parse(raw) : {};
  } catch (_e) {
    const preview = String(raw || '').slice(0, 240).replace(/\\s+/g, ' ');
    throw new Error(`HTTP ${r.status} ${r.statusText || ''} from ${url}. Non-JSON response: ${preview || '<empty>'}`);
  }
  if (!r.ok) {
    const msg = j?.error || ('HTTP ' + r.status);
    const tr = j?.trace ? `\n${j.trace}` : '';
    throw new Error(msg + tr);
  }
  return j;
}

async function getJson(url){
  const r = await fetch(url, { method:'GET' });
  const raw = await r.text();
  let j = null;
  try {
    j = raw ? JSON.parse(raw) : {};
  } catch (_e) {
    const preview = String(raw || '').slice(0, 240).replace(/\\s+/g, ' ');
    throw new Error(`HTTP ${r.status} ${r.statusText || ''} from ${url}. Non-JSON response: ${preview || '<empty>'}`);
  }
  if (!r.ok) {
    const msg = j?.error || ('HTTP ' + r.status);
    const tr = j?.trace ? `\n${j.trace}` : '';
    throw new Error(msg + tr);
  }
  return j;
}

function sleep(ms){
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function pollJob(jobId, label, timeoutMs=20*60*1000){
  const start = Date.now();
  let transientFails = 0;
  while (true){
    try {
      const j = await getJson(`/api/job?id=${encodeURIComponent(jobId)}`);
      transientFails = 0;
      if (j.status === 'done'){
        return j.result;
      }
      if (j.status === 'error'){
        const tr = j.trace ? `\n${j.trace}` : '';
        throw new Error((j.error || `Unknown ${label} job error`) + tr);
      }
      if ((Date.now() - start) > timeoutMs){
        throw new Error(`${label} job timed out. Please retry.`);
      }
      await sleep(1500);
    } catch (e) {
      const msg = String(e?.message || e);
      const transient = msg.includes('HTTP 502') || msg.includes('HTTP 503') || msg.includes('HTTP 504');
      if (!transient) throw e;
      transientFails += 1;
      if ((Date.now() - start) > timeoutMs){
        throw new Error(`${label} job timed out after repeated gateway errors. Please retry.`);
      }
      setStatus('warn', `${label} is still running. Temporary gateway issue; retrying...`);
      await sleep(Math.min(6000, 1000 + transientFails * 500));
    }
  }
}

async function startAndPollWithOneRecovery(startUrl, payload, label){
  const started = await postJson(startUrl, payload);
  return await pollJob(started.jobId, label);
}

function renderTable(rows){
  const tb = q('impactTable').querySelector('tbody');
  tb.innerHTML = '';
  (rows || []).forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${r.activityId||''}</td><td>${r.Activity||''}</td><td>${r.LCStage||''}</td><td>${formatSig(r.requestedAmount, 8)}</td><td>${unitLabel(r.requestedUnit||'')}</td><td>${formatSig(r.activityUnitsDemand, 8)}</td><td>${formatSig(r.incrementalScore, 6)}</td><td>${formatSig(r.cumulativeScore, 6)}</td><td>${r.unit||''}</td>`;
    tb.appendChild(tr);
  });
}

function downloadBlob(filename, content, contentType){
  const blob = new Blob([content], {type: contentType});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function downloadResultsCsv(){
  const r = S.lastResult;
  if (!r || !r.ok){
    setStatus('warn', 'Run a calculation first, then download.');
    return;
  }
  const lines = [];
  lines.push('Activity ID,Activity,Stage,Requested output amount,Reference flow unit,Activity units demand,Incremental impact,Cumulative impact,Impact unit');
  (r.table || []).forEach(x => {
    const cols = [
      x.activityId || '',
      (x.Activity || '').replaceAll('\"','\"\"'),
      (x.LCStage || '').replaceAll('\"','\"\"'),
      formatSig(x.requestedAmount, 8),
      unitLabel(x.requestedUnit || ''),
      formatSig(x.activityUnitsDemand, 8),
      formatSig(x.incrementalScore, 6),
      formatSig(x.cumulativeScore, 6),
      x.unit || '',
    ];
    lines.push(`${cols[0]},\"${cols[1]}\",\"${cols[2]}\",${cols[3]},${cols[4]},${cols[5]},${cols[6]},${cols[7]},${cols[8]}`);
  });
  lines.push('');
  lines.push(`Total,,,,,,,${formatSig(r.total, 6)},${r.unit || ''}`);
  downloadBlob('dpp_lca_results.csv', lines.join('\\n'), 'text/csv;charset=utf-8');
}

function refreshStopDropdownFromJson(){
  const sel = q('stopActivity');
  let current = sel.value;
  let acts = [];
  try {
    const txt = q('dppJson').value.trim();
    if (!txt) { optionize(sel, []); refreshPerActivityDemandTableFromJson(); return; }
    const dpp = JSON.parse(txt);
    acts = (dpp.activities || []).map(a => `${a.activityId} :: ${a.Activity || ''}`);
    optionize(sel, acts, current);
    if (!sel.value && acts.length) sel.value = acts[acts.length-1];
    const units = [];
    for (const a of (dpp.activities || [])){
      const det = (a.flows || []).find(f => f.flowId === a.determiningFlowId) || {};
      const u = String(det.unit || '').trim().toUpperCase();
      if (u && !units.includes(u)) units.push(u);
    }
    const refSel = q('refUnit');
    const currentUnit = refSel?.value || '';
    if (refSel){
      refSel.innerHTML = '';
      units.forEach(u => {
        const o = document.createElement('option');
        o.value = u;
        o.textContent = unitOptionHtml(u);
        if (u === currentUnit) o.selected = true;
        refSel.appendChild(o);
      });
      if (!refSel.value && units.length){
        refSel.value = units[0];
      }
    }
    refreshPerActivityDemandTableFromJson();
  } catch (_) {
    optionize(sel, []);
    const refSel = q('refUnit');
    if (refSel){
      refSel.innerHTML = '';
      const o = document.createElement('option');
      o.value = 'C62';
      o.textContent = unitOptionHtml('C62');
      o.selected = true;
      refSel.appendChild(o);
    }
    refreshPerActivityDemandTableFromJson();
  }
}

function selectedStopActivityId(){
  const raw = q('stopActivity').value || '';
  return raw.split(' :: ')[0].trim();
}

function readPerActivityDemandRows(){
  const rows = [];
  q('perActTable').querySelectorAll('tbody tr').forEach(tr => {
    rows.push({
      activityId: tr.dataset.activityId || '',
      enabled: !!tr.querySelector('input[data-k="enabled"]')?.checked,
      amount: Number(tr.querySelector('input[data-k="amount"]')?.value || 0),
      unit: tr.dataset.detUnit || '',
    });
  });
  return rows;
}

function refreshPerActivityDemandTableFromJson(){
  const tb = q('perActTable').querySelector('tbody');
  const prev = {};
  tb.querySelectorAll('tr').forEach(tr => {
    prev[tr.dataset.activityId || ''] = {
      enabled: !!tr.querySelector('input[data-k="enabled"]')?.checked,
      amount: tr.querySelector('input[data-k="amount"]')?.value || '1',
    };
  });
  tb.innerHTML = '';
  q('perActCount').textContent = '';
  const txt = q('dppJson').value.trim();
  if (!txt) return;
  try {
    const dpp = JSON.parse(txt);
    const acts = Array.isArray(dpp.activities) ? dpp.activities : [];
    const detMap = {};
    for (const a of acts){
      const det = (a.flows || []).find(f => f.flowId === a.determiningFlowId) || {};
      detMap[a.activityId] = det;
    }
    for (const a of acts){
      const det = detMap[a.activityId] || {};
      const detUnit = String(det.unit || '');
      const detAmt = Number(det.amount ?? 1);
      const p = prev[a.activityId] || {enabled: true, amount: '1'};
      const tr = document.createElement('tr');
      tr.dataset.activityId = a.activityId || '';
      tr.dataset.detUnit = detUnit;
      tr.innerHTML = `
        <td><input data-k="enabled" type="checkbox" ${p.enabled ? 'checked' : ''}></td>
        <td>${a.activityId || ''}</td>
        <td>${a.Activity || ''}</td>
        <td>${a.LCStage || ''}</td>
        <td><input data-k="amount" type="number" step="any" min="0" value="${String(p.amount).replace(/"/g,'&quot;')}"></td>
        <td>${unitLabel(detUnit)}</td>
        <td>${formatSig(detAmt, 8)}</td>
      `;
      tb.appendChild(tr);
    }
    q('perActCount').textContent = `${acts.length} DPP activities`;
  } catch (_) {
    // ignore invalid JSON here; main validator will report at run time
  }
}

function updateCalcModeUi(){
  const mode = q('calcMode').value;
  const per = mode === 'per-activity';
  q('perActCard').classList.toggle('hidden', !per);
  q('perActResetBtn').disabled = !per;
  q('stopActivity').disabled = per;
  q('refAmount').disabled = per;
  q('refUnit').disabled = per;
  q('perActTable').querySelectorAll('tbody input').forEach(el => {
    el.disabled = !per;
  });
}

function uploadDppFromFile(file){
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    try {
      const txt = String(reader.result || '');
      JSON.parse(txt);
      q('dppJson').value = txt;
      refreshStopDropdownFromJson();
      setStatus('ok', 'DPP JSON uploaded.');
      renderErrors([]);
    } catch (e) {
      setStatus('err', 'Invalid JSON file.');
      renderErrors([String(e.message || e)]);
    }
  };
  reader.readAsText(file);
}

async function pasteDppFromClipboard(){
  try {
    const txt = await navigator.clipboard.readText();
    JSON.parse(txt);
    // Always replace current content with pasted DPP
    q('dppJson').value = txt;
    refreshStopDropdownFromJson();
    setStatus('ok', 'DPP JSON pasted.');
    renderErrors([]);
  } catch (e) {
    setStatus('err', 'Paste failed or clipboard JSON is invalid.');
    renderErrors([String(e.message || e)]);
  }
}

async function refreshMethods(){
  setStatus('warn', 'Preparing Brightway project and methods...');
  renderErrors([]);
  try {
    const setup = setupOptions();
    const payload = {
      project: setupProjectValue(),
      createProject: setup.createProject,
      importBonsaiIfMissing: setup.importBonsaiIfMissing,
    };
    const j = await startAndPollWithOneRecovery('/api/setup_start', payload, 'Setup');
    S.projects = j.projects || [];
    optionize(q('projectSelect'), S.projects, j.projectInfo?.project || '');
    q('newProject').value = '';
    S.methods = j.projectInfo?.methodLabels || [];
    optionize(q('methodSelect'), S.methods, j.projectInfo?.defaultMethod || '');
    setStatus('ok', 'Project ready. Methods loaded.');
    q('meta').textContent = `Project: ${j.projectInfo?.project || ''} | BONSAI imported now: ${j.projectInfo?.bonsaiImported ? 'yes' : 'no'}`;
  } catch (e) {
    setStatus('err', 'Setup failed.');
    const msg = String(e?.message || e);
    const stk = String(e?.stack || '').trim();
    renderErrors(stk && !stk.includes(msg) ? [msg, stk] : [msg]);
  }
}

async function runCalc(){
  setStatus('warn', 'Running strict DPP calculation...');
  renderErrors([]);
  try {
    refreshStopDropdownFromJson();
    const payload = collectPayload();
    if (payload.calculationMode === 'single'){
      payload.stopActivityId = selectedStopActivityId();
    } else {
      payload.stopActivityId = '';
    }

    const j = await startAndPollWithOneRecovery('/api/calc_start', payload, 'Calculation');

    if (!j.ok) {
      setStatus('err', 'Blocked by strict validation/mapping errors.');
      renderErrors(j.errors || ['Unknown error']);
      if (j.activities){
        const opts = j.activities.map(a => `${a.activityId} :: ${a.Activity || ''}`);
        const sel = q('stopActivity').value;
        optionize(q('stopActivity'), opts, sel);
      }
      renderTable([]);
      q('impactComparisonImg').removeAttribute('src');
      q('impactActivityImg').removeAttribute('src');
      q('impactComparisonWrap').classList.add('hidden');
      q('impactActivityWrap').classList.add('hidden');
      q('impactGraphGrid').classList.remove('per-activity');
      q('impactGraphGrid').classList.add('single-mode');
      q('ioImg').removeAttribute('src');
      q('total').textContent = '';
      q('meta').textContent = j.dbName ? `Foreground DB: ${j.dbName}` : '';
      S.lastResult = null;
      return;
    }

    setStatus('ok', 'Calculation completed.');
    renderErrors([]);

    const opts = (j.activities || []).map(a => `${a.activityId} :: ${a.Activity || ''}`);
    if (j.stopActivityId){
      optionize(q('stopActivity'), opts, `${j.stopActivityId} :: ${(j.activities||[]).find(a=>a.activityId===j.stopActivityId)?.Activity||''}`);
    } else {
      optionize(q('stopActivity'), opts, q('stopActivity').value);
    }

    renderTable(j.table || []);
    const perMode = j.calculationMode === 'per-activity';
    const comparisonPng = j.impactComparisonPng || null;
    const activityPng = j.impactActivityPng || j.impactPng || null;
    const graphGrid = q('impactGraphGrid');
    if (perMode && comparisonPng){
      graphGrid.classList.add('per-activity');
      graphGrid.classList.remove('single-mode');
      q('impactComparisonWrap').classList.remove('hidden');
      q('impactComparisonImg').src = `data:image/png;base64,${comparisonPng}`;
      q('impactComparisonWrap').querySelector('h3').textContent = j.cumulativeMode
        ? 'Scenario comparison (cumulative total)'
        : 'Scenario comparison (total)';
    } else {
      graphGrid.classList.remove('per-activity');
      graphGrid.classList.add('single-mode');
      q('impactComparisonWrap').classList.add('hidden');
      q('impactComparisonImg').removeAttribute('src');
    }
    if (activityPng){
      q('impactActivityWrap').classList.remove('hidden');
      q('impactActivityImg').src = `data:image/png;base64,${activityPng}`;
    } else {
      q('impactActivityWrap').classList.add('hidden');
      q('impactActivityImg').removeAttribute('src');
    }
    q('impactActivityWrap').querySelector('h3').textContent = perMode
      ? 'Activity detail (single activity impacts)'
      : 'Activity impact';
    q('ioImg').src = `data:image/png;base64,${j.ioPng}`;
    q('total').textContent = `Total: ${formatSig(j.total, 6)} ${j.unit || ''}`;
    const scalingTxt = (j.calculationMode === 'per-activity') ? 'n/a (manual table)' : formatSig(j.scalingFactor, 8);
    const baselineTxt = (j.calculationMode === 'per-activity' && j.baselineTotal !== null && j.baselineTotal !== undefined)
      ? ` | Baseline: ${formatSig(j.baselineTotal, 6)} ${j.unit || ''}`
      : '';
    q('meta').textContent = `Project: ${j.projectInfo?.project || ''} | Foreground DB: ${j.dbName || ''} | Method: ${j.methodLabel || ''} | Mode: ${j.calculationMode || 'single'} | Scaling factor: ${scalingTxt} | Cumulative: ${j.cumulativeMode ? 'ON' : 'OFF'}${baselineTxt}`;
    S.lastResult = j;
  } catch (e) {
    setStatus('err', 'Run failed.');
    renderErrors([String(e.message || e)]);
    S.lastResult = null;
  }
}

function scheduleRun(){
  // Intentionally disabled: requires explicit user confirmation via Run button.
  return;
}

window.addEventListener('DOMContentLoaded', async () => {
  try {
    const init = await fetch('/api/init').then(r => r.json());
    S.fixedProject = String(init.fixedProject || '').trim();
    if (S.fixedProject){
      optionize(q('projectSelect'), [S.fixedProject], S.fixedProject);
      q('projectSelect').disabled = true;
      q('newProject').value = '';
      q('newProject').disabled = true;
      q('newProject').placeholder = 'Fixed by server';
    } else {
      q('projectSelect').disabled = false;
      q('newProject').disabled = false;
      optionize(q('projectSelect'), init.projects || [], init.currentProject || '');
    }
  } catch (_) {}

  refreshStopDropdownFromJson();

  q('connectBtn').addEventListener('click', refreshMethods);
  q('runBtn').addEventListener('click', runCalc);
  q('downloadCsvBtn').addEventListener('click', downloadResultsCsv);
  q('perActResetBtn').addEventListener('click', () => {
    q('perActTable').querySelectorAll('tbody tr').forEach(tr => {
      const cb = tr.querySelector('input[data-k="enabled"]');
      const amt = tr.querySelector('input[data-k="amount"]');
      if (cb) cb.checked = true;
      if (amt) amt.value = '1';
    });
  });
  q('uploadDppBtn').addEventListener('click', () => q('uploadDppInput').click());
  q('pasteDppBtn').addEventListener('click', pasteDppFromClipboard);
  q('uploadDppInput').addEventListener('change', (ev) => {
    const f = ev.target?.files?.[0];
    uploadDppFromFile(f);
    ev.target.value = '';
  });

  ['projectSelect','newProject','methodSelect','stopActivity','refAmount','refUnit','dppJson','fuWhat','fuHowMuchQuantity','fuHowMuchUnit','fuHowLong','fuHowWell','cumulativeMode','calcMode']
    .forEach(id => {
      const el = q(id);
      if (!el) return;
      el.addEventListener('input', () => {
        if (id === 'dppJson') refreshStopDropdownFromJson();
        if (id === 'calcMode') updateCalcModeUi();
        if (id === 'newProject') setupOptions();
      });
      el.addEventListener('change', () => {
        if (id === 'dppJson') refreshStopDropdownFromJson();
        if (id === 'calcMode') updateCalcModeUi();
        if (id === 'newProject') setupOptions();
      });
    });

  q('calcMode').addEventListener('change', updateCalcModeUi);
  setStatus('warn', 'Click "Refresh methods" to initialize project and methods.');
  setupOptions();
  refreshPerActivityDemandTableFromJson();
  updateCalcModeUi();
});
</script>
</body>
</html>
"""


def make_handler(bw_dir: Path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            route = parsed.path

            if route in {"/", "/index.html"}:
                body = HTML.encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if route == "/api/init":
                try:
                    os.environ["BRIGHTWAY2_DIR"] = str(bw_dir)
                    os.environ["BW2_DIR"] = str(bw_dir)
                    fixed_project = _fixed_project_name()
                    if fixed_project:
                        projects = [fixed_project]
                        current = fixed_project
                    else:
                        projects = _project_names()
                        current = getattr(bd.projects.current, "name", "") if bd.projects.current else ""
                    _json_response(
                        self,
                        {"projects": projects, "currentProject": current, "fixedProject": fixed_project},
                    )
                except Exception as exc:
                    _json_response(self, {"error": str(exc)}, status=500)
                return

            if route == "/api/job":
                q = parse_qs(parsed.query or "")
                job_id = (q.get("id") or [""])[0].strip()
                if not job_id:
                    _json_response(self, {"error": "Missing job id"}, status=400)
                    return
                snap = _job_snapshot(job_id)
                if snap is None:
                    _json_response(self, {"error": f"Job '{job_id}' not found"}, status=404)
                    return
                _json_response(self, snap)
                return

            _text_response(self, "Not found", status=404)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            route = parsed.path
            length = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                _json_response(self, {"error": "Invalid JSON body"}, status=400)
                return

            try:
                if route == "/api/setup_start":
                    job_id = _job_create("setup", payload)
                    t = threading.Thread(target=_run_setup_job, args=(job_id, payload, bw_dir), daemon=True)
                    t.start()
                    _json_response(self, {"jobId": job_id, "status": "queued"})
                    return

                if route == "/api/calc_start":
                    job_id = _job_create("calc", payload)
                    t = threading.Thread(target=_run_calc_job, args=(job_id, payload, bw_dir), daemon=True)
                    t.start()
                    _json_response(self, {"jobId": job_id, "status": "queued"})
                    return

                if route == "/api/setup":
                    project = _effective_project_name(payload.get("project", ""))
                    setup = _ensure_project_and_bonsai(
                        project=project,
                        create_project=bool(payload.get("createProject", False)),
                        import_bonsai=bool(payload.get("importBonsaiIfMissing", True)),
                        bw_dir=bw_dir,
                    )
                    try:
                        projects = _project_names()
                    except Exception:
                        projects = [setup.get("project", "")]
                    _json_response(self, {"projectInfo": setup, "projects": projects})
                    return

                if route == "/api/calc":
                    out = _handle_calculation(payload=payload, bw_dir=bw_dir)
                    _json_response(self, out)
                    return

                _json_response(self, {"error": "Not found"}, status=404)
            except Exception as exc:
                tb = traceback.format_exc(limit=5)
                _json_response(self, {"error": str(exc), "trace": tb}, status=500)

        def log_message(self, fmt: str, *args: Any) -> None:
            _ = (fmt, args)

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description="Run online DPP impact calculator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8790)
    parser.add_argument("--bw-dir", type=Path, default=DEFAULT_BW_DIR)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    global _JOBS_DIR
    _JOBS_DIR = Path(args.bw_dir).expanduser() / "dpp_jobs"
    _JOBS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Using Brightway dir: {Path(args.bw_dir).expanduser()}")

    handler = make_handler(args.bw_dir)

    server = None
    chosen = None
    for p in [args.port] + list(range(args.port + 1, args.port + 12)):
        try:
            server = ThreadingHTTPServer((args.host, p), handler)
            chosen = p
            break
        except OSError:
            continue

    if server is None or chosen is None:
        raise RuntimeError(f"Could not bind {args.host}:{args.port}..{args.port+11}")

    url = f"http://{args.host}:{chosen}"
    if chosen != args.port:
        print(f"Requested port {args.port} busy; using {chosen}")
    print(f"DPP impact calculator running at: {url}")

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
