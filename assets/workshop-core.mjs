export const STAGES = [
  "Raw material acquisition",
  "Manufacturing",
  "Installation/distribution/retail",
  "Use",
  "Maintenance, repair, refurbishment",
  "End-of-life",
];

export const INPUT_UNITS = ["kg", "g", "tonnes", "kWh", "MJ", "TJ", "m2", "items", "cell", "panel", "custom"];

export const ACTIVITY_TYPES = ["Database activity", "Linked DPP", "Direct emission"];

export function uid(prefix = "row") {
  return `${prefix}_${Math.random().toString(36).slice(2, 9)}_${Date.now().toString(36)}`;
}

export function toNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

export function normalizeInputUnit(unit) {
  const raw = String(unit || "").trim().toLowerCase();
  const map = {
    kg: "kg",
    kgm: "kg",
    kilogram: "kg",
    kilograms: "kg",
    g: "g",
    grm: "g",
    gram: "g",
    grams: "g",
    t: "tonnes",
    tonne: "tonnes",
    tonnes: "tonnes",
    ton: "tonnes",
    tons: "tonnes",
    kwh: "kWh",
    mj: "MJ",
    tj: "TJ",
    item: "items",
    items: "items",
    unit: "items",
    units: "items",
    piece: "items",
    pieces: "items",
    cell: "cell",
    cells: "cell",
    panel: "panel",
    panels: "panel",
    m2: "m2",
    "m²": "m2",
    sqm: "m2",
    "square metre": "m2",
    "square meter": "m2",
    "square metres": "m2",
    "square meters": "m2",
  };
  return map[raw] || unit;
}

export function convertQuantity(amount, fromUnit, referenceUnit) {
  const value = toNumber(amount);
  const unit = normalizeInputUnit(fromUnit);
  if (!value) return 0;

  if (referenceUnit === "tonnes" || referenceUnit === "tonnes (service)") {
    if (unit === "tonnes") return value;
    if (unit === "kg") return value / 1000;
    if (unit === "g") return value / 1000000;
  }

  if (referenceUnit === "TJ") {
    if (unit === "TJ") return value;
    if (unit === "MJ") return value / 1000000;
    if (unit === "kWh") return value * 0.0000036;
  }

  if (referenceUnit === "items") {
    if (unit === "items") return value;
  }

  return NaN;
}

export function factorFor(entry, referenceUnit, region) {
  if (!entry || !referenceUnit || !region) return undefined;
  return entry.factors?.[`${referenceUnit}|${region}`];
}

export function co2ToTonnes(value, unit) {
  const num = toNumber(value);
  const raw = String(unit || "").toLowerCase();
  if (!num) return 0;
  if (raw.includes("kg")) return num / 1000;
  if (raw.includes("tonne") || raw.includes("ton")) return num;
  return num;
}

function firstNumber(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n)) return n;
  }
  return undefined;
}

function linkedDppMetadata(payload) {
  const referenceFlow = payload?.functionalUnit?.referenceFlow || payload?.referenceFlow || null;
  const referenceAmount = firstNumber(
    referenceFlow?.amount,
    payload?.functionalUnit?.howMuch,
    payload?.referenceAmount,
    payload?.amount,
  );
  const referenceUnit = referenceFlow?.unit || payload?.functionalUnit?.unit || payload?.referenceUnit || payload?.unit || "";
  const referenceDescription = referenceFlow?.description || payload?.functionalUnit?.what || payload?.functionalUnit?.description || "";
  return {
    dppId: payload.dppId || payload.id || payload.product?.dppId || "uploaded-cell-dpp",
    productName: payload.product?.name || payload.productName || payload.name || "Uploaded linked DPP",
    productDescription: payload.product?.description || payload.description || "",
    productLocation: payload.product?.productionLocation || payload.product?.location || payload.location || "",
    referenceFlow,
    referenceAmount: referenceAmount ?? null,
    referenceUnit,
    referenceDescription,
    functionalUnit: payload.functionalUnit || null,
  };
}

export function extractLinkedDppImpact(payload) {
  if (!payload || typeof payload !== "object") {
    return { ok: false, error: "The uploaded file is not a JSON object." };
  }

  const candidates = [
    {
      value: payload?.results?.totals?.totalImpactTonnesCO2eq,
      unit: "tonnes CO2eq",
      source: "results.totals.totalImpactTonnesCO2eq",
    },
    {
      value: payload?.results?.totalImpactTonnesCO2eq,
      unit: "tonnes CO2eq",
      source: "results.totalImpactTonnesCO2eq",
    },
    {
      value: payload?.impact?.totalImpactTonnesCO2eq,
      unit: "tonnes CO2eq",
      source: "impact.totalImpactTonnesCO2eq",
    },
    {
      value: payload?.totalImpactTonnesCO2eq,
      unit: "tonnes CO2eq",
      source: "totalImpactTonnesCO2eq",
    },
    {
      value: payload?.impactResults?.totalImpact,
      unit: payload?.impactResults?.unit || "tonnes CO2eq",
      source: "impactResults.totalImpact",
    },
  ];

  for (const candidate of candidates) {
    const n = firstNumber(candidate.value);
    if (n !== undefined) {
      return {
        ok: true,
        impactTonnesCO2eq: co2ToTonnes(n, candidate.unit),
        impactUnit: "tonnes CO2eq",
        source: candidate.source,
        ...linkedDppMetadata(payload),
      };
    }
  }

  const rowTotal = Array.isArray(payload?.results?.rows)
    ? payload.results.rows.reduce((sum, row) => sum + co2ToTonnes(row.totalImpactTonnesCO2eq ?? row.impactTonnesCO2eq ?? row.impact, row.unit || "tonnes CO2eq"), 0)
    : 0;
  if (rowTotal) {
    return {
      ok: true,
      impactTonnesCO2eq: rowTotal,
      impactUnit: "tonnes CO2eq",
      source: "sum(results.rows)",
      ...linkedDppMetadata(payload),
    };
  }

  return { ok: false, error: "No usable total impact was found in the uploaded DPP." };
}

export function displayUnit(row) {
  return row?.unit === "custom" ? `custom: ${row.customUnit || "unit"}` : row?.unit || "";
}

export function calculationUnit(row) {
  return row?.unit === "custom" ? row.customUnit || "custom" : row?.unit || "";
}

export function calculateRows(rows, lookupByCode = new Map()) {
  const resultRows = rows.map((row) => {
    const activityType = row.activityType || (row.linkedDpp ? "Linked DPP" : "Database activity");
    const isDirectEmission = activityType === "Direct emission";
    const isLinkedActivity = activityType === "Linked DPP" || (!row.activityType && Boolean(row.linkedDpp));
    const entry = lookupByCode.get(row.footprintCode);
    const ownEnabled = !isDirectEmission && !isLinkedActivity;
    const linkedEnabled = !isDirectEmission && isLinkedActivity;
    const factor = ownEnabled ? factorFor(entry, row.referenceUnit, row.region) : undefined;
    const normalizedQuantity = ownEnabled ? convertQuantity(row.quantity, calculationUnit(row), row.referenceUnit) : NaN;
    const hasFactor = Number.isFinite(Number(factor));
    const hasOwnQuantity = Number.isFinite(normalizedQuantity);
    const ownImpactTonnesCO2eq = ownEnabled && hasFactor && hasOwnQuantity ? normalizedQuantity * Number(factor) : 0;

    const linkedBase = linkedEnabled ? toNumber(row.linkedDpp?.impactTonnesCO2eq) : 0;
    const hasLinkedMultiplier = linkedEnabled && row.linkedDpp && Object.hasOwn(row.linkedDpp, "multiplier");
    const linkedMultiplier = linkedEnabled && row.linkedDpp
      ? toNumber(hasLinkedMultiplier ? row.linkedDpp.multiplier : row.quantity)
      : 0;
    const linkedImpactTonnesCO2eq = linkedBase && linkedMultiplier ? linkedBase * linkedMultiplier : 0;
    const totalImpactTonnesCO2eq = ownImpactTonnesCO2eq + linkedImpactTonnesCO2eq;

    let status = "not calculated";
    const warnings = [];
    if (isDirectEmission) {
      if (!row.directEmissionName) warnings.push("Direct emission name is required for vocabulary mapping.");
      if (row.directEmissionName && !row.directEmissionVocabulary?.biosphere3Uuid) warnings.push("No Biosphere3 vocabulary mapping was found for this emission.");
      status = warnings.length ? "needs attention" : "mapped direct emission";
    }
    if (ownEnabled && row.footprintCode && !entry) warnings.push("Selected database entry is not available in the lookup.");
    if (ownEnabled && entry && !hasFactor) warnings.push("No impact factor is available for the selected database entry, reference unit, and region.");
    if (ownEnabled && entry && hasFactor && !hasOwnQuantity) warnings.push("Quantity unit cannot be converted to the selected database-entry reference unit.");
    if (isLinkedActivity && row.linkedDpp?.error) warnings.push(row.linkedDpp.error);
    if (ownImpactTonnesCO2eq || linkedImpactTonnesCO2eq) status = "calculated";
    else if (!isDirectEmission && warnings.length) status = "needs attention";

    return {
      rowId: row.id,
      activityType,
      componentName: row.componentName || row.directEmissionName || "Unnamed component",
      stage: row.stage || "Unspecified",
      quantity: toNumber(row.quantity),
      unit: displayUnit(row),
      footprintCode: isDirectEmission ? "" : row.footprintCode || "",
      footprintLabel: isDirectEmission ? row.directEmissionVocabulary?.biosphere3Name || "Direct emission" : entry?.label || "",
      referenceUnit: isDirectEmission ? "elementary flow" : row.referenceUnit || "",
      region: isDirectEmission ? row.sourceLocation || "" : row.region || "",
      factor: hasFactor ? Number(factor) : null,
      normalizedQuantity: hasOwnQuantity ? normalizedQuantity : null,
      directEmissionName: row.directEmissionName || "",
      directEmissionVocabulary: row.directEmissionVocabulary || null,
      ownImpactTonnesCO2eq,
      linkedImpactTonnesCO2eq,
      totalImpactTonnesCO2eq,
      status,
      warnings,
    };
  });

  const byStage = {};
  let ownTotal = 0;
  let linkedTotal = 0;
  for (const row of resultRows) {
    ownTotal += row.ownImpactTonnesCO2eq;
    linkedTotal += row.linkedImpactTonnesCO2eq;
    byStage[row.stage] = (byStage[row.stage] || 0) + row.totalImpactTonnesCO2eq;
  }

  return {
    rows: resultRows,
    totals: {
      ownImpactTonnesCO2eq: ownTotal,
      upstreamLinkedDppImpactTonnesCO2eq: linkedTotal,
      totalImpactTonnesCO2eq: ownTotal + linkedTotal,
      byStage,
    },
  };
}

export function buildExportPackage(state, calculation, lookupMetadata) {
  return {
    schemaVersion: "workshop-pv-dpp-1.0.0",
    packageType: "CERISE_DPP_WORKSHOP_EXPORT",
    exportedAt: new Date().toISOString(),
    product: state.product,
    functionalUnit: state.functionalUnit,
    lca: state.lca || null,
    inventoryRows: state.rows,
    linkedDpps: state.rows
      .filter((row) => row.linkedDpp)
      .map((row) => ({ rowId: row.id, componentName: row.componentName, ...row.linkedDpp })),
    results: {
      metric: "GWP100",
      unit: "tonnes CO2eq",
      ...calculation,
    },
    provenance: {
      app: "CERISE-DPP-Workshop",
      lookup: lookupMetadata || null,
      assumptions: [
        "Rows without a selected database entry are retained as DPP information but excluded from calculated totals.",
        "Impact factors are read from the selected database entries in the generated GWP100 lookup.",
        "Activity type decides whether a row is calculated as a database activity, a linked DPP, or retained as a mapped direct emission.",
        "Linked DPP rows use the participant quantity to scale the uploaded DPP reference flow.",
        "Direct emission rows are mapped to Biosphere3 vocabulary when an emission name and compartment match the direct-emission vocabulary asset; they are retained as elementary-flow DPP information and not included in BONSAI process/object totals.",
      ],
    },
  };
}

export function csvEscape(value) {
  const text = String(value ?? "");
  return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

export function resultsToCsv(calculation) {
  const lines = [
    [
      "Component",
      "Stage",
      "Quantity",
      "Unit",
      "Database entry code",
      "Reference unit",
      "Region / compartment",
      "Activity type",
      "Direct emission mapping UUID",
      "Own impact (tonnes CO2eq)",
      "Linked DPP impact (tonnes CO2eq)",
      "Total impact (tonnes CO2eq)",
      "Status",
      "Warnings",
    ].map(csvEscape).join(","),
  ];
  for (const row of calculation.rows) {
    lines.push([
      row.componentName,
      row.stage,
      row.quantity,
      row.unit,
      row.footprintCode,
      row.referenceUnit,
      row.region,
      row.activityType,
      row.directEmissionVocabulary?.biosphere3Uuid || "",
      row.ownImpactTonnesCO2eq,
      row.linkedImpactTonnesCO2eq,
      row.totalImpactTonnesCO2eq,
      row.status,
      row.warnings.join("; "),
    ].map(csvEscape).join(","));
  }
  return `${lines.join("\n")}\n`;
}
