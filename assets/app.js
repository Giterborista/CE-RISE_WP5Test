import {
  INPUT_UNITS,
  ACTIVITY_TYPES,
  uid,
  calculateRows,
  extractLinkedDppImpact,
  buildExportPackage,
  resultsToCsv,
} from "./workshop-core.mjs?v=20260531-guide-rounding-1";

const state = {
  product: {},
  functionalUnit: {},
  lca: {},
  rows: [],
};

let lookup = { metadata: {}, entries: [] };
let lookupByCode = new Map();
let directEmissionVocabulary = { metadata: {}, entries: [] };
let directEmissionByKey = new Map();
let calculation = { rows: [], totals: { ownImpactTonnesCO2eq: 0, upstreamLinkedDppImpactTonnesCO2eq: 0, totalImpactTonnesCO2eq: 0, byStage: {} } };
let expandedDppRows = new Set();
let treeDppPayloads = new Map();
let activeScenario = "assembly";

const BASE_LOCATION_OPTIONS = ["Global", "EU", "WE", "RER", "CN", "DE", "NL", "FR", "IT", "ES", "BE", "CH", "US", "JP", "KR", "IN"];
let locationOptions = [...BASE_LOCATION_OPTIONS];
let directEmissionByName = new Map();

const $ = (id) => document.getElementById(id);
const fmtNumber = (value) => Number(value || 0).toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const fmtKg = (tonnes) => `${fmtNumber(Number(tonnes || 0) * 1000)} kg CO2e`;
const fmtQuantity = (amount, unit = "") => {
  if (amount === undefined || amount === null || amount === "") return "-";
  const number = Number(amount);
  const value = Number.isFinite(number) ? fmtNumber(number) : String(amount);
  return `${value} ${unit || ""}`.trim();
};
const fmtInputNumber = (amount) => amount === undefined || amount === null || amount === "" ? "" : fmtNumber(amount);
const hasImpact = (tonnes) => Math.abs(Number(tonnes || 0)) > 1e-12;
const hasVisibleImpact = (tonnes) => Math.abs(Number(tonnes || 0) * 1000) >= 0.005;
const CELL_AREA_M2 = 0.0441;
const PANEL_REFERENCE_CELL_AREA_M2 = 0.935415;
const WORKSHOP_CELL_PRODUCT_NAME = "Photovoltaic cell, single-Si, 210 x 210 mm, at plant";

function setButtonBusy(button, busy, label) {
  if (!button) return;
  if (busy) {
    button.dataset.defaultLabel = button.textContent;
    button.textContent = label;
    button.disabled = true;
    button.setAttribute("aria-busy", "true");
  } else {
    button.textContent = button.dataset.defaultLabel || button.textContent;
    button.disabled = false;
    button.removeAttribute("aria-busy");
  }
}

function setLookupLoading(isLoading) {
  const meta = $("lookupMeta");
  meta.classList.toggle("loading", isLoading);
  meta.setAttribute("aria-busy", String(isLoading));
  const databaseStatus = $("statusDatabase");
  if (databaseStatus) databaseStatus.textContent = isLoading ? "Loading" : "Ready";
  ["addRowBtn", "emptyAddRowBtn", "downloadJsonBtn"].forEach((id) => {
    const button = $(id);
    if (button) button.disabled = isLoading;
  });
}

function rowsForCalculation() {
  const stage = state.product?.lifecycleStage || "Manufacturing";
  return state.rows.map((row) => ({ ...row, stage }));
}

function renderStatusStrip() {
  const rowCount = state.rows.length;
  const calculated = calculation.rows.filter((row) => row.status === "calculated").length;
  if ($("statusRows")) $("statusRows").textContent = String(rowCount);
  if ($("statusCalculated")) $("statusCalculated").textContent = String(calculated);
  if ($("statusTotal")) $("statusTotal").textContent = fmtKg(calculation.totals.totalImpactTonnesCO2eq);
}

function switchTab(tabName) {
  document.querySelectorAll(".tab-btn").forEach((button) => {
    const active = button.dataset.tab === tabName;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", String(active));
  });
  document.querySelectorAll(".panel").forEach((panel) => panel.classList.remove("active"));
  $(`${tabName}Panel`)?.classList.add("active");
}

function readSetup() {
  state.product = {
    name: $("productName").value.trim(),
    dppId: $("dppId")?.value.trim() || state.product?.dppId || "urn:dpp:workshop:pv-panel",
    reportingRole: $("actorRole").value.trim(),
    reporterName: $("reporterName").value.trim(),
    location: $("productLocation").value.trim(),
    lifecycleStage: $("productStage")?.value.trim() || "",
    referenceYear: Number($("referenceYear").value) || null,
  };
}

function hydrateSetup() {
  $("productName").value = state.product.name || "";
  $("dppId").value = state.product.dppId || "";
  $("actorRole").value = state.product.reportingRole || "";
  $("reporterName").value = state.product.reporterName || "";
  populateProductLocationSelect(state.product.location || "");
  if ($("productStage")) $("productStage").value = state.product.lifecycleStage || "Manufacturing";
  $("referenceYear").value = state.product.referenceYear || "";
}

function referenceFlowFromState() {
  const ref = state.functionalUnit?.referenceFlow || {};
  return {
    amount: Number(ref.amount ?? state.functionalUnit?.howMuch ?? 1) || 1,
    unit: ref.unit || state.functionalUnit?.unit || "panel",
    description: ref.description || "One assembled photovoltaic panel for workshop calculation. Typical module built from single-Si 210 x 210 mm cells, front glass, aluminium frame, wiring, polymer layers, and assembly materials.",
  };
}

function functionalUnitStatement() {
  const ref = referenceFlowFromState();
  return `Provide one photovoltaic panel; reference flow: ${ref.amount} ${ref.unit}`;
}

function rowUnitForCalculation(row) {
  return row?.unit === "custom" ? row.customUnit || "custom" : row?.unit || "";
}

function rowUnitForDisplay(row) {
  return row?.unit === "custom" ? `custom: ${row.customUnit || "unit"}` : row?.unit || "";
}

function directEmissionKey(name, compartment) {
  return `${String(name || "").trim().toLowerCase()}|${String(compartment || "").trim().toLowerCase()}`;
}

function preferredEmissionMapping(entries = []) {
  return entries.find((entry) => String(entry.compartment || "").includes("high population density"))
    || entries.find((entry) => String(entry.compartment || "").startsWith("air"))
    || entries[0]
    || null;
}

function findDirectEmissionVocabulary(name) {
  return preferredEmissionMapping(directEmissionByName.get(String(name || "").trim().toLowerCase()) || []);
}

function updateDirectEmissionMapping(row) {
  if (!row) return;
  const mapping = findDirectEmissionVocabulary(row.directEmissionName);
  row.directEmissionVocabulary = mapping ? {
    ontology: mapping.ontology || "biosphere3",
    source: mapping.source || directEmissionVocabulary.metadata?.source || "direct_emission_vocabulary.json",
    sourceDescription: mapping.description,
    sourceCompartment: mapping.compartment,
    biosphere3Name: mapping.biosphere3Name,
    biosphere3Compartment: mapping.biosphere3Compartment,
    biosphere3Uuid: mapping.biosphere3Uuid,
    mappingStatus: "mapped",
  } : null;
}

function directEmissionMappingText(row) {
  const mapping = row?.directEmissionVocabulary;
  if (!row?.directEmissionName) return "Select an emission name to map.";
  if (!mapping) return "No Biosphere3 mapping found for this emission.";
  return `${mapping.biosphere3Name} | ${mapping.biosphere3Compartment} | ${mapping.biosphere3Uuid}`;
}

function buildLocationOptions() {
  const values = new Set(BASE_LOCATION_OPTIONS);
  for (const entry of lookup.entries || []) {
    for (const regions of Object.values(entry.regionsByUnit || {})) {
      for (const region of regions || []) if (region) values.add(region);
    }
  }
  return [...values].sort((a, b) => a.localeCompare(b));
}

function populateProductLocationSelect(selected = "") {
  const select = $("productLocation");
  if (!select) return;
  const value = selected || select.value || "";
  const options = locationOptions.includes(value) ? locationOptions : [value, ...locationOptions];
  const cleanOptions = ["", ...options.filter(Boolean)];
  select.innerHTML = cleanOptions.map((item) => `<option value="${escapeHtml(item)}"${item === value ? " selected" : ""}>${escapeHtml(item)}</option>`).join("");
}

function readLca() {
  const fallbackRef = referenceFlowFromState();
  const referenceAmount = Number($("dppReferenceAmount")?.value) || null;
  const referenceUnit = $("dppReferenceUnit")?.value.trim() || "";
  const referenceDescription = $("dppReferenceDescription")?.value.trim() || (referenceAmount && referenceUnit ? `${referenceAmount} ${referenceUnit}` : "");
  const statement = referenceAmount && referenceUnit ? `Provide one photovoltaic panel; reference flow: ${referenceAmount} ${referenceUnit}` : "";
  state.functionalUnit = {
    what: statement,
    howMuch: referenceAmount || fallbackRef.amount,
    unit: referenceUnit,
    referenceFlow: {
      amount: referenceAmount || fallbackRef.amount,
      unit: referenceUnit,
      description: referenceDescription.trim(),
    },
  };
  state.lca = {
    functionalUnitStatement: statement,
    referenceFlow: state.functionalUnit.referenceFlow,
  };
}

function hydrateLca() {
  const ref = referenceFlowFromState();
  if ($("dppReferenceAmount")) $("dppReferenceAmount").value = state.functionalUnit?.referenceFlow ? ref.amount || "" : "";
  if ($("dppReferenceUnit")) $("dppReferenceUnit").value = state.functionalUnit?.referenceFlow ? ref.unit || "" : "";
  if ($("dppReferenceDescription")) $("dppReferenceDescription").value = state.functionalUnit?.referenceFlow ? ref.description || "" : "";
}

function normalizeImportedRows(rows = []) {
  return rows.map((row) => ({
    activityType: row.activityType || (row.directEmissionName ? "Direct emission" : row.linkedDpp?.ok ? "Linked DPP" : "Database activity"),
    ...row,
  }));
}

async function loadDppPackage(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) throw new Error(`DPP package load failed: ${response.status}`);
  return response.json();
}

async function loadEolScenario() {
  const payload = await loadDppPackage("data/pv_panel_eol_scenario_dpp.json");
  if (!Array.isArray(payload.inventoryRows)) throw new Error("The EoL DPP scenario does not contain inventory rows.");
  state.product = payload.product || {};
  state.functionalUnit = payload.functionalUnit || {};
  state.lca = payload.lca || {};
  if (payload.functionalUnit?.referenceFlow) state.lca.referenceFlow = payload.functionalUnit.referenceFlow;
  state.rows = normalizeImportedRows(payload.inventoryRows);
  activeScenario = "eol";
  expandedDppRows = new Set(["eol_input_panel_dpp"]);
  refreshDirectEmissionMappings();
  hydrateSetup();
  hydrateLca();
  switchTab("setup");
  renderAll();
}

function optionHtml(values, selected = "") {
  return values.map((value) => `<option value="${escapeHtml(value)}"${value === selected ? " selected" : ""}>${escapeHtml(value)}</option>`).join("");
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
}

function factorDisplay(entry) {
  if (!entry) return "";
  return `${entry.label} [${entry.code}]`;
}

function entryFromDisplay(value) {
  const text = String(value || "").trim();
  if (!text) return null;
  const bracket = text.match(/\[([^\]]+)\]\s*$/);
  const code = bracket ? bracket[1] : text;
  return lookupByCode.get(code) || lookup.entries.find((entry) => entry.label.toLowerCase() === text.toLowerCase()) || null;
}

function linkedReferenceText(linked) {
  if (!linked) return "";
  if (!linked.ok) return linked.error || "Linked DPP could not be read.";
  const amount = linked.referenceAmount ?? 1;
  const unit = linked.referenceUnit || "reference flow";
  const description = linked.referenceDescription || linked.referenceFlow?.description || "";
  return `${fmtQuantity(amount, unit)}${description ? ` - ${description}` : ""}`;
}

function linkedSummaryText(linked) {
  if (!linked) return "No linked DPP";
  if (!linked.ok) return linked.error || "Linked DPP could not be read.";
  return `${linked.productName || linked.dppId} | ${linkedReferenceText(linked)}`;
}

function linkedCardHtml(linked) {
  if (!linked) {
    return `<div class="linked-card linked-card-empty"><strong>No file selected</strong><span>Upload a component passport JSON.</span></div>`;
  }
  if (!linked.ok) {
    return `<div class="linked-card linked-card-error"><strong>Linked DPP issue</strong><span>${escapeHtml(linked.error || "The uploaded DPP could not be read.")}</span></div>`;
  }
  const location = linked.productLocation ? `Source location: ${linked.productLocation}` : "Source location not provided in DPP";
  return `<div class="linked-card linked-card-ready">
    <strong>${escapeHtml(linked.productName || linked.dppId || "Linked DPP")}</strong>
    <span>${escapeHtml(linkedReferenceText(linked))}</span>
    <span>${escapeHtml(location)}</span>
  </div>`;
}

function isWorkshopCellDpp(linked) {
  return Boolean(linked?.ok && String(linked.productName || "").trim() === WORKSHOP_CELL_PRODUCT_NAME);
}

function textareaRows(value, min = 3, max = 9) {
  const lines = String(value || "").split(/\n/).reduce((sum, line) => sum + Math.max(1, Math.ceil(line.length / 78)), 0);
  return Math.min(max, Math.max(min, lines));
}

function textInputHtml(row, field, value, placeholder, locked = false) {
  return `<input data-field="${escapeHtml(field)}" value="${escapeHtml(value)}" placeholder="${escapeHtml(placeholder)}"${locked ? " readonly aria-readonly=\"true\"" : ""} />`;
}

function textareaHtml(row, field, value, placeholder, locked = false) {
  return `<textarea data-field="${escapeHtml(field)}" placeholder="${escapeHtml(placeholder)}" rows="${textareaRows(value)}"${locked ? " readonly aria-readonly=\"true\"" : ""}>${escapeHtml(value)}</textarea>`;
}

function sameUnit(a, b) {
  return String(a || "").trim().toLowerCase() === String(b || "").trim().toLowerCase();
}

function syncLinkedMultiplierFromQuantity(row) {
  const linked = row.linkedDpp;
  if (!linked?.ok) return;
  const quantity = Number(row.quantity);
  const referenceAmount = Number(linked.referenceAmount || 1);
  if (!Number.isFinite(quantity)) return;
  const unit = rowUnitForCalculation(row);
  if (row.unit !== "custom" && sameUnit(unit, linked.referenceUnit) && Number.isFinite(referenceAmount) && referenceAmount) {
    linked.multiplier = quantity / referenceAmount;
    linked.quantitySync = "quantity-unit";
  } else {
    linked.multiplier = quantity;
    linked.quantitySync = "custom-multiplier";
  }
}

function applyLinkedDppToRow(row, linked) {
  row.linkedDpp = linked;
  if (!linked.ok) return;
  row.activityType = "Linked DPP";
  row.componentName = linked.productName || row.componentName;
  const details = [
    linked.productDescription,
    linked.dppId ? `Linked DPP: ${linked.dppId}` : "",
    linkedReferenceText(linked) ? `Reference flow: ${linkedReferenceText(linked)}` : "",
    linked.productLocation ? `Source location from DPP: ${linked.productLocation}` : "",
  ].filter(Boolean);
  row.description = details.join("\n");
  row.quantity = linked.referenceAmount ?? 1;
  row.unit = linked.referenceUnit || row.unit || "reference flow";
  row.sourceLocation = linked.productLocation || row.sourceLocation;
  row.footprintCode = "";
  row.factorSearchText = "";
  row.referenceUnit = "";
  row.region = "";
  row.notes = [row.notes, `Imported from linked DPP file: ${linked.fileName || "uploaded JSON"}. Impact source: ${linked.source}.`].filter(Boolean).join("\n");
  syncLinkedMultiplierFromQuantity(row);
}

let activeEntryRowId = null;
let entryMenu = null;

function keepFloatingMenuScrollInside(menu) {
  menu.addEventListener("wheel", (event) => event.stopPropagation(), { passive: true });
  menu.addEventListener("touchmove", (event) => event.stopPropagation(), { passive: true });
  menu.addEventListener("scroll", (event) => event.stopPropagation(), true);
}

function ensureEntryMenu() {
  if (entryMenu) return entryMenu;
  entryMenu = document.createElement("div");
  entryMenu.className = "entry-menu hidden";
  entryMenu.innerHTML = `<input class="select-search entry-search" type="text" placeholder="Search database entries" /><div class="entry-options"></div>`;
  document.body.appendChild(entryMenu);
  keepFloatingMenuScrollInside(entryMenu);
  entryMenu.querySelector(".entry-search").addEventListener("input", (event) => updateEntryOptions(event.target.value));
  entryMenu.querySelector(".entry-search").addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    const first = entryMenu.querySelector(".entry-option");
    if (first) {
      event.preventDefault();
      first.click();
    }
  });
  return entryMenu;
}

function searchEntries(query) {
  const q = String(query || "").trim().toLowerCase();
  if (!q) {
    return [...lookup.entries]
      .sort((a, b) => String(a.label || "").localeCompare(String(b.label || "")))
      .slice(0, 80);
  }
  const tokens = q.split(/\s+/).filter(Boolean);
  return lookup.entries
    .map((entry) => {
      const label = entry.label || "";
      const haystack = `${label} ${entry.description || ""} ${entry.code}`.toLowerCase();
      if (!tokens.every((token) => haystack.includes(token))) return null;
      const starts = label.toLowerCase().startsWith(q) ? 0 : 1;
      return { entry, score: starts + Math.min(label.length / 1000, 1) };
    })
    .filter(Boolean)
    .sort((a, b) => a.score - b.score || a.entry.label.localeCompare(b.entry.label))
    .slice(0, 80)
    .map((item) => item.entry);
}

function closeEntryMenu() {
  activeEntryRowId = null;
  if (entryMenu) entryMenu.classList.add("hidden");
}

function positionFloatingMenu(menu, anchor, minWidth = 260) {
  const rect = anchor.getBoundingClientRect();
  const gutter = 12;
  const width = Math.min(Math.max(rect.width, minWidth), window.innerWidth - gutter * 2);
  const left = Math.min(Math.max(gutter, rect.left), Math.max(gutter, window.innerWidth - width - gutter));
  const spaceBelow = window.innerHeight - rect.bottom - gutter;
  const maxHeight = Math.min(340, Math.max(180, Math.max(spaceBelow, rect.top - gutter)));
  const shouldOpenAbove = spaceBelow < 220 && rect.top > spaceBelow;
  const top = shouldOpenAbove ? Math.max(gutter, rect.top - maxHeight - 6) : Math.min(rect.bottom + 6, window.innerHeight - maxHeight - gutter);
  menu.style.left = `${left}px`;
  menu.style.top = `${top}px`;
  menu.style.width = `${width}px`;
  menu.style.maxHeight = `${maxHeight}px`;
  menu.style.setProperty("--floating-menu-max", `${maxHeight}px`);
}

function entryOptionHtml(entry) {
  const units = (entry.units || []).join(", ");
  const regionCount = Object.values(entry.regionsByUnit || {}).reduce((max, regions) => Math.max(max, regions.length), 0);
  return `<button type="button" class="entry-option" data-entry-code="${escapeHtml(entry.code)}">
    <span class="entry-label">${escapeHtml(entry.label)}</span>
    <span class="entry-meta">${escapeHtml(entry.code)} · ${escapeHtml(units || "no physical unit")} · ${regionCount} regions</span>
  </button>`;
}

function updateEntryOptions(query) {
  if (!entryMenu || !activeEntryRowId) return;
  const matches = searchEntries(query);
  const container = entryMenu.querySelector(".entry-options");
  if (!matches.length) {
    container.innerHTML = `<div class="entry-empty">No database entries found.</div>`;
    return;
  }
  container.innerHTML = matches.map(entryOptionHtml).join("");
}

function openEntryMenu(input, row) {
  const menu = ensureEntryMenu();
  activeEntryRowId = row.id;
  positionFloatingMenu(menu, input, 420);
  menu.classList.remove("hidden");
  const search = menu.querySelector(".entry-search");
  const initial = row.factorSearchText && !lookupByCode.get(row.footprintCode) ? row.factorSearchText : "";
  search.value = initial;
  updateEntryOptions(initial);
  requestAnimationFrame(() => search.focus());
}

function selectEntryForRow(row, entry) {
  row.footprintCode = entry.code;
  row.factorSearchText = factorDisplay(entry);
  row.referenceUnit = entry.units?.[0] || "";
  const regions = entry.regionsByUnit?.[row.referenceUnit] || [];
  row.region = regions.includes("WE") ? "WE" : regions.includes("Global") ? "Global" : regions[0] || "";
  closeEntryMenu();
  renderAll();
}


let activeSelect = null;
let selectMenu = null;

function ensureSelectMenu() {
  if (selectMenu) return selectMenu;
  selectMenu = document.createElement("div");
  selectMenu.className = "select-menu hidden";
  selectMenu.innerHTML = `<input class="select-search" type="text" placeholder="Search options" /><div class="select-options"></div>`;
  document.body.appendChild(selectMenu);
  keepFloatingMenuScrollInside(selectMenu);
  selectMenu.querySelector(".select-search").addEventListener("input", (event) => updateSelectOptions(event.target.value));
  selectMenu.addEventListener("click", (event) => {
    const option = event.target.closest(".select-option");
    if (!option || !activeSelect) return;
    const row = state.rows.find((item) => item.id === activeSelect.rowId);
    if (!row) return;
    applySelectValue(row, activeSelect.field, option.dataset.value || "");
  });
  return selectMenu;
}

function closeSelectMenu() {
  activeSelect = null;
  if (selectMenu) selectMenu.classList.add("hidden");
}

function optionListFor(row, field) {
  if (field === "activityType") return ACTIVITY_TYPES;
  if (field === "unit") return INPUT_UNITS;
  if (field === "sourceLocation") return locationOptions;
  if (field === "directEmissionName") return [...directEmissionByName.keys()].sort((a, b) => a.localeCompare(b)).map((name) => directEmissionByName.get(name)?.[0]?.description || name);
  const entry = lookupByCode.get(row.footprintCode);
  if (field === "referenceUnit") return entry?.units || [];
  if (field === "region") return entry?.regionsByUnit?.[row.referenceUnit] || [];
  return [];
}

function selectText(value) {
  return value ? String(value) : "-";
}

function selectButton(row, field, options) {
  const value = row[field] || "";
  const disabled = options.length === 0;
  return `<button class="select-button${disabled ? " disabled" : ""}" data-action="customSelect" data-field="${escapeHtml(field)}" type="button"${disabled ? " disabled" : ""}>${escapeHtml(selectText(value))}</button>`;
}

function openSelectMenu(button, row, field) {
  const options = optionListFor(row, field);
  if (!options.length) return;
  closeEntryMenu();
  const menu = ensureSelectMenu();
  activeSelect = { rowId: row.id, field, options };
  positionFloatingMenu(menu, button, field === "activityType" ? 300 : 220);
  menu.classList.remove("hidden");
  const search = menu.querySelector(".select-search");
  search.value = "";
  updateSelectOptions("");
  requestAnimationFrame(() => search.focus());
}

function updateSelectOptions(query) {
  if (!selectMenu || !activeSelect) return;
  const q = String(query || "").trim().toLowerCase();
  const filtered = activeSelect.options.filter((value) => String(value).toLowerCase().includes(q)).slice(0, 80);
  const container = selectMenu.querySelector(".select-options");
  if (!filtered.length) {
    container.innerHTML = `<div class="entry-empty">No options found.</div>`;
    return;
  }
  container.innerHTML = filtered.map((value) => `<button type="button" class="select-option" data-value="${escapeHtml(value)}">${escapeHtml(value)}</button>`).join("");
}

function applySelectValue(row, field, value) {
  row[field] = value;
  if (field === "activityType") {
    if (value === "Direct emission") updateDirectEmissionMapping(row);
  }
  if (field === "directEmissionName") {
    updateDirectEmissionMapping(row);
    if (!row.componentName) row.componentName = row.directEmissionName;
  }
  if (field === "unit" && value !== "custom") row.customUnit = "";
  if (field === "unit") syncLinkedMultiplierFromQuantity(row);
  if (field === "referenceUnit") {
    const entry = lookupByCode.get(row.footprintCode);
    const regions = entry?.regionsByUnit?.[row.referenceUnit] || [];
    row.region = regions.includes(row.region) ? row.region : regions.includes("WE") ? "WE" : regions.includes("Global") ? "Global" : regions[0] || "";
  }
  closeSelectMenu();
  renderAll();
}

function createRow() {
  return {
    id: uid("component"),
    activityType: "Database activity",
    componentName: "",
    description: "",
    stage: "",
    quantity: "",
    unit: "kg",
    customUnit: "",
    sourceLocation: state.product?.location || "",
    directEmissionName: "",
    directEmissionVocabulary: null,
    footprintCode: "",
    referenceUnit: "",
    region: "",
    notes: "",
    evidence: "",
    linkedDpp: null,
  };
}

function addBlankRow() {
  state.rows.push(createRow());
  switchTab("inventory");
  renderAll();
}

function updateRow(id, patch) {
  const row = state.rows.find((item) => item.id === id);
  if (!row) return;
  Object.assign(row, patch);
  renderAll();
}

function removeRow(id) {
  state.rows = state.rows.filter((row) => row.id !== id);
  renderAll();
}

function renderInventory() {
  const body = $("inventoryBody");
  $("inventoryNotice").classList.toggle("hidden", state.rows.length > 0);
  body.innerHTML = state.rows.map((row, index) => {
    const entry = lookupByCode.get(row.footprintCode);
    const regions = entry?.regionsByUnit?.[row.referenceUnit] || [];
    const result = calculation.rows.find((item) => item.rowId === row.id);
    const statusText = result?.status === "not calculated" ? "draft" : result?.status || "draft";
    const statusClass = result?.status === "calculated" ? "status-calculated" : result?.status === "needs attention" ? "status-warn" : result?.status === "mapped direct emission" ? "status-mapped" : "status-not";
    const linked = row.linkedDpp;
    const activityType = row.activityType || (linked?.ok ? "Linked DPP" : "Database activity");
    const isDirectEmission = activityType === "Direct emission";
    const isLinkedMode = activityType === "Linked DPP";
    const isLinkedOk = Boolean(linked?.ok);
    const lockLinkedCell = isWorkshopCellDpp(linked);
    const unitHint = isDirectEmission ? "Elementary-flow quantity. Mapping is shown below; LCIA is not added to BONSAI totals." : "";
    const customUnitInput = row.unit === "custom"
      ? `<input class="custom-unit-input" data-field="customUnit" value="${escapeHtml(row.customUnit || "")}" placeholder="Specify custom unit" />`
      : "";
    const directPanel = `<section class="row-fields calculation-fields direct-emission-panel" aria-label="Direct emission mapping">
          <div class="notice linked-impact-note">This row is an elementary-flow direct emission. It is mapped to Biosphere3 vocabulary and exported in the DPP, but not added to the BONSAI database-entry total.</div>
          <div class="field database-field"><label>Emission name</label>${selectButton(row, "directEmissionName", optionListFor(row, "directEmissionName"))}</div>
          <div class="field notes-field"><label>Mapped ontology entry</label><div class="readonly-field mapping-readout">${escapeHtml(directEmissionMappingText(row))}</div></div>
          <div class="field notes-field"><label>Notes / evidence</label><textarea data-field="notes" placeholder="Evidence, assumptions, notes">${escapeHtml(row.notes)}</textarea></div>
        </section>`;
    const linkedPanel = `<section class="row-fields calculation-fields linked-calculation-summary" aria-label="Linked DPP source">
          <div class="field database-field"><label>Linked DPP file</label>${isLinkedOk ? `<div class="upload-complete">DPP correctly uploaded</div>` : `<button class="upload-field-btn" data-action="uploadDpp" type="button">Upload DPP JSON</button>`}<input class="hidden" data-field="linkedDppFile" type="file" accept="application/json,.json" />${linkedCardHtml(linked)}</div>
          <div class="field notes-field"><label>Notes / evidence</label>${textareaHtml(row, "notes", row.notes, "Evidence, assumptions, notes", lockLinkedCell)}</div>
        </section>`;
    const databaseMeta = entry ? `<div class="field"><label>Reference unit</label><div class="readonly-field">${escapeHtml(row.referenceUnit || "From database entry")}</div></div>
          <div class="field"><label>Database location</label>${selectButton(row, "region", regions)}</div>` : "";
    const databasePanel = `<section class="row-fields calculation-fields" aria-label="Calculation fields">
          <div class="field database-field"><label>Database entry</label><input class="database-entry-input" data-field="factorSearch" autocomplete="off" value="${escapeHtml(entry ? factorDisplay(entry) : row.factorSearchText || "")}" placeholder="Search database entry" /></div>
          ${databaseMeta}
          <div class="field notes-field"><label>Notes / evidence</label><textarea data-field="notes" placeholder="Evidence, assumptions, notes">${escapeHtml(row.notes)}</textarea></div>
        </section>`;
    const calculationPanel = isDirectEmission ? directPanel : isLinkedMode ? linkedPanel : databasePanel;
    return `<article class="inventory-card${isLinkedOk ? " linked-row" : ""}${isDirectEmission ? " direct-emission-row" : ""}" data-row="${row.id}">
      <header class="inventory-card-header">
        <div class="component-name-block">
          <div class="row-kicker">Component ${index + 1}</div>
          <strong>${escapeHtml(isDirectEmission ? row.directEmissionName || "Direct emission" : row.componentName || "Unnamed component")}</strong>
        </div>
        <div class="row-actions">
          <span class="status-pill ${statusClass}">${escapeHtml(statusText)}</span>
          <button class="btn small danger" data-action="remove" type="button">Remove</button>
        </div>
      </header>

      <div class="inventory-card-grid">
        <section class="row-fields primary-fields" aria-label="Component fields">
          <div class="field"><label>Activity type</label>${selectButton(row, "activityType", ACTIVITY_TYPES)}</div>
          ${isDirectEmission ? "" : `<div class="field"><label>Component name</label>${textInputHtml(row, "componentName", row.componentName, "e.g. glass front sheet", lockLinkedCell)}</div>`}
          <div class="field notes-field"><label>Description</label>${textareaHtml(row, "description", row.description, "Free text", lockLinkedCell)}</div>
          <div class="field quantity-field"><label>Quantity</label><input data-field="quantity" type="number" step="any" value="${escapeHtml(fmtInputNumber(row.quantity))}" /><span class="field-hint">${unitHint}</span></div>
          <div class="field"><label>Unit</label>${lockLinkedCell ? `<div class="readonly-field">${escapeHtml(rowUnitForDisplay(row) || "-")}</div>` : `${selectButton(row, "unit", INPUT_UNITS)}${customUnitInput}`}</div>
          ${isLinkedMode || isDirectEmission ? `<div class="field"><label>Source / location</label>${lockLinkedCell ? `<div class="readonly-field">${escapeHtml(row.sourceLocation || "-")}</div>` : selectButton(row, "sourceLocation", optionListFor(row, "sourceLocation"))}</div>` : ""}
        </section>

        ${calculationPanel}
      </div>
    </article>`;
  }).join("");
}

function chainSourceLabel(result, sourceRow, entry) {
  if (result.activityType === "Linked DPP") return sourceRow.linkedDpp?.productName || sourceRow.linkedDpp?.dppId || "Linked DPP";
  if (result.activityType === "Direct emission") return sourceRow.directEmissionVocabulary?.biosphere3Name || sourceRow.directEmissionName || "Direct emission";
  return entry?.label || sourceRow.footprintCode || "Database entry not selected";
}

function chainSourceMeta(result, sourceRow, entry) {
  if (result.activityType === "Linked DPP") return `Reference: ${linkedReferenceText(sourceRow.linkedDpp)} | impact used ${fmtKg(result.linkedImpactTonnesCO2eq)}`;
  if (result.activityType === "Direct emission") return sourceRow.directEmissionVocabulary?.biosphere3Uuid ? `Mapped: ${sourceRow.directEmissionVocabulary.biosphere3Uuid}` : "No mapped elementary flow yet";
  return entry ? `Reference unit: ${sourceRow.referenceUnit || "-"} | region: ${sourceRow.region || "-"}` : "Select a database entry to calculate this row";
}

function chainActivityDetail(result, sourceRow, entry) {
  if (result.activityType === "Linked DPP") return linkedReferenceText(sourceRow.linkedDpp) || "Uploaded DPP reference flow";
  if (result.activityType === "Direct emission") return sourceRow.directEmissionVocabulary?.biosphere3Uuid || "Mapped elementary flow pending";
  return entry ? `${entry.label || sourceRow.footprintCode} [${sourceRow.footprintCode || entry.code}]` : "Database entry not selected";
}

function nestedDppEntries(linked) {
  const payload = linked?.rawDpp || {};
  const resultRows = Array.isArray(payload?.results?.rows) ? payload.results.rows : [];
  const inventoryRows = Array.isArray(payload?.inventoryRows) ? payload.inventoryRows : [];
  const rows = inventoryRows.length ? inventoryRows.map((row, index) => ({ source: row, result: resultRows[index] || {} })) : resultRows.map((row) => ({ source: row, result: row }));
  return rows
    .map(({ source, result }, index) => ({
      id: `nested-${index}`,
      label: rowLabel(source, result),
      type: source.activityType || source.type || result.type || "DPP entry",
      meta: rowQuantityText(source, result),
      status: rowStatusText(source, result),
      impact: rowImpactValue(source, result),
      database: rowDatabaseText(source, result),
    }))
    .filter((entry) => entry.type !== "direct emission")
    .filter((entry) => hasVisibleImpact(entry.impact));
}

function nestedDppEmissions(linked) {
  const payload = linked?.rawDpp || {};
  const inventoryRows = Array.isArray(payload?.inventoryRows) ? payload.inventoryRows : [];
  const resultRows = Array.isArray(payload?.results?.rows) ? payload.results.rows : [];
  const rows = inventoryRows.length ? inventoryRows.map((row, index) => ({ source: row, result: resultRows[index] || {} })) : resultRows.map((row) => ({ source: row, result: row }));
  return rows
    .filter(({ source, result }) => (source.type || result.type || source.activityType || "").toLowerCase() === "direct emission")
    .map(({ source, result }, index) => ({
      id: `emission-${index}`,
      label: rowLabel(source, result),
      type: "Direct emission",
      meta: [rowQuantityText(source, result), source.directEmissionVocabulary?.biosphere3Uuid || result.directEmissionVocabulary?.biosphere3Uuid || ""].filter(Boolean).join(" | "),
      status: rowStatusText(source, result),
    }))
    .filter((entry) => entry.label !== "Unnamed entry" || entry.meta);
}

function treeImpactClass(impact) {
  if (impact < 0) return "tree-credit";
  if (impact > 0) return "tree-burden";
  return "tree-neutral";
}

function treeActivityClass(activityType = "") {
  if (activityType === "Linked DPP") return "tree-linked";
  if (activityType === "Direct emission") return "tree-direct";
  return "tree-database";
}

function treeNodeHtml(node, childrenHtml = "") {
  const openButton = node.rowId ? `<button class="tree-open" data-action="viewDpp" data-row-id="${escapeHtml(node.rowId)}" type="button">Open DPP</button>` : "";
  const openPayloadButton = node.payloadKey ? `<button class="tree-open" data-action="viewDppPayload" data-payload-key="${escapeHtml(node.payloadKey)}" type="button">Open DPP</button>` : "";
  const expandButton = node.expandRowId ? `<button class="tree-open" data-action="toggleDppTree" data-row-id="${escapeHtml(node.expandRowId)}" type="button" aria-expanded="${node.expanded ? "true" : "false"}">${node.expanded ? "Collapse" : "Expand DPP"}</button>` : "";
  return `<li>
    <div class="tree-node-card ${escapeHtml(node.kind || "")} ${escapeHtml(treeImpactClass(node.impact || 0))} ${escapeHtml(treeActivityClass(node.activityType))}">
      <span class="tree-node-dot" aria-hidden="true"></span>
      <div class="tree-node-text">
        <span>${escapeHtml(node.type || "Entry")}</span>
        <strong>${escapeHtml(node.label || "Unnamed entry")}</strong>
        <small>${escapeHtml(node.meta || "")}</small>
      </div>
      <div class="tree-node-impact">
        ${node.impactLabel ? `<strong>${escapeHtml(node.impactLabel)}</strong>` : hasImpact(node.impact) ? `<strong>${escapeHtml(fmtKg(node.impact || 0))}</strong>` : ""}
        ${expandButton}
        ${openButton}
        ${openPayloadButton}
      </div>
    </div>
    ${childrenHtml}
  </li>`;
}

function payloadTreeRows(payload) {
  const resultRows = Array.isArray(payload?.results?.rows) ? payload.results.rows : [];
  const inventoryRows = Array.isArray(payload?.inventoryRows) ? payload.inventoryRows : [];
  return inventoryRows.length ? inventoryRows.map((row, index) => ({ source: row, result: resultRows[index] || {} })) : resultRows.map((row, index) => ({ source: row, result: row, index }));
}

function linkedPayloadFromRow(row) {
  return row?.linkedDpp?.ok && row.linkedDpp.rawDpp ? row.linkedDpp.rawDpp : null;
}

function payloadTotalImpact(payload) {
  return payload?.results?.totals?.totalImpactTonnesCO2eq ?? payload?.results?.totalImpactTonnesCO2eq ?? payload?.totalImpactTonnesCO2eq ?? 0;
}

function payloadReferenceText(payload, linked) {
  const ref = payload?.functionalUnit?.referenceFlow || linked?.referenceFlow || {};
  const amount = ref.amount ?? payload?.functionalUnit?.howMuch ?? linked?.referenceAmount ?? 1;
  const unit = ref.unit || payload?.functionalUnit?.unit || linked?.referenceUnit || "reference flow";
  const description = ref.description || payload?.functionalUnit?.what || linked?.referenceDescription || "";
  return `${fmtQuantity(amount, unit)}${description ? ` - ${description}` : ""}`;
}

function renderDppPayloadTree(payload, keyPrefix, depth = 0) {
  if (!payload || depth > 8) return "";
  const rows = payloadTreeRows(payload);
  const impactNodes = rows.filter(({ source, result }) => {
    const type = (source.activityType || source.type || result.type || "").toLowerCase();
    return type !== "direct emission" && hasVisibleImpact(rowImpactValue(source, result));
  });
  const emissionNodes = rows.filter(({ source, result }) => {
    const type = (source.activityType || source.type || result.type || "").toLowerCase();
    return type === "direct emission";
  });
  const impactHtml = impactNodes.length ? `<li class="tree-group-label"><span>Impact entries inside linked DPP</span></li>${impactNodes.map(({ source, result }, index) => {
    const linkedPayload = linkedPayloadFromRow(source);
    const nodeKey = `${keyPrefix}.${source.id || index}`;
    const isLinked = Boolean(linkedPayload);
    const expanded = isLinked && expandedDppRows.has(nodeKey);
    if (linkedPayload) treeDppPayloads.set(nodeKey, { payload: linkedPayload, linked: source.linkedDpp });
    const childHtml = linkedPayload && expanded ? renderDppPayloadTree(linkedPayload, nodeKey, depth + 1) : "";
    return treeNodeHtml({
      kind: `nested${isLinked ? " linked-nested" : ""}`,
      type: isLinked ? "Linked DPP" : source.activityType || source.type || result.type || "DPP entry",
      label: isLinked ? source.linkedDpp.productName || source.componentName || source.linkedDpp.dppId || "Linked DPP" : rowLabel(source, result),
      meta: isLinked ? payloadReferenceText(linkedPayload, source.linkedDpp) : [rowQuantityText(source, result), rowDatabaseText(source, result), rowStatusText(source, result)].filter((item) => item && item !== "-").join(" | "),
      impact: rowImpactValue(source, result),
      activityType: isLinked ? "Linked DPP" : source.activityType || source.type || result.type,
      expandRowId: isLinked ? nodeKey : "",
      payloadKey: isLinked ? nodeKey : "",
      expanded,
    }, childHtml);
  }).join("")}` : `<li class="tree-group-label"><span>No non-zero impact entries inside this linked DPP.</span></li>`;
  const emissionsHtml = emissionNodes.length ? `<li class="tree-group-label emissions"><span>Direct emissions reported separately</span></li>${emissionNodes.map(({ source, result }) => treeNodeHtml({
    kind: "nested emission-node",
    type: "Direct emission",
    label: rowLabel(source, result),
    meta: [rowQuantityText(source, result), source.directEmissionVocabulary?.biosphere3Uuid || result.directEmissionVocabulary?.biosphere3Uuid || "", rowStatusText(source, result)].filter((item) => item && item !== "-").join(" | "),
    impactLabel: "reported separately",
    activityType: "Direct emission",
  })).join("")}` : "";
  return `<ul class="tree-nested-expanded">${impactHtml}${emissionsHtml}</ul>`;
}

function renderLcaPanel() {
  const tree = $("valueChainTree");
  if (!tree) return;
  treeDppPayloads = new Map();
  const productName = state.product.name || "Product";
  const ref = referenceFlowFromState();
  const impactRows = calculation.rows.filter((result) => result.activityType !== "Direct emission" && hasVisibleImpact(result.totalImpactTonnesCO2eq));
  const emissionRows = calculation.rows.filter((result) => result.activityType === "Direct emission");
  const rows = impactRows.map((result) => {
    const sourceRow = state.rows.find((row) => row.id === result.rowId) || {};
    const linked = sourceRow.linkedDpp;
    const entry = lookupByCode.get(sourceRow.footprintCode);
    const expanded = linked?.ok && expandedDppRows.has(sourceRow.id);
    if (linked?.ok && linked.rawDpp) treeDppPayloads.set(sourceRow.id, { payload: linked.rawDpp, linked });
    const nestedHtml = linked?.ok && linked.rawDpp && expanded ? renderDppPayloadTree(linked.rawDpp, sourceRow.id) : "";
    return treeNodeHtml({
      kind: "reported",
      type: result.activityType,
      label: result.activityType === "Linked DPP" ? chainSourceLabel(result, sourceRow, entry) : result.componentName,
      meta: `${fmtQuantity(result.quantity, result.unit)} | ${result.activityType === "Database activity" ? result.region || "database location not set" : sourceRow.sourceLocation || result.region || "location not set"} | ${chainActivityDetail(result, sourceRow, entry)}`,
      impact: result.totalImpactTonnesCO2eq,
      activityType: result.activityType,
      rowId: linked?.ok ? sourceRow.id : "",
      expandRowId: linked?.ok ? sourceRow.id : "",
      expanded,
    }, nestedHtml);
  }).join("");
  const emissionsHtml = emissionRows.length ? treeNodeHtml({
    kind: "reported emission-node",
    type: "Direct emissions",
    label: "Reported direct emissions",
    meta: `${emissionRows.length} elementary flow${emissionRows.length === 1 ? "" : "s"} kept separate from GWP100 impact tree`,
    impactLabel: "reported separately",
    activityType: "Direct emission",
  }, `<ul>${emissionRows.map((result) => {
    const sourceRow = state.rows.find((row) => row.id === result.rowId) || {};
    return treeNodeHtml({
      kind: "nested emission-node",
      type: "Direct emission",
      label: sourceRow.directEmissionVocabulary?.biosphere3Name || sourceRow.directEmissionName || result.componentName || "Direct emission",
      meta: [result.quantity && result.unit ? fmtQuantity(result.quantity, result.unit) : "", sourceRow.sourceLocation || "", sourceRow.directEmissionVocabulary?.biosphere3Uuid || ""].filter(Boolean).join(" | "),
      impactLabel: "reported separately",
      activityType: "Direct emission",
    });
  }).join("")}</ul>`) : "";
  const rootNode = treeNodeHtml({
    kind: "root",
    type: "Final DPP",
    label: productName,
    meta: `${fmtQuantity(ref.amount, ref.unit)} - ${ref.description}`,
    impact: calculation.totals.totalImpactTonnesCO2eq,
    activityType: "Final DPP",
  }, rows || emissionsHtml ? `<ul>${rows}${emissionsHtml}</ul>` : "");
  tree.innerHTML = `<div class="dpp-tree" aria-label="DPP tree view">
    <ul class="dpp-tree-list">${rows || emissionsHtml ? rootNode : `<li><div class="notice">Add inventory rows to build the DPP tree.</div></li>`}</ul>
  </div>`;
}


function dppReferenceSummary(payload, linked) {
  const ref = payload?.functionalUnit?.referenceFlow || linked?.referenceFlow || {};
  const amount = ref.amount ?? payload?.functionalUnit?.howMuch ?? linked?.referenceAmount ?? 1;
  const unit = ref.unit || payload?.functionalUnit?.unit || linked?.referenceUnit || "reference flow";
  const description = ref.description || payload?.functionalUnit?.what || linked?.referenceDescription || "";
  return `${fmtQuantity(amount, unit)}${description ? ` - ${description}` : ""}`;
}

function rowImpactValue(row, fallbackRow = {}) {
  return row?.totalImpactTonnesCO2eq ?? row?.impactTonnesCO2eq ?? row?.impact ?? row?.calculation?.impactTonnesCO2eq ?? fallbackRow?.totalImpactTonnesCO2eq ?? fallbackRow?.impactTonnesCO2eq ?? fallbackRow?.impact ?? fallbackRow?.calculation?.impactTonnesCO2eq ?? 0;
}

function rowLabel(row, fallbackRow = {}) {
  return row?.componentName || row?.description || row?.name || fallbackRow?.componentName || fallbackRow?.description || fallbackRow?.name || "Unnamed entry";
}

function rowQuantityText(row, fallbackRow = {}) {
  const amount = row?.quantity ?? row?.value ?? fallbackRow?.quantity ?? fallbackRow?.value;
  const unit = row?.unit || fallbackRow?.unit || "";
  return fmtQuantity(amount, unit);
}

function rowDatabaseText(row, fallbackRow = {}) {
  const direct = row?.directEmissionVocabulary || fallbackRow?.directEmissionVocabulary;
  if (direct) {
    const name = direct.biosphere3Name || direct.sourceDescription || "Direct emission";
    const compartment = direct.biosphere3Compartment || direct.sourceCompartment || "";
    const uuid = direct.biosphere3Uuid ? ` | ${direct.biosphere3Uuid}` : "";
    return direct.mappingStatus === "mapped" ? `${name}${compartment ? ` | ${compartment}` : ""}${uuid}` : `${name} | not mapped`;
  }
  const db = row?.databaseEntry || fallbackRow?.databaseEntry || {};
  return db.label || db.code || row?.footprintLabel || row?.footprintCode || row?.code || fallbackRow?.code || "-";
}

function rowStatusText(row, fallbackRow = {}) {
  return row?.status || row?.calculation?.status || fallbackRow?.status || fallbackRow?.calculation?.status || "-";
}

function directEmissionsHtml(payload) {
  const direct = payload?.directEmissions;
  const rows = Array.isArray(direct?.rows) ? direct.rows : [];
  if (!rows.length) {
    return `<div class="notice">No direct-emission vocabulary rows are listed in this DPP.</div>`;
  }
  const fossil = direct.reportedDirectFossilCO2Tonnes ? `<div class="notice warn">Reported fossil CO2 direct emission: ${escapeHtml(fmtKg(direct.reportedDirectFossilCO2Tonnes))}. This is shown separately from the BONSAI process/object lookup total.</div>` : "";
  return `${fossil}<div class="viewer-table-wrap"><table class="viewer-table viewer-table-direct"><thead><tr><th>Emission</th><th>Compartment</th><th>Quantity</th><th>Biosphere3 mapping</th><th>UUID</th><th>Status</th></tr></thead><tbody>${rows.map((row) => `<tr>
    <td>${escapeHtml(row.description || "-")}</td>
    <td>${escapeHtml(row.compartment || "-")}</td>
    <td>${escapeHtml(fmtQuantity(row.quantity, row.unit || ""))}</td>
    <td>${escapeHtml(row.biosphere3Name || "-")}<br><small>${escapeHtml(row.biosphere3Compartment || "")}</small></td>
    <td><small>${escapeHtml(row.biosphere3Uuid || "-")}</small></td>
    <td>${escapeHtml(row.mappingStatus || "-")}</td>
  </tr>`).join("")}</tbody></table></div>`;
}

function viewerRowsHtml(payload) {
  const resultRows = Array.isArray(payload?.results?.rows) ? payload.results.rows : [];
  const inventoryRows = Array.isArray(payload?.inventoryRows) ? payload.inventoryRows : [];
  const rows = inventoryRows.length ? inventoryRows.map((row, index) => ({ source: row, result: resultRows[index] || {} })) : resultRows.map((row) => ({ source: row, result: row }));
  const meaningfulRows = rows.filter(({ source, result }) => rowLabel(source, result) !== "Unnamed entry" || rowImpactValue(source, result) !== 0 || rowStatusText(source, result) !== "-");

  if (meaningfulRows.length) {
    return `<div class="viewer-table-wrap"><table class="viewer-table"><thead><tr><th>Entry</th><th>Type/stage</th><th>Quantity</th><th>Database entry</th><th>Status</th><th>Impact</th></tr></thead><tbody>${meaningfulRows.map(({ source, result }) => `<tr>
      <td>${escapeHtml(rowLabel(source, result))}</td>
      <td>${escapeHtml(source.type || result.type || source.stage || result.stage || "-")}</td>
      <td>${escapeHtml(rowQuantityText(source, result))}</td>
      <td>${escapeHtml(rowDatabaseText(source, result))}</td>
      <td>${escapeHtml(rowStatusText(source, result))}</td>
      <td>${fmtKg(rowImpactValue(source, result))}</td>
    </tr>`).join("")}</tbody></table></div>`;
  }

  return `<div class="notice">This linked DPP does not contain explorable component rows.</div>`;
}

function openDppViewer(rowId) {
  const row = state.rows.find((item) => item.id === rowId);
  const linked = row?.linkedDpp;
  if (!linked?.ok) return;
  openDppPayloadViewer(linked.rawDpp || {}, linked);
}

function openDppPayloadViewer(payload = {}, linked = {}) {
  const totals = payload?.results?.totals || {};
  const product = payload.product || {};
  const linkedDpps = Array.isArray(payload.linkedDpps) ? payload.linkedDpps : [];
  $("dppViewerTitle").textContent = linked.productName || product.name || linked.dppId || "Linked DPP";
  $("dppViewerBody").innerHTML = `<div class="viewer-summary">
    <div><span>DPP ID</span><strong>${escapeHtml(payload.dppId || product.dppId || linked.dppId || "-")}</strong></div>
    <div><span>Product</span><strong>${escapeHtml(product.name || linked.productName || "-")}</strong></div>
    <div><span>Reference flow</span><strong>${escapeHtml(dppReferenceSummary(payload, linked))}</strong></div>
    <div><span>Total impact</span><strong>${escapeHtml(fmtKg(totals.totalImpactTonnesCO2eq ?? linked.impactTonnesCO2eq))}</strong></div>
  </div>
  <section class="viewer-section"><h3>Impact entries inside this DPP</h3>${viewerRowsHtml(payload)}</section>
  <section class="viewer-section"><h3>Direct emissions vocabulary</h3>${directEmissionsHtml(payload)}</section>
  <section class="viewer-section"><h3>Linked DPPs carried by this DPP</h3>${linkedDpps.length ? `<div class="linked-dpp-list">${linkedDpps.map((item) => `<div class="linked-dpp-item"><strong>${escapeHtml(item.productName || item.dppId || "Linked DPP")}</strong><span>${escapeHtml(linkedReferenceText(item))}</span><span>${escapeHtml(fmtKg(item.impactTonnesCO2eq))} per reference flow</span></div>`).join("")}</div>` : `<div class="notice">No nested linked DPPs are listed in this DPP.</div>`}</section>`;
  $("dppViewer").classList.remove("hidden");
}

function closeDppViewer() {
  $("dppViewer")?.classList.add("hidden");
}

function renderResults() {
  calculation = calculateRows(rowsForCalculation(), lookupByCode);
  $("totalImpact").textContent = fmtKg(calculation.totals.totalImpactTonnesCO2eq);
  $("ownImpact").textContent = fmtKg(calculation.totals.ownImpactTonnesCO2eq);
  $("linkedImpact").textContent = fmtKg(calculation.totals.upstreamLinkedDppImpactTonnesCO2eq);
  $("calculatedRows").textContent = String(calculation.rows.filter((row) => row.status === "calculated").length);
  renderStatusStrip();
  const hasCff = state.rows.some((row) => row.id === "eol_silicon_cff_credit" || /cff/i.test(`${row.componentName || ""} ${row.description || ""} ${row.notes || ""}`));
  $("cffNotice")?.remove();
  if (hasCff) {
    const notice = document.createElement("div");
    notice.id = "cffNotice";
    notice.className = "notice cff-notice";
    notice.textContent = "Silicon recycling benefit is calculated using the Circular Footprint Formula (CFF).";
    $("resultsPanel")?.querySelector(".summary-grid")?.before(notice);
  }

  renderContributionList("componentChart", calculation.rows.filter((row) => row.totalImpactTonnesCO2eq !== 0).map((row) => ({ label: row.componentName, value: row.totalImpactTonnesCO2eq })), calculation.totals.totalImpactTonnesCO2eq);
  renderContributionList("stageChart", Object.entries(calculation.totals.byStage).filter(([, value]) => value !== 0).map(([label, value]) => ({ label, value })), calculation.totals.totalImpactTonnesCO2eq);
  renderLcaPanel();

  const body = $("resultsTable").querySelector("tbody");
  body.innerHTML = calculation.rows.map((row) => `<tr>
    <td>${escapeHtml(row.componentName)}</td>
    <td>${escapeHtml(row.stage)}</td>
    <td>${escapeHtml(fmtQuantity(row.quantity, row.unit))}</td>
    <td>${escapeHtml(row.activityType === "Direct emission" ? directEmissionMappingText(row) : row.footprintLabel || row.footprintCode || "-")}</td>
    <td>${fmtKg(row.ownImpactTonnesCO2eq)}</td>
    <td>${fmtKg(row.linkedImpactTonnesCO2eq)}</td>
    <td>${fmtKg(row.totalImpactTonnesCO2eq)}</td>
    <td>${escapeHtml(row.status)}</td>
    <td>${escapeHtml(row.warnings.join("; "))}</td>
  </tr>`).join("");
}

function renderContributionList(id, items, total) {
  const el = $(id);
  if (!items.length) {
    el.innerHTML = `<div class="notice">No calculated impact to display.</div>`;
    return;
  }
  const positiveTotal = items.filter((item) => item.value > 0).reduce((sum, item) => sum + item.value, 0);
  const absoluteTotal = items.reduce((sum, item) => sum + Math.abs(item.value), 0);
  const denominator = positiveTotal || absoluteTotal;
  if (!denominator) {
    el.innerHTML = `<div class="notice">No calculated impact to display.</div>`;
    return;
  }
  const burdens = items.filter((item) => item.value > 0);
  const credits = items.filter((item) => item.value < 0);
  const neutral = items.filter((item) => item.value === 0);
  const ordered = [...burdens, ...credits, ...neutral];
  el.innerHTML = ordered.map((item) => {
    const isCredit = item.value < 0;
    const percent = Math.abs(item.value) / denominator * 100;
    const width = Math.min(100, percent);
    const label = isCredit ? `credit -${percent.toLocaleString(undefined, { maximumFractionDigits: 1 })}% of gross burdens` : `burden ${percent.toLocaleString(undefined, { maximumFractionDigits: 1 })}% of gross burdens`;
    return `<div class="contribution-row${isCredit ? " credit" : ""}">
      <div class="contribution-top"><strong>${escapeHtml(item.label)}</strong><span>${escapeHtml(label)}</span></div>
      <div class="contribution-track" aria-label="${escapeHtml(item.label)} ${isCredit ? "credit" : "burden"}"><div class="contribution-fill" style="width:${width}%"></div></div>
      <div class="contribution-value">${fmtKg(item.value)}</div>
    </div>`;
  }).join("");
}

function exportPackage() {
  readLca();
  readSetup();
  const rows = rowsForCalculation();
  const exportedRows = rows.map(({ includeOwnCalculation, includeLinkedDpp, directEmissionCompartment, ...row }) => row);
  calculation = calculateRows(rows, lookupByCode);
  return buildExportPackage({ ...state, rows: exportedRows }, calculation, lookup.metadata);
}

function renderExport() {
  $("exportPreview").value = JSON.stringify(exportPackage(), null, 2);
}

function renderAll() {
  readLca();
  readSetup();
  calculation = calculateRows(rowsForCalculation(), lookupByCode);
  renderInventory();
  renderResults();
  renderExport();
}

async function loadLookup() {
  const response = await fetch("data/footprint_lookup.json", { cache: "no-store" });
  if (!response.ok) throw new Error(`Lookup load failed: ${response.status}`);
  lookup = await response.json();
  lookupByCode = new Map(lookup.entries.map((entry) => [entry.code, entry]));
  locationOptions = buildLocationOptions();
  populateProductLocationSelect(state.product.location || "");
  $("lookupMeta").textContent = `${lookup.metadata.sourceVersion || "database"} | ${lookup.metadata.metric}`;
  if ($("statusDatabase")) $("statusDatabase").textContent = `${lookup.metadata.metric} ready`;
}

async function loadDirectEmissionVocabulary() {
  const response = await fetch("data/direct_emission_vocabulary.json", { cache: "no-store" });
  if (!response.ok) throw new Error(`Direct-emission vocabulary load failed: ${response.status}`);
  directEmissionVocabulary = await response.json();
  directEmissionByKey = new Map((directEmissionVocabulary.entries || []).map((entry) => [directEmissionKey(entry.description, entry.compartment), entry]));
  directEmissionByName = new Map();
  for (const entry of directEmissionVocabulary.entries || []) {
    const key = String(entry.description || "").trim().toLowerCase();
    if (!directEmissionByName.has(key)) directEmissionByName.set(key, []);
    directEmissionByName.get(key).push(entry);
  }
}

function refreshDirectEmissionMappings(rows = state.rows) {
  rows.forEach((row) => {
    if ((row.activityType || "Database activity") === "Direct emission") updateDirectEmissionMapping(row);
  });
}

async function loadDefaultCellDppRow() {
  if (state.rows.length > 0) return;
  const response = await fetch("data/pv_cell_single_si_cn_dpp.json", { cache: "no-store" });
  if (!response.ok) throw new Error(`Default PV cell DPP load failed: ${response.status}`);
  const payload = await response.json();
  const row = createRow();
  row.activityType = "Linked DPP";
  row.quantity = "";
  row.unit = "cell";
  row.sourceLocation = "CN";
  row.notes = "Preloaded from the workshop PV cell DPP. The DPP reference flow is one 210 x 210 mm cell; change the cell count to match the panel design.";
  const extracted = extractLinkedDppImpact(payload);
  applyLinkedDppToRow(row, { fileName: "pv_cell_single_si_cn_dpp.json", rawDpp: payload, multiplier: 0, ...extracted });
  row.quantity = "";
  row.unit = "cell";
  row.sourceLocation = extracted.productLocation || "CN";
  syncLinkedMultiplierFromQuantity(row);
  state.rows.push(row);
}

function download(name, text, type) {
  const blob = new Blob([text], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function readJsonFile(file) {
  return JSON.parse(await file.text());
}

function installEvents() {
  document.querySelectorAll(".tab-btn").forEach((button) => {
    button.addEventListener("click", () => switchTab(button.dataset.tab));
  });
  $("brandMark")?.addEventListener("dblclick", async () => {
    const button = $("brandMark");
    setButtonBusy(button, true, "EoL");
    try {
      await loadEolScenario();
    } catch (error) {
      alert(`EoL scenario load failed: ${error.message || error}`);
    } finally {
      setButtonBusy(button, false);
    }
  });

  document.querySelectorAll("#setupPanel input, #setupPanel textarea, #setupPanel select").forEach((input) => input.addEventListener("input", renderAll));
  ["addRowBtn", "emptyAddRowBtn"].forEach((id) => $(id)?.addEventListener("click", addBlankRow));
  $("refreshExportBtn").addEventListener("click", renderExport);
  $("downloadJsonBtn")?.addEventListener("click", () => download("pv-panel-dpp-workshop-package.json", JSON.stringify(exportPackage(), null, 2), "application/json;charset=utf-8"));
  $("downloadCsvBtn")?.addEventListener("click", () => download("pv-panel-dpp-workshop-results.csv", resultsToCsv(calculateRows(rowsForCalculation(), lookupByCode)), "text/csv;charset=utf-8"));
  $("loadPackageBtn")?.addEventListener("click", () => $("loadPackageInput")?.click());
  $("loadPackageInput")?.addEventListener("change", async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const button = $("loadPackageBtn");
    setButtonBusy(button, true, "Loading package...");
    try {
      const payload = await readJsonFile(file);
      if (Array.isArray(payload.inventoryRows)) {
        state.product = payload.product || state.product;
        state.functionalUnit = payload.functionalUnit || state.functionalUnit;
        state.lca = payload.lca || state.lca;
        if (payload.functionalUnit?.referenceFlow) {
          state.lca.referenceFlow = payload.functionalUnit.referenceFlow;
        }
        if (!payload.lca && payload.functionalUnit?.referenceFlow?.description) {
          state.lca.functionalUnitStatement = payload.functionalUnit.referenceFlow.description;
        }
        state.rows = normalizeImportedRows(payload.inventoryRows || []);
        refreshDirectEmissionMappings();
        hydrateSetup();
        hydrateLca();
        renderAll();
      } else {
        alert("The selected JSON is not a workshop export package.");
      }
    } catch (error) {
      alert(`Package import failed: ${error.message || error}`);
    } finally {
      setButtonBusy(button, false);
      event.target.value = "";
    }
  });

  $("inventoryBody").addEventListener("input", (event) => {
    const target = event.target;
    const tr = target.closest("[data-row]");
    if (!tr) return;
    const row = state.rows.find((item) => item.id === tr.dataset.row);
    if (!row) return;
    const field = target.dataset.field;
    if (!field || field === "linkedDppFile") return;
    let needsInventoryRefresh = false;
    if (field === "factorSearch") {
      const previousCode = row.footprintCode;
      row.factorSearchText = target.value;
      const entry = entryFromDisplay(target.value);
      if (entry) {
        row.footprintCode = entry.code;
        row.factorSearchText = factorDisplay(entry);
        row.referenceUnit = entry.units?.[0] || "";
        const regions = entry.regionsByUnit?.[row.referenceUnit] || [];
        row.region = regions.includes("WE") ? "WE" : regions.includes("Global") ? "Global" : regions[0] || "";
        closeEntryMenu();
        needsInventoryRefresh = true;
      } else {
        if (previousCode) {
          row.footprintCode = "";
          row.referenceUnit = "";
          row.region = "";
          needsInventoryRefresh = true;
        }
        openEntryMenu(target, row);
      }
    } else if (field === "linkedMultiplier") {
      row.linkedDpp = row.linkedDpp || { ok: false, error: "Linked DPP file not uploaded." };
      row.linkedDpp.multiplier = Number(target.value) || 0;
    } else {
      row[field] = target.type === "checkbox" ? target.checked : target.value;
      if (field === "activityType") {
        if (row.activityType === "Direct emission") updateDirectEmissionMapping(row);
        needsInventoryRefresh = true;
      }
      if (field === "directEmissionName") {
        updateDirectEmissionMapping(row);
        if (!row.componentName) row.componentName = row.directEmissionName;
        needsInventoryRefresh = true;
      }
      if (field === "quantity" || field === "unit" || field === "customUnit") syncLinkedMultiplierFromQuantity(row);
      if (field === "referenceUnit") {
        const entry = lookupByCode.get(row.footprintCode);
        const regions = entry?.regionsByUnit?.[row.referenceUnit] || [];
        row.region = regions.includes(row.region) ? row.region : regions.includes("WE") ? "WE" : regions.includes("Global") ? "Global" : regions[0] || "";
        needsInventoryRefresh = true;
      }
    }
    calculation = calculateRows(rowsForCalculation(), lookupByCode);
    renderResults();
    renderExport();
    if (needsInventoryRefresh) renderInventory();
  });

  $("inventoryBody").addEventListener("focusin", (event) => {
    const target = event.target;
    if (target?.dataset?.field !== "factorSearch") return;
    const tr = target.closest("[data-row]");
    const row = state.rows.find((item) => item.id === tr?.dataset.row);
    if (row) openEntryMenu(target, row);
  });

  $("inventoryBody").addEventListener("keydown", (event) => {
    if (event.target?.dataset?.field !== "factorSearch" || event.key !== "Enter") return;
    const first = entryMenu?.querySelector(".entry-option");
    if (first) {
      event.preventDefault();
      first.click();
    }
  });

  document.addEventListener("click", (event) => {
    if (event.target.closest(".entry-menu") || event.target.closest(".select-menu") || event.target.closest('input[data-field="factorSearch"]') || event.target.closest('button[data-action="customSelect"]')) return;
    closeEntryMenu();
    closeSelectMenu();
  });
  // Keep floating menus open while users scroll inside them; close on outside click or resize only.
  window.addEventListener("resize", () => {
    closeEntryMenu();
    closeSelectMenu();
  });

  ensureEntryMenu().addEventListener("click", (event) => {
    const option = event.target.closest(".entry-option");
    if (!option || !activeEntryRowId) return;
    const row = state.rows.find((item) => item.id === activeEntryRowId);
    const entry = lookupByCode.get(option.dataset.entryCode);
    if (row && entry) selectEntryForRow(row, entry);
  });

  $("inventoryBody").addEventListener("change", async (event) => {
    const target = event.target;
    const tr = target.closest("[data-row]");
    if (!tr) return;
    const row = state.rows.find((item) => item.id === tr.dataset.row);
    if (!row) return;
    if (target.dataset.action === "remove") return;
    if ((target.dataset.field === "quantity" || target.dataset.field === "unit" || target.dataset.field === "customUnit") && row.linkedDpp?.ok) {
      syncLinkedMultiplierFromQuantity(row);
      renderAll();
      return;
    }
    if (target.dataset.field === "linkedDppFile") {
      const file = target.files?.[0];
      if (!file) return;
      const button = tr.querySelector("button[data-action='uploadDpp']");
      setButtonBusy(button, true, "Reading DPP...");
      try {
        const payload = await readJsonFile(file);
        const extracted = extractLinkedDppImpact(payload);
        row.activityType = "Linked DPP";
        applyLinkedDppToRow(row, { fileName: file.name, rawDpp: payload, multiplier: row.linkedDpp?.multiplier || 1, ...extracted });
      } catch (error) {
        row.linkedDpp = { ok: false, fileName: file.name, multiplier: row.linkedDpp?.multiplier || 1, error: String(error.message || error) };
      } finally {
        setButtonBusy(button, false);
        target.value = "";
      }
      renderAll();
    }
  });

  $("inventoryBody").addEventListener("click", (event) => {
    const selectButtonEl = event.target.closest("button[data-action='customSelect']");
    if (selectButtonEl) {
      const tr = selectButtonEl.closest("[data-row]");
      const row = state.rows.find((item) => item.id === tr?.dataset.row);
      if (row) openSelectMenu(selectButtonEl, row, selectButtonEl.dataset.field);
      return;
    }
    const uploadButton = event.target.closest("button[data-action='uploadDpp']");
    if (uploadButton) {
      const tr = uploadButton.closest("[data-row]");
      tr?.querySelector('input[data-field="linkedDppFile"]')?.click();
      return;
    }
    const button = event.target.closest("button[data-action='remove']");
    if (!button) return;
    const tr = button.closest("[data-row]");
    removeRow(tr.dataset.row);
  });

  document.addEventListener("click", (event) => {
    const toggleButton = event.target.closest('button[data-action="toggleDppTree"]');
    if (toggleButton) {
      const rowId = toggleButton.dataset.rowId;
      if (expandedDppRows.has(rowId)) expandedDppRows.delete(rowId);
      else expandedDppRows.add(rowId);
      renderLcaPanel();
      return;
    }
    const payloadButton = event.target.closest('button[data-action="viewDppPayload"]');
    if (payloadButton) {
      const record = treeDppPayloads.get(payloadButton.dataset.payloadKey);
      if (record) openDppPayloadViewer(record.payload, record.linked);
      return;
    }
    const viewButton = event.target.closest('button[data-action="viewDpp"]');
    if (viewButton) {
      openDppViewer(viewButton.dataset.rowId);
      return;
    }
    if (event.target.id === "dppViewer" || event.target.closest("#closeDppViewerBtn")) {
      closeDppViewer();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeDppViewer();
  });

}

async function init() {
  readSetup();
  hydrateLca();
  installEvents();
  setLookupLoading(true);
  try {
    await loadLookup();
    await loadDirectEmissionVocabulary();
    refreshDirectEmissionMappings();
    await loadDefaultCellDppRow();
  } catch (error) {
    $("lookupMeta").textContent = "Database lookup failed to load.";
    if ($("statusDatabase")) $("statusDatabase").textContent = "Unavailable";
    $("inventoryNotice").className = "empty-state error";
    $("inventoryNotice").textContent = String(error.message || error);
  } finally {
    setLookupLoading(false);
  }
  renderAll();
}

init();
