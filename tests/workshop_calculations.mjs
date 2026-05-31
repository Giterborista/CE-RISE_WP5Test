import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { calculateRows, convertQuantity, extractLinkedDppImpact, buildExportPackage } from "../assets/workshop-core.mjs";

assert.equal(convertQuantity(1, "kg", "tonnes"), 0.001);
assert.equal(convertQuantity(1000, "g", "tonnes"), 0.001);
assert.equal(convertQuantity(1, "kWh", "TJ"), 0.0000036);
assert.equal(convertQuantity(3, "items", "items"), 3);
assert.ok(Number.isNaN(convertQuantity(1, "kg", "TJ")));

const lookupByCode = new Map([
  ["M_GLAS_01_RES", { code: "M_GLAS_01_RES", label: "glass", factors: { "tonnes|WE": 1.5 } }],
]);

const calc = calculateRows([
  { id: "r1", componentName: "Front glass", stage: "Manufacturing", quantity: 10, unit: "kg", footprintCode: "M_GLAS_01_RES", referenceUnit: "tonnes", region: "WE" },
  { id: "r2", componentName: "Free text row", stage: "Manufacturing", quantity: 1, unit: "kg" },
  { id: "r3", componentName: "Cells", stage: "Raw material acquisition", quantity: 60, unit: "items", linkedDpp: { impactTonnesCO2eq: 0.0025, multiplier: 60 } },
], lookupByCode);
assert.equal(calc.rows[0].ownImpactTonnesCO2eq, 0.015);
assert.equal(calc.rows[1].status, "not calculated");
assert.equal(calc.rows[2].linkedImpactTonnesCO2eq, 0.15);
assert.ok(Math.abs(calc.totals.totalImpactTonnesCO2eq - 0.165) < 1e-12);

const blankLinkedQuantity = calculateRows([
  { id: "blank-cell", componentName: "Cells", quantity: "", unit: "cell", activityType: "Linked DPP", linkedDpp: { impactTonnesCO2eq: 0.000805664894730034 } },
], lookupByCode);
assert.equal(blankLinkedQuantity.rows[0].linkedImpactTonnesCO2eq, 0);

const zeroLinkedQuantity = calculateRows([
  { id: "zero-cell", componentName: "Cells", quantity: 0, unit: "cell", activityType: "Linked DPP", linkedDpp: { impactTonnesCO2eq: 0.000805664894730034, multiplier: 0 } },
], lookupByCode);
assert.equal(zeroLinkedQuantity.rows[0].linkedImpactTonnesCO2eq, 0);

const cellPayload = JSON.parse(await readFile(new URL("./fixtures/cell_dpp_example.json", import.meta.url), "utf8"));
const linked = extractLinkedDppImpact(cellPayload);
assert.equal(linked.ok, true);
assert.equal(linked.impactTonnesCO2eq, 0.0025);

const pkg = buildExportPackage({ product: { name: "PV panel" }, functionalUnit: { unit: "panel" }, rows: [] }, calc, { sourceVersion: "v2.1.6" });
assert.equal(pkg.packageType, "CERISE_DPP_WORKSHOP_EXPORT");
assert.ok(Math.abs(pkg.results.totals.totalImpactTonnesCO2eq - 0.165) < 1e-12);
console.log("workshop calculation tests passed");
