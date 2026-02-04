export function normalizeText(value) {
  if (!value) return "";
  return String(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

export function normalizeCode(value) {
  if (value == null) return null;
  const cleaned = String(value).replace(/[^0-9a-z]/gi, "").toUpperCase();
  return cleaned || null;
}

function cleanValue(value) {
  if (value == null) return null;
  const str = String(value).trim();
  if (!str) return null;
  if (/^(n\/a|na|none|aucun|inconnu|not available)$/i.test(str)) return null;
  return str;
}

async function expandSpecsSection(page) {
  const labels = ["Spécifications", "Specifications", "Détails", "Details"];
  for (const label of labels) {
    const selectors = [
      `button:has-text("${label}")`,
      `summary:has-text("${label}")`,
      `[role="button"]:has-text("${label}")`,
      `a:has-text("${label}")`,
    ];
    for (const selector of selectors) {
      const loc = page.locator(selector).first();
      if (await loc.isVisible().catch(() => false)) {
        const isExpanded = await loc.evaluate((el) => {
          if (!el) return false;
          const expandedAttr = el.getAttribute?.("aria-expanded");
          if (expandedAttr) {
            return expandedAttr.toLowerCase() === "true";
          }
          if (el.tagName === "SUMMARY") {
            const parent = el.closest("details");
            if (parent) return parent.hasAttribute("open");
          }
          return el.classList?.contains("is-open") || el.classList?.contains("open");
        }).catch(() => false);
        if (isExpanded) return;
        await loc.click({ timeout: 2000 }).catch(() => {});
        await page.waitForTimeout(300);
        return;
      }
    }
  }
}

export async function extractModelDataFromPage(page) {
  await expandSpecsSection(page);

  const raw = await page.evaluate(() => {
    const normalizeText = (value) => {
      if (!value) return "";
      return String(value)
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();
    };

    const cleanValue = (value) => {
      if (value == null) return null;
      const str = String(value).trim();
      if (!str) return null;
      if (/^(n\/a|na|none|aucun|inconnu|not available)$/i.test(str)) return null;
      return str;
    };

    const extractValue = (value) => {
      if (value == null) return null;
      if (typeof value === "string" || typeof value === "number") {
        return cleanValue(value);
      }
      if (typeof value === "object") {
        if (value.name) return cleanValue(value.name);
        if (value.value) return cleanValue(value.value);
      }
      return null;
    };

    const dfsFindValue = (root, keys) => {
      const stack = [root];
      while (stack.length) {
        const current = stack.pop();
        if (!current || typeof current !== "object") continue;
        for (const key of keys) {
          if (Object.prototype.hasOwnProperty.call(current, key)) {
            const candidate = extractValue(current[key]);
            if (candidate) return candidate;
          }
        }
        for (const value of Object.values(current)) {
          if (value && typeof value === "object") {
            stack.push(value);
          }
        }
      }
      return null;
    };

    const jsonLdParse = (text) => {
      try {
        return JSON.parse(text);
      } catch {
        const sanitized = text
          .replace(/\u2028/g, "\\u2028")
          .replace(/\u2029/g, "\\u2029");
        try {
          return JSON.parse(sanitized);
        } catch {
          return null;
        }
      }
    };

    const extractJsonLd = () => {
      const scripts = Array.from(
        document.querySelectorAll('script[type="application/ld+json"]')
      );
      for (const script of scripts) {
        const text = script.textContent;
        if (!text) continue;
        const parsed = jsonLdParse(text);
        if (!parsed) continue;
        const items = Array.isArray(parsed) ? parsed : [parsed];
        for (const item of items) {
          const model = dfsFindValue(item, [
            "mpn",
            "model",
            "modelNumber",
            "model_number",
            "manufacturerPartNumber",
            "partNumber",
            "mfrPartNumber",
            "modelNo",
            "modelNum",
          ]);
          const brand = dfsFindValue(item, ["brand", "brandName", "manufacturer"]);
          const upc = dfsFindValue(item, [
            "gtin12",
            "gtin13",
            "gtin14",
            "gtin8",
            "gtin",
            "upc",
            "upcCode",
            "upc_code",
          ]);
          if (model || brand || upc) {
            return { model_number: model, brand, upc, source: "jsonld" };
          }
        }
      }
      return null;
    };

    const extractNextData = () => {
      const script = document.querySelector("script#__NEXT_DATA__");
      if (!script?.textContent) return null;
      let parsed;
      try {
        parsed = JSON.parse(script.textContent);
      } catch {
        return null;
      }
      const model = dfsFindValue(parsed, [
        "mpn",
        "modelNumber",
        "manufacturerPartNumber",
        "model_number",
        "partNumber",
        "mfrPartNumber",
        "modelNo",
        "modelNum",
      ]);
      const brand = dfsFindValue(parsed, ["brand", "brandName", "manufacturer"]);
      const upc = dfsFindValue(parsed, [
        "upc",
        "gtin",
        "gtin12",
        "gtin13",
        "gtin14",
        "gtin8",
        "upcCode",
        "upc_code",
      ]);
      if (model || brand || upc) {
        return { model_number: model, brand, upc, source: "next" };
      }
      return null;
    };

    const extractFromSpecs = () => {
      const labelGroups = {
        model_number: [
          "numero de modele",
          "numero de modèle",
          "n° modele",
          "n° modèle",
          "no. de modele",
          "no. de modèle",
          "no de modele",
          "no de modèle",
          "numero du modele",
          "numero du modèle",
          "model number",
          "mpn",
        ],
        part_number: [
          "numero de piece",
          "numero de pièce",
          "numero de produit",
          "part number",
          "item number",
          "sku",
          "n° de piece",
          "n° de pièce",
        ],
        upc: ["upc", "gtin", "code-barres", "code barres", "ean"],
        brand: ["marque", "brand", "fabricant", "manufacturer"],
      };

      const buildLabelIndex = () => {
        const index = new Map();
        for (const [key, labels] of Object.entries(labelGroups)) {
          labels.forEach((label) => index.set(label, key));
        }
        return index;
      };

      const labelIndex = buildLabelIndex();

      const matchLabelKey = (label) => {
        const normalized = normalizeText(label);
        if (!normalized) return null;
        if (labelIndex.has(normalized)) return labelIndex.get(normalized);
        for (const [labelKey, key] of labelIndex.entries()) {
          if (normalized === labelKey || normalized.startsWith(labelKey)) {
            return key;
          }
        }
        return null;
      };

      const extractPairsFromContainer = (root) => {
        const pairs = [];
        if (!root) return pairs;

        const addPair = (label, value) => {
          const cleanedValue = cleanValue(value);
          if (!label || !cleanedValue) return;
          pairs.push({ label: String(label), value: cleanedValue });
        };

        root.querySelectorAll("dt").forEach((dt) => {
          const dd = dt.nextElementSibling;
          addPair(dt.textContent, dd ? dd.textContent : null);
        });

        root.querySelectorAll("table tr").forEach((row) => {
          const cells = Array.from(row.querySelectorAll("th, td"));
          if (cells.length < 2) return;
          addPair(cells[0].textContent, cells[1].textContent);
        });

        root.querySelectorAll("li").forEach((li) => {
          const text = li.textContent || "";
          if (text.includes(":")) {
            const [label, ...rest] = text.split(":");
            addPair(label, rest.join(":"));
            return;
          }
          const children = Array.from(li.children || []);
          if (children.length >= 2) {
            addPair(children[0].textContent, children[1].textContent);
          }
        });

        root.querySelectorAll("div, p, span").forEach((node) => {
          if (node.children && node.children.length >= 2) {
            const children = Array.from(node.children).filter(
              (child) => (child.textContent || "").trim()
            );
            if (children.length >= 2) {
              addPair(children[0].textContent, children[1].textContent);
              return;
            }
          }
          const text = node.textContent || "";
          if (text.includes(":")) {
            const [label, ...rest] = text.split(":");
            addPair(label, rest.join(":"));
          }
        });

        return pairs;
      };

      const findSpecsContainer = () => {
        const labels = ["specifications", "spécifications", "details", "détails"];
        const nodes = Array.from(
          document.querySelectorAll("h2, h3, h4, button, summary, a, span, div")
        );
        for (const node of nodes) {
          const text = normalizeText(node.textContent || "");
          if (!text) continue;
          if (!labels.some((label) => text === label || text.includes(label))) continue;
          return (
            node.closest("section, article, div") || node.parentElement || document.body
          );
        }
        return document.body;
      };

      const container = findSpecsContainer();
      const pairs = extractPairsFromContainer(container);
      if (!pairs.length) {
        pairs.push(...extractPairsFromContainer(document.body));
      }

      const output = {
        model_number: null,
        part_number: null,
        upc: null,
        brand: null,
        pairs,
      };

      for (const { label, value } of pairs) {
        const key = matchLabelKey(label);
        if (!key) continue;
        if (!output[key]) output[key] = value;
      }

      return output;
    };

    const jsonLd = extractJsonLd();
    const nextData = extractNextData();
    const specData = extractFromSpecs();

    return {
      jsonLd,
      nextData,
      specData,
    };
  });

  const modelNumber = cleanValue(
    raw?.jsonLd?.model_number
      || raw?.nextData?.model_number
      || raw?.specData?.model_number
  );
  const partNumber = cleanValue(raw?.specData?.part_number);
  const brand = cleanValue(raw?.jsonLd?.brand || raw?.nextData?.brand);
  const upc = cleanValue(raw?.jsonLd?.upc || raw?.nextData?.upc || raw?.specData?.upc);
  const brandSpec = cleanValue(raw?.specData?.brand);

  let source = null;
  if (cleanValue(raw?.jsonLd?.model_number)) source = "jsonld";
  else if (cleanValue(raw?.nextData?.model_number)) source = "next";
  else if (cleanValue(raw?.specData?.model_number)) source = "dom";

  return {
    model_number: modelNumber,
    model_number_norm: normalizeCode(modelNumber),
    part_number: partNumber,
    part_number_norm: normalizeCode(partNumber),
    brand: brand || brandSpec,
    upc,
    source,
    specs: raw?.specData?.pairs || [],
  };
}
