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
      const labels = new Set([
        "numero de modele",
        "numero de modèle",
        "no. de modele",
        "no. de modèle",
        "no de modele",
        "no de modèle",
        "n° modele",
        "n° modèle",
        "numero du modele",
        "numero du modèle",
        "model number",
        "mpn",
        "part number",
      ]);

      const matchesLabel = (text) => {
        const normalized = normalizeText(text);
        if (!normalized) return false;
        for (const label of labels) {
          if (normalized === label || normalized.startsWith(label)) {
            return true;
          }
        }
        return false;
      };

      const extractFromDl = () => {
        const dts = Array.from(document.querySelectorAll("dt"));
        for (const dt of dts) {
          if (!matchesLabel(dt.textContent || "")) continue;
          const dd = dt.nextElementSibling;
          const value = dd ? cleanValue(dd.textContent) : null;
          if (value) return value;
        }
        return null;
      };

      const extractFromTable = () => {
        const rows = Array.from(document.querySelectorAll("table tr"));
        for (const row of rows) {
          const cells = Array.from(row.querySelectorAll("th, td"));
          if (cells.length < 2) continue;
          if (!matchesLabel(cells[0].textContent || "")) continue;
          const value = cleanValue(cells[1].textContent);
          if (value) return value;
        }
        return null;
      };

      const extractFromTextBlocks = () => {
        const nodes = Array.from(document.querySelectorAll("li, p"));
        for (const node of nodes) {
          const text = node.textContent || "";
          if (!text.includes(":")) continue;
          const [label, ...rest] = text.split(":");
          if (!matchesLabel(label)) continue;
          const value = cleanValue(rest.join(":"));
          if (value) return value;
        }
        return null;
      };

      const extractFromKeyValueBlocks = () => {
        const nodes = Array.from(
          document.querySelectorAll("dt, th, span, p, div, strong, label")
        );
        for (const node of nodes) {
          const labelText = node.textContent || "";
          if (!labelText || labelText.length > 80) continue;
          if (!matchesLabel(labelText)) continue;
          const next = node.nextElementSibling;
          const nextValue = next ? cleanValue(next.textContent) : null;
          if (nextValue) return nextValue;
          const parent = node.parentElement;
          if (parent) {
            const siblings = Array.from(parent.children);
            const index = siblings.indexOf(node);
            for (let i = index + 1; i < siblings.length; i += 1) {
              const value = cleanValue(siblings[i].textContent);
              if (value) return value;
            }
          }
        }
        return null;
      };

      return (
        extractFromDl()
        || extractFromTable()
        || extractFromTextBlocks()
        || extractFromKeyValueBlocks()
      );
    };

    const jsonLd = extractJsonLd();
    const nextData = extractNextData();
    const specModel = extractFromSpecs();

    return {
      jsonLd,
      nextData,
      specModel: specModel || null,
    };
  });

  const modelNumber = cleanValue(
    raw?.jsonLd?.model_number
      || raw?.nextData?.model_number
      || raw?.specModel
  );
  const brand = cleanValue(raw?.jsonLd?.brand || raw?.nextData?.brand);
  const upc = cleanValue(raw?.jsonLd?.upc || raw?.nextData?.upc);

  let source = null;
  if (cleanValue(raw?.jsonLd?.model_number)) source = "jsonld";
  else if (cleanValue(raw?.nextData?.model_number)) source = "next";
  else if (cleanValue(raw?.specModel)) source = "dom";

  return {
    model_number: modelNumber,
    model_number_norm: normalizeCode(modelNumber),
    brand,
    upc,
    source,
  };
}
