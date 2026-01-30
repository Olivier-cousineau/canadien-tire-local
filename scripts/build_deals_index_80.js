import fs from "fs/promises";
import path from "path";

const OUTPUT_PATH = path.join(process.cwd(), "public", "index", "deals-80.json");
const IGNORE_DIRS = new Set([".git", "node_modules", "public"]);
const ROOT_DIR = process.cwd();

const CANDIDATE_ROOTS = [
  "outputs",
  "output",
  "results",
  "result",
  path.join("data", "outputs"),
  path.join("data", "output"),
  path.join("data", "results"),
  path.join("data", "result"),
];

const readDirSafe = async (dirPath) => {
  try {
    return await fs.readdir(dirPath, { withFileTypes: true });
  } catch (error) {
    if (error && error.code === "ENOENT") {
      return [];
    }
    throw error;
  }
};

const exists = async (targetPath) => {
  try {
    await fs.access(targetPath);
    return true;
  } catch (error) {
    if (error && error.code === "ENOENT") {
      return false;
    }
    throw error;
  }
};

const getDynamicRoots = async () => {
  const roots = new Set();
  const rootEntries = await readDirSafe(ROOT_DIR);
  for (const entry of rootEntries) {
    if (!entry.isDirectory()) {
      continue;
    }
    if (IGNORE_DIRS.has(entry.name)) {
      continue;
    }
    if (/^(output|outputs|result|results)$/i.test(entry.name)) {
      roots.add(entry.name);
    }
  }

  const dataDir = path.join(ROOT_DIR, "data");
  if (await exists(dataDir)) {
    const dataEntries = await readDirSafe(dataDir);
    for (const entry of dataEntries) {
      if (!entry.isDirectory()) {
        continue;
      }
      if (/^(output|outputs|result|results)$/i.test(entry.name)) {
        roots.add(path.join("data", entry.name));
      }
    }
  }

  return Array.from(roots);
};

const collectJsonFiles = async (dirPath) => {
  const entries = await readDirSafe(dirPath);
  const files = [];

  for (const entry of entries) {
    const entryPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      if (IGNORE_DIRS.has(entry.name)) {
        continue;
      }
      const nested = await collectJsonFiles(entryPath);
      files.push(...nested);
      continue;
    }

    if (entry.isFile() && entry.name.toLowerCase().endsWith(".json")) {
      files.push(entryPath);
    }
  }

  return files;
};

const toNumber = (value) => {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = Number.parseFloat(String(value));
  return Number.isNaN(parsed) ? null : parsed;
};

const getValue = (item, keys) => {
  for (const key of keys) {
    if (item && Object.prototype.hasOwnProperty.call(item, key)) {
      return item[key];
    }
  }
  return null;
};

const extractItems = (data) => {
  if (Array.isArray(data)) {
    return data;
  }
  if (!data || typeof data !== "object") {
    return [];
  }

  const candidateArrays = [
    data.items,
    data.results,
    data.data,
    data.products,
    data.deals,
  ];

  for (const candidate of candidateArrays) {
    if (Array.isArray(candidate)) {
      return candidate;
    }
  }

  const values = Object.values(data);
  if (values.length > 0 && values.every((value) => Array.isArray(value))) {
    return values.flat();
  }

  return [];
};

const normalizeItem = (item) => {
  const name = getValue(item, ["name", "title", "productName"]) || "";
  const sku = getValue(item, ["sku", "productId", "id", "skuId"]) || "";
  const storeId = getValue(item, ["storeId", "store", "locationId", "store_id"]);
  const city = getValue(item, ["city", "storeCity", "locationCity"]) || "";
  const regularPrice = toNumber(
    getValue(item, [
      "regularPrice",
      "regular_price",
      "originalPrice",
      "listPrice",
      "regular",
    ])
  );
  const salePrice = toNumber(
    getValue(item, [
      "salePrice",
      "sale_price",
      "price",
      "currentPrice",
      "sale",
    ])
  );
  const discountPctRaw = toNumber(
    getValue(item, [
      "discountPct",
      "discount_pct",
      "discountPercent",
      "discount",
      "pctOff",
    ])
  );
  const availability = getValue(item, [
    "availability",
    "availabilityStatus",
    "stockStatus",
    "stock",
  ]);
  const image = getValue(item, ["image", "imageUrl", "image_url", "img"]);
  const url = getValue(item, ["url", "productUrl", "product_url", "link"]);

  let discountPct = discountPctRaw;
  if (discountPct === null && regularPrice !== null && salePrice !== null && regularPrice > 0) {
    discountPct = Math.round(((regularPrice - salePrice) / regularPrice) * 100);
  }

  return {
    name,
    sku,
    storeId: storeId === null ? null : Number.parseInt(storeId, 10),
    city,
    regularPrice,
    salePrice,
    discountPct,
    availability: availability ?? "",
    image: image ?? "",
    url: url ?? "",
  };
};

const sortItems = (items) => {
  return items.sort((a, b) => {
    if (a.discountPct !== b.discountPct) {
      return (b.discountPct ?? 0) - (a.discountPct ?? 0);
    }
    if (a.sku !== b.sku) {
      return String(a.sku).localeCompare(String(b.sku));
    }
    if (a.storeId !== b.storeId) {
      return (a.storeId ?? 0) - (b.storeId ?? 0);
    }
    if (a.salePrice !== b.salePrice) {
      return (a.salePrice ?? 0) - (b.salePrice ?? 0);
    }
    if (a.name !== b.name) {
      return String(a.name).localeCompare(String(b.name));
    }
    return String(a.url).localeCompare(String(b.url));
  });
};

const buildDealsIndex = async () => {
  const dynamicRoots = await getDynamicRoots();
  const candidateRoots = Array.from(new Set([...CANDIDATE_ROOTS, ...dynamicRoots]));
  const jsonFiles = [];

  for (const root of candidateRoots) {
    const rootPath = path.join(ROOT_DIR, root);
    if (await exists(rootPath)) {
      const files = await collectJsonFiles(rootPath);
      jsonFiles.push(...files);
    }
  }

  const items = [];
  const dedupe = new Set();
  let latestMtimeMs = 0;

  for (const filePath of jsonFiles) {
    const stat = await fs.stat(filePath);
    if (stat.mtimeMs > latestMtimeMs) {
      latestMtimeMs = stat.mtimeMs;
    }

    let parsed;
    try {
      parsed = JSON.parse(await fs.readFile(filePath, "utf8"));
    } catch (error) {
      console.warn(`Skipping invalid JSON: ${filePath}`);
      continue;
    }

    for (const rawItem of extractItems(parsed)) {
      const normalized = normalizeItem(rawItem);
      if (normalized.discountPct === null || normalized.discountPct < 80) {
        continue;
      }

      const dedupeKey = [
        normalized.sku || normalized.name || "unknown",
        normalized.storeId ?? "unknown",
        normalized.salePrice ?? "unknown",
        normalized.regularPrice ?? "unknown",
      ].join("|");
      if (dedupe.has(dedupeKey)) {
        continue;
      }

      dedupe.add(dedupeKey);
      items.push(normalized);
    }
  }

  const sortedItems = sortItems(items);
  const generatedAt = latestMtimeMs
    ? new Date(latestMtimeMs).toISOString()
    : new Date(0).toISOString();

  return {
    generatedAt,
    count: sortedItems.length,
    items: sortedItems,
  };
};

const main = async () => {
  const outputDir = path.dirname(OUTPUT_PATH);
  await fs.mkdir(outputDir, { recursive: true });

  const output = await buildDealsIndex();
  const serialized = `${JSON.stringify(output, null, 2)}\n`;
  await fs.writeFile(OUTPUT_PATH, serialized, "utf8");
  console.log(`Wrote ${output.count} deals to ${OUTPUT_PATH}`);
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
