#!/usr/bin/env node
import { chromium } from "playwright";
import fs from "fs-extra";
import slugify from "slugify";
import { createObjectCsvWriter } from "csv-writer";
import minimist from "minimist";
import path from "path";
import { fileURLToPath } from "url";
import pLimit from "p-limit";

const args = minimist(process.argv.slice(2), {
  string: ["store", "city", "concurrency", "pages"],
});
const STORE_ID = args.store || args.s || null;
const CITY = args.city || "";
const HEADLESS = args.headless !== false;
const MAX_PAGES = args.pages ? Number(args.pages) : null;
const CONCURRENCY = Number.isFinite(Number(args.concurrency))
  ? Math.max(1, Number(args.concurrency))
  : 4;

const DISCOUNT_FILTER = ["50-59", "60-69", "70-79", "80-89", "90-100"];
const BASE_URL = "https://www.canadiantire.ca/en/search-results.html";

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const storesFilePath = path.join(__dirname, "data", "canadian_tire_stores.json");

const loadStores = async () => {
  if (!(await fs.pathExists(storesFilePath))) {
    return [];
  }
  return fs.readJson(storesFilePath);
};

const buildUrl = (storeId, page = 1) => {
  const refinements = DISCOUNT_FILTER.map((range) => `discount_percent%3A${range}`).join("%3A");
  const params = new URLSearchParams({
    q: "*",
    openfacetrefinements: refinements,
    store: storeId || "",
    page: String(page),
  });
  return `${BASE_URL}?${params.toString()}`;
};

const parsePrice = (value = "") => {
  const numeric = value.replace(/[^0-9.,]/g, "").replace(",", ".");
  const parsed = Number.parseFloat(numeric);
  return Number.isFinite(parsed) ? parsed : null;
};

const scrapePage = async (page, pageNumber, storeId, city) => {
  const url = buildUrl(storeId, pageNumber);
  console.log(`Navigating to ${url}`);
  await page.goto(url, { waitUntil: "networkidle" });
  await page.waitForLoadState("domcontentloaded");

  const cards = await page.$$('[data-test="product-tile"]');
  const items = [];

  for (const card of cards) {
    const title = (
      await card
        .$eval("[data-test='product-title']", (el) => el.textContent?.trim() || "")
        .catch(() => "")
    ).trim();
    const link = await card.$eval("a", (el) => el.href).catch(() => "");

    const priceText = await card
      .$eval("[data-test='product-price']", (el) => el.textContent || "")
      .catch(() => "");
    const wasPriceText = await card
      .$eval("[data-test='product-price-was']", (el) => el.textContent || "")
      .catch(() => "");

    const price = parsePrice(priceText);
    const wasPrice = parsePrice(wasPriceText);
    const discount = price && wasPrice ? Math.round(((wasPrice - price) / wasPrice) * 100) : null;

    if (!discount || discount < 50) continue;

    items.push({
      storeId,
      city,
      title,
      price,
      wasPrice,
      discount,
      url: link,
      page: pageNumber,
    });
  }

  console.log(`Collected ${items.length} items on page ${pageNumber}`);

  const nextButton = await page.$("button[aria-label='Next'], a[aria-label='Next']");
  const disabled = nextButton ? (await nextButton.getAttribute("aria-disabled")) === "true" : true;

  return {
    items,
    hasNext: Boolean(nextButton) && !disabled,
  };
};

const writeCsv = async (outCsv, records) => {
  const csvWriter = createObjectCsvWriter({
    path: outCsv,
    header: [
      { id: "storeId", title: "STORE_ID" },
      { id: "city", title: "CITY" },
      { id: "title", title: "TITLE" },
      { id: "price", title: "PRICE" },
      { id: "wasPrice", title: "WAS_PRICE" },
      { id: "discount", title: "DISCOUNT" },
      { id: "url", title: "URL" },
      { id: "page", title: "PAGE" },
    ],
  });

  await csvWriter.writeRecords(records);
};

const scrapeStore = async ({ storeId, storeName }) => {
  const citySlug = storeName ? `-${slugify(storeName, { lower: true, strict: true })}` : "";
  const outBase = `./outputs/canadiantire/${storeId || "default"}${citySlug}`;
  const outJson = `${outBase}/data.json`;
  const outCsv = `${outBase}/data.csv`;

  const browser = await chromium.launch({ headless: HEADLESS });
  const context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
  });
  const page = await context.newPage();

  let pageNumber = 1;
  let hasNext = true;
  const results = [];

  try {
    while (hasNext) {
      const { items, hasNext: next } = await scrapePage(page, pageNumber, storeId, storeName || "");
      results.push(...items);
      hasNext = next;
      pageNumber += 1;

      if (MAX_PAGES && pageNumber > MAX_PAGES) {
        console.log("Reached max pages limit, stopping.");
        break;
      }

      if (hasNext) {
        await wait(500);
      }
    }
  } catch (error) {
    console.error(`Error during scrape for store ${storeId}`, error);
  } finally {
    await browser.close();
  }

  await fs.ensureDir(outBase);
  await fs.writeJson(outJson, results, { spaces: 2 });
  await writeCsv(outCsv, results);

  console.log(`Saved ${results.length} items to ${outJson} and ${outCsv}`);
};

const main = async () => {
  let stores = await loadStores();

  if (!stores.length) {
    if (STORE_ID) {
      stores = [{ storeId: STORE_ID, storeName: CITY }];
    } else {
      console.error(`No stores found in ${storesFilePath} and no --store provided.`);
      process.exit(1);
    }
  }

  if (STORE_ID) {
    stores = stores.filter((store) => String(store.storeId ?? "") === String(STORE_ID));
    if (!stores.length) {
      stores = [{ storeId: STORE_ID, storeName: CITY }];
    }
  }

  if (!STORE_ID && CITY) {
    stores = stores.filter((store) => String(store.storeName ?? "").includes(CITY));
  }

  console.log(`Scraping ${stores.length} stores with concurrency ${CONCURRENCY}.`);

  const limit = pLimit(CONCURRENCY);
  await Promise.all(stores.map((store) => limit(() => scrapeStore(store))));
};

main();
