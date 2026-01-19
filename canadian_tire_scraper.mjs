
#!/usr/bin/env node
import { chromium } from "playwright";
import fs from "fs-extra";
import slugify from "slugify";
import { createObjectCsvWriter } from "csv-writer";
import minimist from "minimist";

const args = minimist(process.argv.slice(2));
const STORE_ID = args.store || args.s || null;
const CITY = args.city || "";
const HEADLESS = args.headless !== false;
const MAX_PAGES = args.pages ? Number(args.pages) : null;

const citySlug = CITY ? `-${slugify(CITY, { lower: true, strict: true })}` : "";
const OUT_BASE = `./outputs/canadiantire/${STORE_ID || "default"}${citySlug}`;
const OUT_JSON = `${OUT_BASE}/data.json`;
const OUT_CSV = `${OUT_BASE}/data.csv`;

const DISCOUNT_FILTER = ["50-59", "60-69", "70-79", "80-89", "90-100"];
const BASE_URL = "https://www.canadiantire.ca/en/search-results.html";

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const buildUrl = (page = 1) => {
  const refinements = DISCOUNT_FILTER.map((range) => `discount_percent%3A${range}`).join("%3A");
  const params = new URLSearchParams({
    q: "*",
    openfacetrefinements: refinements,
    store: STORE_ID || "",
    page: String(page),
  });
  return `${BASE_URL}?${params.toString()}`;
};

const parsePrice = (value = "") => {
  const numeric = value.replace(/[^0-9.,]/g, "").replace(",", ".");
  const parsed = Number.parseFloat(numeric);
  return Number.isFinite(parsed) ? parsed : null;
};

const scrapePage = async (page, pageNumber) => {
  const url = buildUrl(pageNumber);
  console.log(`Navigating to ${url}`);
  await page.goto(url, { waitUntil: "networkidle" });
  await page.waitForLoadState("domcontentloaded");

  const cards = await page.$$("[data-test='product-tile']");
  const items = [];

  for (const card of cards) {
    const title = (await card.$eval("[data-test='product-title']", (el) => el.textContent?.trim() || "").catch(() => "")).trim();
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
      storeId: STORE_ID,
      city: CITY,
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

const writeCsv = async (records) => {
  const csvWriter = createObjectCsvWriter({
    path: OUT_CSV,
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

const main = async () => {
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
      const { items, hasNext: next } = await scrapePage(page, pageNumber);
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
    console.error("Error during scrape", error);
  } finally {
    await browser.close();
  }

  await fs.ensureDir(OUT_BASE);
  await fs.writeJson(OUT_JSON, results, { spaces: 2 });
  await writeCsv(results);

  console.log(`Saved ${results.length} items to ${OUT_JSON} and ${OUT_CSV}`);
};

main();
