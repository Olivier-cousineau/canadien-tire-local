

// @ts-check
/**
 * Scraper Canadian Tire - Liquidation (Playwright + enrichissement fiche produit)
 * - Multi-magasins via --store <ID> --city "<Nom>"
 * - Titres/prix robustes (aria-label/title/alt, data-*), scroll "lazy"
 * - Enrichissement depuis la liste uniquement (pas de PDP)
 * - Sorties par magasin: outputs/canadiantire/<store>-<city-slug>/data.json
 */
import fs from "fs";
import path from "path";
import { chromium } from "playwright";
import fsExtra from "fs-extra";
import slugify from "slugify";
import minimist from "minimist";
import {
  buildCtKeysFromText,
  makeCtProductKey,
  normalizeCtProductNumber,
} from "./lib/ctProductKey.js";

const args = minimist(process.argv.slice(2), {
  string: ["storeId", "storeName", "outBase", "maxPages"],
  boolean: ["debug", "headful", "downloadImages"],
  default: { maxPages: "120" },
});

const buildCtKeysFromAvailability = (availabilityText) =>
  buildCtKeysFromText(availabilityText);

const STORES_PATH = path.join(process.cwd(), "data", "canadian_tire_stores.json");
let allStores = [];
if (fs.existsSync(STORES_PATH)) {
  allStores = JSON.parse(fs.readFileSync(STORES_PATH, "utf8"));
} else {
  console.warn(`[SCRAPER] Fichier introuvable: ${STORES_PATH}`);
}

const rawShardIndex = process.env.SHARD_INDEX;
const rawTotalShards = process.env.TOTAL_SHARDS;

// On considère que SHARD_INDEX est 1-based (1,2,...,TOTAL_SHARDS)
const shardIndex = rawShardIndex ? parseInt(rawShardIndex, 10) : 0;
const totalShards = rawTotalShards ? parseInt(rawTotalShards, 10) : 0;
let stopRequested = false;

let storesToProcess = allStores;
if (storesToProcess.length === 0) {
  const fallbackStoreId = args.storeId || args.store || null;
  if (fallbackStoreId) {
    storesToProcess = [
      {
        storeId: String(fallbackStoreId),
        storeName: String(args.storeName || args.city || ""),
      },
    ];
    console.log(
      `[SCRAPER] Fallback local → storeId=${fallbackStoreId} storeName=${args.storeName || args.city || ""}`
    );
  } else {
    throw new Error(
      `[SCRAPER] Impossible de démarrer sans liste de magasins. Fournir --storeId et --storeName ou ajouter ${STORES_PATH}.`
    );
  }
}

const baseStores = allStores.length ? allStores : storesToProcess;

if (!Number.isFinite(shardIndex) || !Number.isFinite(totalShards) || totalShards <= 0) {
  console.log("[SHARD] Pas de sharding –", baseStores.length, "magasins.");
  storesToProcess = baseStores;
} else {
  // Nombre max de magasins par shard (≈8 si 340 magasins / 43 shards)
  const maxPerShard = Math.ceil(baseStores.length / totalShards);

  // Sécurité : on limite à 8 magasins par shard même si totalShards change
  const storesPerShard = Math.min(maxPerShard, 8);

  // shardIndex est 1-based, on convertit en 0-based
  const zeroBasedIndex = Math.max(0, shardIndex - 1);

  const start = zeroBasedIndex * storesPerShard;
  const end = Math.min(start + storesPerShard, baseStores.length);

  storesToProcess = baseStores.slice(start, end);

  console.log(
    `[SHARD] Shard ${shardIndex}/${totalShards} – magasins index ${start} à ${end - 1} (total: ${storesToProcess.length})`
  );
}

console.log(
  `[SCRAPER] ${storesToProcess.length} magasins à traiter, 4 en parallèle.`
);

function hasReachedTimeLimit() {
  if (stopRequested) return true;

  return stopRequested;
}

// TASK FOR CODEX:
// Ensure this scraper always writes JSON to the OUT_BASE passed via CLI,
// and that the paths never depend on a previous store.
//
// 1) Parse the CLI arguments like this:
//    const args = minimist(process.argv.slice(2), {
//      string: ["storeId", "storeName", "outBase", "maxPages"],
//      boolean: ["debug", "headful", "downloadImages"],
//      default: { maxPages: "120" },
//    });
//
// 2) Derive OUT_BASE using only args.outBase when provided:
//    const storeId = String(args.storeId);
//    const storeName = String(args.storeName || "");
//    let OUT_BASE = args.outBase;
//    if (!OUT_BASE) {
//      const citySlug = slugify(storeName, { lower: true, strict: true });
//      OUT_BASE = path.join("outputs", "canadiantire", `${storeId}-${citySlug}`);
//    }
//    const jsonPath = path.join(OUT_BASE, "data.json");
//    console.log(`OUT_BASE=${OUT_BASE}`);
//    console.log(`💾  JSON → ${jsonPath}`);
//
// 3) When writing files, always use jsonPath above.
//    Do NOT hard-code "418-rosemere-qc" or any specific store folder.
//
// 4) If there is a git commit step inside this file, make sure the commit message uses:
//      storeName and storeId from the current args,
//    e.g. "Canadian Tire: St. Eustache, QC (218) – XXX produits".
//
// With these changes, each run of the scraper will correctly write to the folder matching
// the current store (including 218 St. Eustache), and will not reuse Rosemere's paths.

const storeFilter = args.store || args.storeId || null;
if (storeFilter) {
  const storeFilterNorm = normalizeStoreId(storeFilter);
  storesToProcess = storesToProcess.filter((store) => {
    const candidate = normalizeStoreId(store.storeId);
    return candidate === storeFilterNorm;
  });
  console.log(`[SCRAPER] Filtre CLI – store=${storeFilter} → ${storesToProcess.length} magasin(s).`);
}

function parseBooleanArg(value, defaultValue = false) {
  if (value === undefined) return defaultValue;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["false","0","no","off"].includes(normalized)) return false;
    if (["true","1","yes","on"].includes(normalized)) return true;
  }
  return defaultValue;
}

function normalizeStoreId(value) {
  if (value == null) return "";
  return String(value).replace(/^0+/, "");
}

// ---------- CLI ----------
const DEFAULT_BASE = "https://www.canadiantire.ca/fr/promotions/liquidation.html";

const HEADLESS  = !args.headful;

const INCLUDE_REGULAR_PRICE    = parseBooleanArg(args["include-regular-price"] ?? args.includeRegularPrice, true);
const INCLUDE_LIQUIDATION_PRICE= parseBooleanArg(args["include-liquidation-price"] ?? args.includeLiquidationPrice, true);

// === Helpers & Sélecteurs ===
const BASE = "https://www.canadiantire.ca";

const SELECTORS = {
  card: [
    "li[data-testid='product-grids']",
    "article:has(a[href*='/p/'])",
    ".nl-product-card",
  ].join(", "),
};

const CARD_COUNT_SELECTOR = [
  "[data-testid*='product-card']",
  "article",
  ".product-card",
  "[class*='productCard']",
  "li[data-testid='product-grids']",
  ".nl-product-card",
].join(", ");

const NETWORKIDLE_RACE_TIMEOUT_MS = 6000;

async function waitForNetworkIdleOrTimeout(page, label = "") {
  const suffix = label ? ` (${label})` : "";
  console.log(`[AWAIT] ▶ waitForLoadState(networkidle)${suffix}`);
  try {
    await Promise.race([
      page.waitForLoadState("networkidle"),
      page.waitForTimeout(NETWORKIDLE_RACE_TIMEOUT_MS),
    ]);
    console.log(`[AWAIT] ✓ waitForLoadState(networkidle)${suffix}`);
  } catch (err) {
    console.warn(
      `[AWAIT] ✗ waitForLoadState(networkidle)${suffix}: ${err?.message || err}`
    );
  }
}

async function withAwaitLog(label, action) {
  console.log(`[AWAIT] ▶ ${label}`);
  try {
    const result = await action();
    console.log(`[AWAIT] ✓ ${label}`);
    return result;
  } catch (err) {
    console.warn(`[AWAIT] ✗ ${label}: ${err?.message || err}`);
    throw err;
  }
}

function loggedPromise(label, promise) {
  console.log(`[AWAIT] ▶ ${label}`);
  return promise
    .then((result) => {
      console.log(`[AWAIT] ✓ ${label}`);
      return result;
    })
    .catch((err) => {
      console.warn(`[AWAIT] ✗ ${label}: ${err?.message || err}`);
      throw err;
    });
}

function createTimeoutPromise(ms, label = "timeout") {
  return new Promise((_, reject) => {
    const timer = setTimeout(() => {
      const err = new Error(`Timeout after ${ms}ms (${label})`);
      err.name = "TimeoutError";
      reject(err);
    }, ms);
    timer.unref?.();
  });
}

const LOAD_MORE_SELECTORS = [
  "button:has-text('Charger plus')",
  "button:has-text('Load more')",
  "a:has-text('Charger plus')",
  "a:has-text('Load more')",
  "[data-testid*='load-more']",
  "[data-testid*='LoadMore']",
].join(", ");

const PAGINATION_NAV_SELECTOR = [
  "nav[aria-label*='pagination' i]",
  "nav[aria-label*='Pagination' i]",
  "[data-testid='pagination']",
  "[data-testid='pagination-container']",
  "nav[role='navigation']:has([aria-current])",
].join(", ");

const PRICE_SELECTORS = {
  sale: [
    "span[data-testid='priceTotal']",
    "[data-testid='sale-price']",
    ".nl-price--total",
    ".nl-price__total",
    ".price__value",
    ".c-pricing__current",
  ],
  regular: [
    "[data-testid='regular-price']",
    ".nl-price__was s",
    ".nl-price--was",
    ".nl-price__was",
    "del",
    "s",
  ],
  priceContainer: [
    "[data-testid='price']",
    "[data-testid='pricing']",
    ".nl-price",
    ".nl-price__container",
    ".c-pricing",
    ".price",
    ".price__value",
    ".nl-price__text",
  ],
};

const SEL = {
  card: "li[data-testid=\"product-grids\"]",
  price: PRICE_SELECTORS.sale.join(", "),
  paginationNav: PAGINATION_NAV_SELECTOR,
  currentPage: `${PAGINATION_NAV_SELECTOR} [aria-current], ${PAGINATION_NAV_SELECTOR} [aria-current=\"page\"]`,
};

const cleanMoney = (s) => {
  if (!s) return null;
  s = s.replace(/\u00a0/g, " ").trim();
  const m = s.match(/(\d[\d\s.,]*)(?:\s*\$)?/);
  return m ? m[1].replace(/\s/g, "") : s;
};

async function dismissMedalliaPopup(page) {
  try {
    console.log("[AWAIT] ▶ closeMedallia: locate buttons");
    const possibleCloseButtons = page.locator(
      [
        '#kampyleInviteContainer button',
        '#MDigitalInvitationWrapper button',
        'button[aria-label*="close" i]',
        'button[aria-label*="fermer" i]',
        'button[aria-label*="feedback" i]'
      ].join(', ')
    );

    const count = await possibleCloseButtons.count();
    console.log("[AWAIT] ✓ closeMedallia: locate buttons");
    for (let i = 0; i < count; i++) {
      const btn = possibleCloseButtons.nth(i);
      console.log(`[AWAIT] ▶ closeMedallia: check visibility (${i + 1}/${count})`);
      if (await btn.isVisible().catch(() => false)) {
        console.log(`[AWAIT] ✓ closeMedallia: visible (${i + 1}/${count})`);
        console.log('🧹 Medallia: clic sur le bouton de fermeture');
        console.log("[AWAIT] ▶ closeMedallia: click");
        await btn.click({ timeout: 2000 }).catch(() => {});
        console.log("[AWAIT] ✓ closeMedallia: click");
        break;
      }
    }

    console.log("[AWAIT] ▶ closeMedallia: remove nodes");
    await page.evaluate(() => {
      const ids = ['MDigitalInvitationWrapper', 'kampyleInviteContainer', 'kampyleInvite'];
      for (const id of ids) {
        const el = document.getElementById(id);
        if (el) {
          console.log('🧹 Medallia: suppression/masquage de', id);
          el.remove();
        }
      }

      ids.forEach((id) => {
        const el = document.getElementById(id);
        if (el) {
          el.style.setProperty('display', 'none', 'important');
          el.style.setProperty('pointer-events', 'none', 'important');
        }
      });
    });
    console.log("[AWAIT] ✓ closeMedallia: remove nodes");
  } catch (e) {
    console.warn('⚠️ Impossible de fermer le pop-up Medallia:', e);
  }
}

async function closeCookieBanner(page) {
  const selectors = [
    "button:has-text('Accepter')",
    "button:has-text('Accepter tout')",
    "button:has-text('Tout accepter')",
    "button:has-text('Accept')",
    "button:has-text('Accept all')",
    "button[aria-label*='accepter' i]",
    "button[aria-label*='accept' i]",
  ];
  for (const sel of selectors) {
    try {
      const loc = page.locator(sel).first();
      console.log(`[AWAIT] ▶ closeCookieBanner: check ${sel}`);
      if (await loc.isVisible().catch(() => false)) {
        console.log(`[AWAIT] ✓ closeCookieBanner: visible ${sel}`);
        console.log("[AWAIT] ▶ closeCookieBanner: click");
        await loc.click({ timeout: 2000 }).catch(() => {});
        console.log("[AWAIT] ✓ closeCookieBanner: click");
        console.log("[AWAIT] ▶ closeCookieBanner: wait 300ms");
        await page.waitForTimeout(300);
        console.log("[AWAIT] ✓ closeCookieBanner: wait 300ms");
        break;
      }
    } catch {}
  }
}

async function closeInterferingPopups(page) {
  console.log("[AWAIT] ▶ closeInterferingPopups");
  await Promise.allSettled([
    dismissMedalliaPopup(page),
    closeCookieBanner(page),
    maybeCloseStoreModal(page),
  ]);
  console.log("[AWAIT] ✓ closeInterferingPopups");
}

function hasPageParam(urlStr, pageNum) {
  try {
    const url = new URL(urlStr, BASE);
    return url.searchParams.get("page") === String(pageNum);
  } catch {
    return false;
  }
}

async function logNavigationDebug(page, { pageNum, responseStatus } = {}) {
  const finalUrl = page.url();
  const documentUrl = await page.evaluate(() => document.location.href).catch(() => null);
  console.log(`[NAV] Final URL after goto: ${finalUrl}`);
  console.log(`[NAV] document.location.href: ${documentUrl ?? "n/a"}`);
  if (responseStatus != null) {
    console.log(`[NAV] HTTP status: ${responseStatus}`);
  }
  const hasParam = hasPageParam(finalUrl, pageNum);
  console.log(`[NAV] page param present: ${hasParam}`);
  return { finalUrl, documentUrl, hasParam };
}

async function savePageDebugArtifacts(page, debugDir, { pageNum, label, responseStatus } = {}) {
  if (!debugDir) return;
  await fsExtra.ensureDir(debugDir);
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const baseName = `page-${pageNum}-${label || "debug"}-${timestamp}`;
  const screenshotPath = path.join(debugDir, `${baseName}.png`);
  const htmlPath = path.join(debugDir, `${baseName}.html`);
  const logPath = path.join(debugDir, `${baseName}.log`);

  const finalUrl = page.url();
  const hasPage = hasPageParam(finalUrl, pageNum);
  const documentUrl = await page.evaluate(() => document.location.href).catch(() => null);
  const logPayload = [
    `pageNum=${pageNum}`,
    `label=${label || "debug"}`,
    `url=${finalUrl}`,
    `documentUrl=${documentUrl ?? "n/a"}`,
    `hasPageParam=${hasPage}`,
    `status=${responseStatus ?? "n/a"}`,
  ].join("\n");

  await Promise.all([
    page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {}),
    page.content().then((content) => fsExtra.writeFile(htmlPath, content)).catch(() => {}),
    fsExtra.writeFile(logPath, logPayload).catch(() => {}),
  ]);
}

async function savePagePriceDebugArtifacts(page, debugDir, pageNum) {
  if (!debugDir) return;
  await fsExtra.ensureDir(debugDir);
  const baseName = `page${pageNum}-no-prices`;
  const screenshotPath = path.join(debugDir, `${baseName}.png`);
  const htmlPath = path.join(debugDir, `${baseName}.html`);
  const urlPath = path.join(debugDir, `${baseName}.url.txt`);
  const finalUrl = page.url();

  await Promise.all([
    page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {}),
    page.content().then((content) => fsExtra.writeFile(htmlPath, content)).catch(() => {}),
    fsExtra.writeFile(urlPath, finalUrl).catch(() => {}),
  ]);
}

async function waitProductsStableWithRetries(page, {
  timeout = 60000,
  retries = 2,
  pageNum = 1,
  debugDir,
  responseStatus,
} = {}) {
  for (let attempt = 1; attempt <= retries + 1; attempt += 1) {
    const ok = await waitProductsStable(page, timeout);
    if (ok) return true;
    console.warn(`[PAGINATION] Produits non stables (tentative ${attempt}/${retries + 1}).`);
    if (attempt <= retries) {
      await withAwaitLog("waitProductsStableWithRetries wait 2000ms", () =>
        page.waitForTimeout(2000)
      );
      await withAwaitLog("waitProductsStableWithRetries closeInterferingPopups", () =>
        closeInterferingPopups(page)
      );
    } else if (debugDir) {
      await savePageDebugArtifacts(page, debugDir, {
        pageNum,
        label: "wait-products-failed",
        responseStatus,
      });
    }
  }
  return false;
}
async function waitProductsStable(page, timeout = 60000) {
  try {
    await withAwaitLog(
      `waitForSelector cards attached (timeout=${timeout})`,
      () =>
        page.waitForSelector(SELECTORS.card, {
          state: "attached",
          timeout,
        })
    );

    await withAwaitLog("waitForTimeout 300ms (products stable)", () =>
      page.waitForTimeout(300)
    );
    return true;
  } catch (err) {
    console.warn(
      `[waitProductsStable] Impossible de stabiliser les produits : ${err.message}`
    );
    return false;
  }
}

async function waitForRealCards(page, { timeout = 60000, minRealCards = 5 } = {}) {
  const checkPlaceholder = async () => {
    return page.evaluate((cardSelector) => {
      const cards = Array.from(document.querySelectorAll(cardSelector));
      const cardsDetected = cards.length;
      const realCards = cards.filter((card) => {
        const linkEl = card.querySelector("a[href*='/p/'], a[href*='/product/'], a[href*='/produit/']");
        const href = linkEl ? linkEl.getAttribute("href") || "" : "";
        const sku =
          card.getAttribute("data-sku") ||
          card.getAttribute("data-product-id") ||
          card.getAttribute("data-productid") ||
          "";
        return Boolean(href) || Boolean(sku);
      });
      return { cardsDetected, realCards: realCards.length };
    }, SELECTORS.card);
  };

  let placeholderReloaded = false;
  const start = Date.now();

  while (Date.now() - start < timeout) {
    const { cardsDetected, realCards } = await checkPlaceholder();
    if (realCards >= minRealCards) return true;

    if (cardsDetected <= 2 && realCards === 0) {
      if (!placeholderReloaded) {
        placeholderReloaded = true;
        console.warn("[waitForRealCards] Placeholder détecté → reload unique.");
        await withAwaitLog("reload page (waitForRealCards placeholder)", () =>
          page.reload({ waitUntil: "domcontentloaded" }).catch(() => {})
        );
        await waitForNetworkIdleOrTimeout(page, "after reload placeholder (waitForRealCards)");
        continue;
      }
      console.warn("[waitForRealCards] Placeholder persistant, arrêt propre.");
      return false;
    }

    await page.waitForTimeout(800);
  }

  console.warn("[waitForRealCards] Timeout/erreur: seuil non atteint.");
  return false;
}

async function getRealCardStats(page) {
  try {
    return await page.evaluate((cardSelector) => {
      const cards = Array.from(document.querySelectorAll(cardSelector));
      const realCards = cards.filter((card) => {
        const linkEl = card.querySelector(
          "a[href*='/p/'], a[href*='/product/'], a[href*='/produit/']"
        );
        const href = linkEl ? linkEl.getAttribute("href") || "" : "";
        const dataSku =
          card.getAttribute("data-sku") ||
          card.getAttribute("data-product-sku") ||
          "";
        return Boolean(href) || Boolean(dataSku?.trim());
      });
      return {
        cardsDetected: cards.length,
        realCards: realCards.length,
      };
    }, SELECTORS.card);
  } catch {
    return { cardsDetected: 0, realCards: 0 };
  }
}

function findProductNumberInText(value) {
  if (!value) return null;
  const str = String(value);
  const formattedMatch = str.match(/#?\s*(\d{3}-\d{4}-\d)\b/);
  if (formattedMatch) return formattedMatch[0];
  const digitsMatch = str.match(/\b(\d{8})\b/);
  return digitsMatch ? digitsMatch[1] : null;
}

function extractCtProductNumberRaw(...candidates) {
  for (const candidate of candidates) {
    const found = findProductNumberInText(candidate);
    if (found) return found;
  }
  return null;
}

function buildProductIdentifiers(card) {
  const identifiers = [];

  const productKey = card.product_key || makeCtProductKey(card.product_number);
  if (productKey) identifiers.push(productKey);

  if (card.product_id) identifiers.push(`id:${card.product_id}`);

  if (card.link) {
    const normalizedLink = card.link.split("?")[0].toLowerCase();
    identifiers.push(`href:${normalizedLink}`);
  }

  return identifiers.filter(Boolean);
}

async function extractFromCard(card) {
  const data = await card.evaluate((el, { base }) => {
    const cleanMoney = (s) => {
      if (!s) return null;
      s = s.replace(/\u00a0/g, " ").trim();
      const m = s.match(/(\d[\d\s.,]*)(?:\s*\$)?/);
      return m ? m[1].replace(/\s/g, "") : s;
    };

    const textFromEl = (node) => {
      if (!node) return null;
      const t = node.textContent;
      return t ? t.trim() : null;
    };

    const titleEl = el.querySelector("[id^='title__promolisting-'], .nl-product-card__title");
    const title = textFromEl(titleEl);

    const imgEl = el.querySelector(".nl-product-card__image-wrap img");
    let image = null;
    if (imgEl) image = imgEl.getAttribute("src") || imgEl.getAttribute("data-src");
    if (image && image.startsWith("//")) image = `https:${image}`;
    if (image && image.startsWith("/")) image = base + image;

    const availability = textFromEl(el.querySelector(".nl-product-card__availability-message"));

    const badges = Array.from(el.querySelectorAll(".nl-plp-badges"))
      .map((node) => textFromEl(node))
      .filter(Boolean);

    const primaryAnchor = el.querySelector("a.nl-product-card__no-button.prod-link");
    let link = primaryAnchor ? primaryAnchor.getAttribute("href") : null;
    const titleAnchor = titleEl ? titleEl.closest("a") : null;
    if (!link && titleAnchor) link = titleAnchor.getAttribute("href");
    if (!link) {
      const any = el.querySelector("a[href*='/p/'], a[href*='/product/']");
      if (any) link = any.getAttribute("href");
    }
    if (link && link.startsWith("/")) link = base + link;

    const productId = el.getAttribute("data-product-id") || el.getAttribute("data-productid") || null;
    const extractProductNumberRaw = (source) => {
      if (!source) return null;
      const str = String(source);
      const formatted = str.match(/#?\s*(\d{3}-\d{4}-\d)\b/);
      if (formatted) return formatted[0];
      const digits = str.match(/\b(\d{8})\b/);
      return digits ? digits[1] : null;
    };

    let productNumberRaw = null;
    let sku = null;
    let skuFormatted = null;
    if (primaryAnchor) {
      const href = primaryAnchor.getAttribute("href") || "";
      const ariaLabelledby = primaryAnchor.getAttribute("aria-labelledby") || "";
      const skuMatch = href.match(/-([0-9]{7})p\.html/i);
      const skuFormattedMatch = ariaLabelledby.match(/promolisting-([0-9-]+)/i);
      if (skuMatch) sku = skuMatch[1];
      if (skuFormattedMatch) skuFormatted = skuFormattedMatch[1];
      productNumberRaw = extractProductNumberRaw(ariaLabelledby) || extractProductNumberRaw(href);
    }
    if (!productNumberRaw) {
      productNumberRaw = extractProductNumberRaw(link) || extractProductNumberRaw(title);
    }

    return {
      name: title || null,
      image: image || null,
      availability: availability || null,
      badges,
      link: link || null,
      product_id: productId,
      sku,
      sku_formatted: skuFormatted,
      product_number_raw: productNumberRaw,
    };
  }, { base: BASE });

  return data;
}

async function scrapeListing(page, { skipGuards = false } = {}) {
  if (!skipGuards) {
    await withAwaitLog("waitForSelector cards (scrapeListing)", () =>
      page.waitForSelector(SELECTORS.card, { timeout: 60000 })
    );
    await withAwaitLog("waitForSelector sale price (scrapeListing)", () =>
      page.waitForSelector(PRICE_SELECTORS.sale.join(", "), { timeout: 20000 })
    ).catch(() => {});
  } else {
    const hasCards = await page.locator(SELECTORS.card).count();
    if (!hasCards) {
      await withAwaitLog("waitForSelector cards (scrapeListing skipGuards)", () =>
        page.waitForSelector(SELECTORS.card, { timeout: 60000 })
      );
    }
  }

  const cardsLocator = page.locator(SELECTORS.card);
  try {
    const items =
      (await cardsLocator.evaluateAll((nodes, { base }) => {
        const cleanMoney = (s) => {
        if (!s) return null;
        s = s.replace(/\u00a0/g, " ").trim();
        const m = s.match(/(\d[\d\s.,]*)(?:\s*\$)?/);
        return m ? m[1].replace(/\s/g, "") : s;
      };

      const textFromEl = (node) => {
        if (!node) return null;
        const t = node.textContent;
        return t ? t.trim() : null;
      };

      const textFromSelectorList = (root, selectors) => {
        for (const sel of selectors) {
          const node = root.querySelector(sel);
          const text = textFromEl(node);
          if (text) return text;
        }
        return null;
      };

      const extractSkuData = (anchor) => {
        if (!anchor) return { sku: null, sku_formatted: null, product_number_raw: null };
        const href = anchor.getAttribute("href") || "";
        const ariaLabelledby = anchor.getAttribute("aria-labelledby") || "";
        const skuMatch = href.match(/-([0-9]{7})p\.html/i);
        const skuFormattedMatch = ariaLabelledby.match(/promolisting-([0-9-]+)/i);
        const productFormattedMatch = ariaLabelledby.match(/#?\s*(\d{3}-\d{4}-\d)\b/);
        const productDigitsMatch = href.match(/\b(\d{8})\b/);
        return {
          sku: skuMatch ? skuMatch[1] : null,
          sku_formatted: skuFormattedMatch ? skuFormattedMatch[1] : null,
          product_number_raw: productFormattedMatch
            ? productFormattedMatch[0]
            : productDigitsMatch
            ? productDigitsMatch[1]
            : null,
        };
      };

      const extractProductNumberRaw = (sources) => {
        for (const source of sources) {
          if (!source) continue;
          const str = String(source);
          const formatted = str.match(/#?\s*(\d{3}-\d{4}-\d)\b/);
          if (formatted) return formatted[0];
          const digits = str.match(/\b(\d{8})\b/);
          if (digits) return digits[1];
        }
        return null;
      };

      return nodes.map((el) => {
      const titleEl = el.querySelector("[id^='title__promolisting-'], .nl-product-card__title");
      const title = textFromEl(titleEl);
      const pricingRoot =
        el.querySelector(
          "[data-testid='price'], [data-testid='pricing'], .nl-price, .nl-price__container, .c-pricing, .price, .price__value, .nl-price__text"
        ) || el;

      const priceSaleRaw = textFromSelectorList(pricingRoot, [
        "span[data-testid='priceTotal']",
        "[data-testid='sale-price']",
        ".nl-price--total",
        ".nl-price__total",
        ".c-pricing__current",
        ".price__value",
      ]);

      const wasSelectors = [
        "[data-testid='regular-price']",
        ".nl-price__was s",
        ".nl-price--was",
        ".nl-price__was",
        "del",
        "s",
      ];
      let priceWasRaw = textFromSelectorList(pricingRoot, wasSelectors);

      const pricingLabel =
        pricingRoot.getAttribute("aria-label") ||
        pricingRoot.getAttribute("title") ||
        null;
      if (!priceWasRaw && pricingLabel) {
        const wasMatch = pricingLabel.match(/(était|was|regular)[^0-9]*([\d\s.,]+)/i);
        if (wasMatch) {
          priceWasRaw = wasMatch[2];
        }
      }

      let priceSaleLabel = null;
      if (!priceSaleRaw && pricingLabel) {
        const saleMatch = pricingLabel.match(/(maintenant|now|sale|price)[^0-9]*([\d\s.,]+)/i);
        if (saleMatch) {
          priceSaleLabel = saleMatch[2];
        }
      }

      if (!priceWasRaw) {
        const datasetValues = Object.values(pricingRoot.dataset || {}).join(" ");
        if (datasetValues) {
          const wasMatch = datasetValues.match(/(était|was|regular)[^0-9]*([\d\s.,]+)/i);
          if (wasMatch) {
            priceWasRaw = wasMatch[2];
          }
        }
      }

      const priceSaleFinalRaw = priceSaleRaw || priceSaleLabel || null;
        const price_sale = cleanMoney(priceSaleFinalRaw);
        const price_original = cleanMoney(priceWasRaw);

        const imgEl = el.querySelector(".nl-product-card__image-wrap img");
        let image = null;
        if (imgEl) image = imgEl.getAttribute("src") || imgEl.getAttribute("data-src");
        if (image && image.startsWith("//")) image = `https:${image}`;
        if (image && image.startsWith("/")) image = base + image;

        const availability = textFromEl(el.querySelector(".nl-product-card__availability-message"));

        const badges = Array.from(el.querySelectorAll(".nl-plp-badges"))
          .map((node) => textFromEl(node))
          .filter(Boolean);

        const primaryAnchor = el.querySelector("a.nl-product-card__no-button.prod-link");
        let link = primaryAnchor ? primaryAnchor.getAttribute("href") : null;
        const titleAnchor = titleEl ? titleEl.closest("a") : null;
        if (!link && titleAnchor) link = titleAnchor.getAttribute("href");
        if (!link) {
          const any = el.querySelector("a[href*='/p/'], a[href*='/product/']");
          if (any) link = any.getAttribute("href");
        }
        if (link && link.startsWith("/")) link = base + link;

        const productId = el.getAttribute("data-product-id") || el.getAttribute("data-productid") || null;
        const { sku, sku_formatted, product_number_raw: anchorProductNumber } = extractSkuData(primaryAnchor);
        const product_number_raw = extractProductNumberRaw([
          anchorProductNumber,
          sku_formatted,
          sku,
          link,
          title,
          primaryAnchor ? primaryAnchor.getAttribute("aria-labelledby") : null,
        ]);

        return {
          name: title || null,
          price_sale,
          price_sale_raw: priceSaleFinalRaw || null,
          price_original,
          price_original_raw: priceWasRaw || null,
          image: image || null,
          availability: availability || null,
          badges,
          link: link || null,
          product_id: productId,
          sku,
          sku_formatted,
          product_number_raw,
        };
      });
    }, { base: BASE })) || [];

    return items;
  } catch (e) {
    console.warn("scrapeListing evaluateAll error:", e?.message || e);
    if (!skipGuards) {
      await withAwaitLog("waitForSelector cards (scrapeListing fallback)", () =>
        page.waitForSelector(SELECTORS.card, { timeout: 20000 })
      ).catch(() => {});
    }
    const cards = page.locator(SELECTORS.card);
    const n = await cards.count();
    const tasks = [];
    for (let i = 0; i < n; i++) {
      const card = cards.nth(i);
      tasks.push(
        extractFromCard(card).catch((err) => {
          console.warn("extractFromCard error:", err?.message || err);
          return null;
        })
      );
    }
    const out = await Promise.all(tasks);
    return out.filter(Boolean);
  }
}

// ---------- UTILS ----------
function extractPrice(text) {
  if (text == null) return null;
  const normalized = String(text);
  const m = normalized.replace(/\s/g, "").match(/(\d+[\.,]?\d*)/);
  return m ? parseFloat(m[1].replace(",", ".")) : null;
}

function computeDiscountPercent(regularPrice, liquidationPrice) {
  if (regularPrice == null || liquidationPrice == null) return null;
  if (regularPrice <= 0 || liquidationPrice <= 0) return null;

  const discount = ((regularPrice - liquidationPrice) / regularPrice) * 100;
  return Number.isFinite(discount) ? discount : null;
}

function normalizeProductUrlForDedup(rawUrl) {
  if (!rawUrl) return null;
  try {
    const parsed = new URL(rawUrl, BASE);
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString().toLowerCase();
  } catch {
    const str = String(rawUrl);
    return str ? str.toLowerCase() : null;
  }
}

async function clickPaginationNext(page, targetPage) {
  const nav = page.locator(SEL.paginationNav).first();
  const navVisible = await nav.isVisible().catch(() => false);
  if (!navVisible) return { clicked: false, reason: "missing-nav" };

  const targetLabel = targetPage ? String(targetPage) : "2";
  const numberTarget = nav
    .locator(
      [
        `a[aria-label*='${targetLabel}']`,
        `button:has-text('${targetLabel}')`,
        `a:has-text('${targetLabel}')`,
      ].join(", ")
    )
    .first();

  const current = nav.locator("[aria-current], [aria-current=\"page\"]").first();
  const nextFromCurrent = current.locator("xpath=following::a[1] | xpath=following::button[1]").first();
  const relNext = page.locator("a[rel='next'], button[rel='next']").first();
  const textNext = nav.locator("a:has-text('Suivant'), button:has-text('Suivant'), a:has-text('Next'), button:has-text('Next')").first();

  const candidates = [
    { locator: numberTarget, reason: "numeric" },
    { locator: textNext, reason: "text-next" },
    { locator: nextFromCurrent, reason: "relative-next" },
    { locator: relNext, reason: "rel-next" },
  ];

  for (const candidate of candidates) {
    const visible = await candidate.locator.isVisible().catch(() => false);
    if (!visible) continue;
    await candidate.locator.scrollIntoViewIfNeeded().catch(() => {});
    const disabledAttr = await candidate.locator.getAttribute("disabled").catch(() => null);
    const ariaDisabled = await candidate.locator.getAttribute("aria-disabled").catch(() => null);
    const isEnabled = await candidate.locator.isEnabled().catch(() => true);
    const disabled =
      disabledAttr != null ||
      ariaDisabled === "true" ||
      ariaDisabled === "disabled" ||
      !isEnabled;
    if (disabled) {
      return { clicked: false, reason: "disabled-target" };
    }
    const clickOk = await candidate.locator.click({ timeout: 5000 }).then(() => true).catch(() => false);
    if (!clickOk) {
      await candidate.locator.click({ timeout: 5000, force: true }).catch(() => {});
    }
    return { clicked: true, reason: candidate.reason };
  }

  const navText = await nav.innerText().catch(() => "");
  if (navText) {
    console.log(`[PAGINATION] Aucun candidat cliquable. Texte pagination: ${navText}`);
  }
  return { clicked: false, reason: "missing-target" };
}

async function getListingSignature(page) {
  try {
    return await page.evaluate((cardSelector) => {
      const firstCard = document.querySelector(cardSelector);
      const count = document.querySelectorAll(cardSelector).length;
      const titleEl = firstCard
        ? firstCard.querySelector("[id^='title__promolisting-'], .nl-product-card__title")
        : null;
      const title = titleEl ? titleEl.textContent?.trim() : "";
      const linkEl = firstCard ? firstCard.querySelector("a[href]") : null;
      const href = linkEl ? linkEl.getAttribute("href") || "" : "";
      return [count, title || "", href || ""].join("|");
    }, SELECTORS.card);
  } catch {
    return "";
  }
}

async function waitForListingSignatureChange(page, previousSignature, { timeout = 20000 } = {}) {
  try {
    await withAwaitLog(
      `waitForFunction listing signature change (timeout=${timeout})`,
      () =>
        page.waitForFunction(
          (cardSelector, prevSignature) => {
            const firstCard = document.querySelector(cardSelector);
            const count = document.querySelectorAll(cardSelector).length;
            const titleEl = firstCard
              ? firstCard.querySelector("[id^='title__promolisting-'], .nl-product-card__title")
              : null;
            const title = titleEl ? titleEl.textContent?.trim() : "";
            const linkEl = firstCard ? firstCard.querySelector("a[href]") : null;
            const href = linkEl ? linkEl.getAttribute("href") || "" : "";
            const signature = [count, title || "", href || ""].join("|");
            return signature && signature !== prevSignature;
          },
          SELECTORS.card,
          previousSignature || "",
          { timeout }
        )
    );
    return true;
  } catch {
    return false;
  }
}

async function gotoWithRetries(page, url, {
  attempts = 3,
  waitUntil = "domcontentloaded",
  networkIdleTimeout = 30000,
} = {}) {
  let lastError = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await withAwaitLog(
        `goto ${url} (attempt ${attempt}/${attempts})`,
        () => page.goto(url, { timeout: 120000, waitUntil })
      );
      await waitForNetworkIdleOrTimeout(page, `after goto ${url}`);
      await withAwaitLog("closeInterferingPopups after goto", () => closeInterferingPopups(page));
      return response || null;
    } catch (err) {
      lastError = err;
      console.warn(`[NAV] goto failed (${attempt}/${attempts}) → ${err?.message || err}`);
      await withAwaitLog("goto retry wait 2000ms", () => page.waitForTimeout(2000));
    }
  }
  if (lastError) throw lastError;
  return null;
}

function buildStableDedupKey(record) {
  const productKey = record.product_key || record.productKey;
  if (productKey) {
    return `product_key:${String(productKey).toLowerCase()}`;
  }

  const storeId = record.store_id ?? record.storeId ?? null;
  const sku = record.sku ?? record.sku_formatted ?? null;
  if (storeId && sku) {
    return `store:${storeId}|sku:${String(sku).toLowerCase()}`;
  }

  const normalizedUrl = normalizeProductUrlForDedup(record.url || record.link);
  if (storeId && normalizedUrl) {
    return `store:${storeId}|url:${normalizedUrl}`;
  }

  return null;
}

function dedupeDeals(records) {
  const seen = new Set();
  const deduped = [];

  for (const record of records) {
    const key = buildStableDedupKey(record);
    if (!key) {
      deduped.push(record);
      continue;
    }
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(record);
  }

  return deduped;
}

function buildDedupKeysFromRecord(record) {
  const keys = [];
  if (record.product_key) {
    keys.push(`product_key:${String(record.product_key).toLowerCase()}`);
  }

  const productNumber = record.product_number ?? record.productNumber;
  if (productNumber) {
    keys.push(`product_number:${String(productNumber).toLowerCase()}`);
  }

  const sku = record.sku ?? record.sku_formatted;
  if (sku) {
    keys.push(`sku:${String(sku).toLowerCase()}`);
  }

  const normalizedUrl = normalizeProductUrlForDedup(record.url || record.link);
  if (normalizedUrl) {
    keys.push(`url:${normalizedUrl}`);
  }

  return keys;
}

function resolveOutputPaths(storeId, storeName = "") {
  const storeIdStr = storeId != null ? String(storeId) : "";
  const normalizedStoreName = storeName != null ? String(storeName) : "";
  let OUT_BASE = args.outBase;
  if (!OUT_BASE) {
    const citySlug = slugify(normalizedStoreName, { lower: true, strict: true });
    OUT_BASE = path.join("outputs", "canadiantire", `${storeIdStr}-${citySlug}`);
  }
  const jsonPath = path.join(OUT_BASE, "data.json");
  return { OUT_BASE, jsonPath };
}

function normalizeAvailabilityInfo(rawAvailability, stockQtyInput = null) {
  const availabilityText = rawAvailability == null
    ? null
    : (typeof rawAvailability === "string" ? rawAvailability : String(rawAvailability)).trim();

  const isEnumAvailability =
    availabilityText && ["in_stock", "out_of_stock", "unknown"].includes(availabilityText);

  let availability = isEnumAvailability ? availabilityText : "unknown";
  let stockQty = Number.isFinite(stockQtyInput) ? Number(stockQtyInput) : null;

  const normalizedText = availabilityText
    ? availabilityText
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
    : "";

  if (stockQty == null && availabilityText) {
    const prefix = availabilityText.split("#")[0];
    const prefixNumbers = Array.from(prefix.matchAll(/(\d+)/g))
      .map((match) => Number(match[1]))
      .filter((num) => Number.isFinite(num) && num < 10000);

    if (prefixNumbers.length > 0) {
      stockQty = prefixNumbers[0];
    }
  }

  if (stockQty != null) {
    availability = stockQty > 0 ? "in_stock" : "out_of_stock";
  } else if (!isEnumAvailability && normalizedText) {
    if (/(rupture|out of stock|epuise|sold out|indisponible|not available)/.test(normalizedText)) {
      availability = "out_of_stock";
    } else if (/(en stock|available|disponible|quantite|reste)/.test(normalizedText)) {
      availability = "in_stock";
    }
  }

  return {
    availability,
    stockQty,
    availabilityText: availabilityText || null,
  };
}

function mapRecordToNationalIndex(record, storeMeta) {
  const productKey = record.product_key || record.productKey;
  if (!productKey) return null;

  const availabilityInfo = normalizeAvailabilityInfo(
    record.availability_text ?? record.availabilityText ?? record.availability ?? null,
    record.stockQty ?? record.stock_qty ?? null,
  );

  const price = extractPrice(
    record.liquidation_price ?? record.sale_price ?? record.price ?? null
  );
  const originalPrice = extractPrice(
    record.regular_price ?? record.price_original ?? record.price ?? null
  );

  return {
    productKey,
    storeId: storeMeta.storeId ?? null,
    storeSlug: storeMeta.storeSlug ?? null,
    storeName: storeMeta.storeName ?? null,
    price: Number.isFinite(price) ? Number(price) : null,
    originalPrice: Number.isFinite(originalPrice) ? Number(originalPrice) : null,
    discountPercent: record.discount_percent ?? null,
    title: record.title ?? record.name ?? null,
    productUrl: record.url ?? record.link ?? null,
    stockQty: availabilityInfo.stockQty,
    availability: availabilityInfo.availability,
    availabilityText: availabilityInfo.availabilityText,
  };
}

function createRecordFromCard(card, pageIsClearance, storeContext = { storeId: null, city: null }) {
  const priceSaleRaw = card.price_sale_raw ?? card.price_sale ?? null;
  const priceWasRaw = card.price_original_raw ?? card.price_original ?? null;
  const salePrice = extractPrice(priceSaleRaw ?? undefined);
  const regularPrice = extractPrice(priceWasRaw ?? undefined);
  const priceRaw = priceSaleRaw || priceWasRaw || null;
  const price = salePrice ?? regularPrice ?? null;

  const availabilityKeys = buildCtKeysFromAvailability(card.availability);
  const productNumberRaw =
    card.product_number_raw ??
    extractCtProductNumberRaw(
      card.product_number,
      card.link,
      card.name,
      card.title
    ) ??
    availabilityKeys.productNumberRaw ??
    card.productNumberRaw ??
    null;
  const productNumber =
    normalizeCtProductNumber(productNumberRaw) ??
    availabilityKeys.productNumber ??
    normalizeCtProductNumber(card.product_number ?? card.productNumber ?? null);
  const productKey =
    card.product_key ||
    card.productKey ||
    makeCtProductKey(
      productNumberRaw ??
      availabilityKeys.productNumberRaw ??
      card.product_number ??
      card.productNumber ??
      null
    ) ||
    availabilityKeys.productKey;

  const discountPercent =
    card.discount_percent != null
      ? card.discount_percent
      : computeDiscountPercent(regularPrice, salePrice);

  const meetsDiscountThreshold =
    regularPrice != null &&
    salePrice != null &&
    regularPrice > 0 &&
    salePrice > 0 &&
    discountPercent >= 50;

  if (!meetsDiscountThreshold) return null;

  const discount_percent =
    discountPercent != null ? Math.round(discountPercent * 100) / 100 : null;

  const badges = Array.isArray(card.badges) ? card.badges : [];
  const normalizedBadges = badges.map((b) => b.toLowerCase());
  const hasLiquidationBadge = normalizedBadges.some((b) => /liquidation|clearance/.test(b));
  const isLiquidation = hasLiquidationBadge ||
    (pageIsClearance && salePrice != null && (regularPrice == null || salePrice <= regularPrice));

  const availabilityInfo = normalizeAvailabilityInfo(
    card.availability,
    card.stockQty ?? card.stock_qty ?? null,
  );

  const rec = {
    store_id: storeContext.storeId || null,
    city: storeContext.city || null,
    name: card.name || null,
    title: card.name || null,
    price,
    price_raw: priceRaw,
    liquidation: !!isLiquidation,
    image: card.image || null,
    image_url: card.image || null,
    url: card.link || null,
    link: card.link || null,
    product_id: card.product_id || null,
    sku: card.sku || null,
    sku_formatted: card.sku_formatted || null,
    product_number_raw: productNumberRaw || null,
    product_number: productNumber || null,
    product_key: productKey || null,
    productNumberRaw: productNumberRaw || null,
    productNumber: productNumber || null,
    productKey: productKey || null,
    availability: availabilityInfo.availability,
    availability_text: availabilityInfo.availabilityText,
    stockQty: availabilityInfo.stockQty,
    badges,
    discount_percent,
  };

  if (INCLUDE_LIQUIDATION_PRICE) {
    rec.liquidation_price = salePrice ?? null;
    rec.liquidation_price_raw = priceSaleRaw || null;
    rec.sale_price = salePrice ?? null;
    rec.sale_price_raw = priceSaleRaw || null;
  }
  if (INCLUDE_REGULAR_PRICE) {
    rec.regular_price = regularPrice ?? null;
    rec.regular_price_raw = priceWasRaw || null;
  }

  rec.price_sale_clean = card.price_sale || null;
  rec.price_original_clean = card.price_original || null;

  return rec;
}

async function lazyWarmup(page) {
  console.log("[AWAIT] ▶ lazyWarmup start");
  // scroll rapide pour déclencher lazy render des prix/images sans multiplier les pauses
  await withAwaitLog("lazyWarmup page.evaluate scroll", () =>
    page.evaluate(async () => {
      const viewport = window.innerHeight || 800;
      const maxScroll = document.body.scrollHeight || viewport;
      if (maxScroll <= viewport * 1.15) {
        window.scrollTo(0, 0);
        return;
      }
      const step = Math.max(260, Math.floor(viewport * 1.3));
      const delay = 35;
      for (let y = 0; y < maxScroll; y += step) {
        window.scrollTo(0, y);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
      window.scrollTo(0, 0);
    })
  );
  await withAwaitLog("lazyWarmup waitForTimeout 40ms", () => page.waitForTimeout(40));
  await withAwaitLog("lazyWarmup race price selector", () =>
    Promise.race([
      page.waitForSelector(
        [
          "[data-testid='sale-price']",
          "[data-testid='regular-price']",
          "span[data-testid='priceTotal']",
          ".nl-price--total",
          ".nl-price__total",
          ".price__value",
          ".c-pricing__current",
        ].join(", "),
        { timeout: 4500 }
      ),
      page.waitForTimeout(650),
    ])
  ).catch(()=>{});
  console.log("[AWAIT] ✓ lazyWarmup end");
}

async function maybeCloseStoreModal(page) {
  const selectors = [
    "button[aria-label='Fermer']",
    "button[aria-label='Close']",
    "button:has-text('Plus tard')",
    "button:has-text('Later')",
    "button:has-text('Continuer')",
    "button:has-text('Continue')",
  ];
  for (const sel of selectors) {
    try {
      const loc = page.locator(sel).first();
      if (await loc.isVisible().catch(()=>false)) {
        await withAwaitLog(`maybeCloseStoreModal click ${sel}`, () => loc.click().catch(()=>{}));
        await withAwaitLog("maybeCloseStoreModal wait 500ms", () => page.waitForTimeout(500));
      }
    } catch {}
  }
}

const STORE_SELECTORS = {
  openButtons: [
    "button:has-text('Sélectionner le magasin')",
    "button:has-text('Choose Store')",
    "a:has-text('Changer de magasin')",
    "a:has-text('Change Store')",
  ].join(", "),
  confirmButtons: [
    "button:has-text('Définir ce magasin')",
    "button:has-text('Set as My Store')",
  ].join(", "),
  closeButtons: [
    "button[aria-label*='fermer' i]",
    "button[aria-label*='close' i]",
    "button:has-text('Fermer')",
    "button:has-text('Close')",
  ].join(", "),
  storeCards: "[data-store-id], [href*='store=']",
};

async function openStoreSelector(page) {
  const openButton = page.locator(STORE_SELECTORS.openButtons).first();
  if (await openButton.isVisible().catch(() => false)) {
    await withAwaitLog("openStoreSelector click", () =>
      openButton.click({ timeout: 5000 }).catch(() => {})
    );
  }
  await withAwaitLog("openStoreSelector wait for storeCards visible", () =>
    page.locator(STORE_SELECTORS.storeCards).first().waitFor({ state: "visible", timeout: 10000 })
  ).catch(() => {});
}

async function closeStoreSelector(page) {
  const closeButton = page.locator(STORE_SELECTORS.closeButtons).first();
  if (await closeButton.isVisible().catch(() => false)) {
    await withAwaitLog("closeStoreSelector click", () =>
      closeButton.click({ timeout: 3000 }).catch(() => {})
    );
  }
  await withAwaitLog("closeStoreSelector press Escape", () =>
    page.keyboard.press("Escape").catch(() => {})
  );
  await withAwaitLog("closeStoreSelector wait for storeCards hidden", () =>
    page.locator(STORE_SELECTORS.storeCards).first().waitFor({ state: "hidden", timeout: 5000 })
  ).catch(() => {});
}

async function clickStoreCard(page, storeId) {
  const byId = page.locator(`[data-store-id='${storeId}'], [href*='store=${storeId}']`).first();
  if (await byId.isVisible().catch(() => false)) {
    await byId.click().catch(() => {});
    return true;
  }
  const byText = page.locator(`text=${storeId}`).first();
  if (await byText.isVisible().catch(() => false)) {
    await byText.click().catch(() => {});
    return true;
  }
  return false;
}

async function waitForStoreApplied(page, storeId, storeName) {
  const expectedStoreId = normalizeStoreId(storeId);
  const currentUrl = page.url();
  try {
    const parsed = new URL(currentUrl);
    if (parsed.searchParams.get("store") === expectedStoreId) {
      return true;
    }
  } catch {}

  const checks = [];

  checks.push(
    loggedPromise(
      "waitForFunction store param",
      page.waitForFunction(
        (id) => {
          try {
            const url = new URL(window.location.href);
            return url.searchParams.get("store") === id;
          } catch {
            return false;
          }
        },
        expectedStoreId,
        { timeout: 15000 }
      )
    )
  );

  checks.push(
    loggedPromise(
      "waitForResponse store param",
      page.waitForResponse(
        (response) => {
          const url = response.url();
          if (url.includes(`store=${expectedStoreId}`) || url.includes(`storeId=${expectedStoreId}`)) {
            return true;
          }
          const postData = response.request().postData();
          return postData ? postData.includes(expectedStoreId) : false;
        },
        { timeout: 15000 }
      )
    )
  );

  if (storeName) {
    const normalizedStoreName = String(storeName).trim();
    if (normalizedStoreName) {
      checks.push(
        loggedPromise(
          `waitFor store name visible (${normalizedStoreName})`,
          page.locator(`text=${normalizedStoreName}`).first().waitFor({ state: "visible", timeout: 15000 })
        )
      );
    }
  }

  try {
    await Promise.any(checks);
    return true;
  } catch {
    return false;
  }
}

async function getDetectedStoreText(page) {
  const candidates = page.locator(
    [
      "[data-testid*='store']",
      "[aria-label*='magasin' i]",
      "[aria-label*='store' i]",
      "button:has-text('Magasin')",
      "button:has-text('Store')",
    ].join(", ")
  );
  const count = await candidates.count();
  for (let i = 0; i < Math.min(count, 3); i++) {
    const text = await candidates.nth(i).textContent().catch(() => null);
    if (text && text.trim()) return text.trim();
  }
  return null;
}

async function saveStoreDebugArtifacts(page, storeId, debugDir) {
  if (!debugDir) return;
  await fsExtra.ensureDir(debugDir);
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const baseName = `store-selection-${storeId}-${timestamp}`;
  const screenshotPath = path.join(debugDir, `${baseName}.png`);
  const htmlPath = path.join(debugDir, `${baseName}.html`);
  const logPath = path.join(debugDir, `${baseName}.log`);

  const detectedStore = await getDetectedStoreText(page);
  const logPayload = [
    `storeId=${storeId}`,
    `url=${page.url()}`,
    `detectedStore=${detectedStore || "n/a"}`,
  ].join("\n");

  await Promise.all([
    page.screenshot({ path: screenshotPath, fullPage: true }).catch(() => {}),
    page.content().then((content) => fsExtra.writeFile(htmlPath, content)).catch(() => {}),
    fsExtra.writeFile(logPath, logPayload).catch(() => {}),
  ]);
}

async function selectStore(page, { storeId, storeName, debugDir } = {}) {
  const normalizedStoreId = normalizeStoreId(storeId);
  if (!normalizedStoreId) return false;

  const maxRetries = 2;
  console.log(`Selecting store ${normalizedStoreId}...`);

  for (let attempt = 1; attempt <= maxRetries + 1; attempt += 1) {
    await openStoreSelector(page);
    const clicked = await clickStoreCard(page, normalizedStoreId);
    if (!clicked) {
      console.warn(`[STORE] Impossible de cliquer le magasin ${normalizedStoreId} (tentative ${attempt}).`);
    }

    const confirmButton = page.locator(STORE_SELECTORS.confirmButtons).first();
    if (await confirmButton.isVisible().catch(() => false)) {
      await withAwaitLog("selectStore confirm click", () =>
        confirmButton.click({ timeout: 5000 }).catch(() => {})
      );
      await withAwaitLog("selectStore confirm hidden", () =>
        confirmButton.waitFor({ state: "hidden", timeout: 10000 }).catch(() => {})
      );
    }

    const validated = await withAwaitLog(
      `waitForStoreApplied store=${normalizedStoreId}`,
      () => waitForStoreApplied(page, normalizedStoreId, storeName)
    );
    if (validated) {
      console.log(`Validated store ${normalizedStoreId}`);
      return true;
    }

    if (attempt <= maxRetries) {
      console.warn(`Validation failed → retry ${attempt}/${maxRetries} ...`);
      await withAwaitLog("closeStoreSelector retry", () => closeStoreSelector(page));
      await waitForNetworkIdleOrTimeout(page, "after closeStoreSelector");
    }
  }

  console.error("Failed after retries → debug saved");
  await saveStoreDebugArtifacts(page, normalizedStoreId, debugDir);
  return false;
}

async function scrollUntilCardsStop(page) {
  const start = Date.now();
  const maxTotalMs = 3 * 60 * 1000;
  const stableRoundsToStop = 3;
  let stable = 0;
  let lastCount = await page.locator(CARD_COUNT_SELECTOR).count();

  console.log(`[SCROLL] start cards=${lastCount}`);

  for (let round = 1; ; round += 1) {
    if (Date.now() - start > maxTotalMs) {
      console.log("[SCROLL] timeout global atteint (3 minutes).");
      break;
    }

    await page.mouse.wheel(0, 1600);
    await page.evaluate(() => window.scrollBy(0, 1200));

    const waitMs = 600 + Math.floor(Math.random() * 301);
    await page.waitForTimeout(waitMs);

    const count = await page.locator(CARD_COUNT_SELECTOR).count();
    if (count > lastCount) {
      lastCount = count;
      stable = 0;
    } else {
      stable += 1;
    }

    console.log(`[SCROLL] cards=${count} stable=${stable}/${stableRoundsToStop}`);
    if (stable >= stableRoundsToStop) break;
  }
}

async function clickLoadMoreUntilNoGrowth(page, {
  cardSelector = SELECTORS.card,
  maxClicks = 25,
  perClickWaitMs = 1200,
  stableRoundsToStop = 3,
  maxTotalMs = 300000,
} = {}) {
  const start = Date.now();
  let previousCount = await page.locator(cardSelector).count();
  let clicks = 0;
  let stableRounds = 0;

  for (let attempt = 1; attempt <= maxClicks; attempt += 1) {
    if (Date.now() - start > maxTotalMs) {
      console.log(`[PAGINATION] Charger plus: timeout global (${maxTotalMs}ms).`);
      break;
    }

    const loadMoreButton = page.locator(LOAD_MORE_SELECTORS).first();
    const visible = await loadMoreButton.isVisible().catch(() => false);
    if (!visible) {
      console.log("[PAGINATION] Charger plus: bouton absent.");
      break;
    }

    await loadMoreButton.scrollIntoViewIfNeeded().catch(() => {});
    await page.evaluate(() => window.scrollBy(0, window.innerHeight));

    const clicked = await loadMoreButton
      .click({ timeout: 5000 })
      .then(() => true)
      .catch(() => false);
    if (!clicked) {
      await loadMoreButton.click({ timeout: 5000, force: true }).catch(() => {});
    }

    clicks += 1;
    await waitForNetworkIdleOrTimeout(page, "after load more");
    await withAwaitLog("waitForRealCards after load more", () =>
      waitForRealCards(page, { timeout: 30000, minRealCards: 5 })
    );
    await withAwaitLog(`waitForTimeout ${perClickWaitMs}ms after load more`, () =>
      page.waitForTimeout(perClickWaitMs)
    );

    const newCount = await page.locator(cardSelector).count();
    const delta = newCount - previousCount;
    console.log(
      `[PAGINATION] Charger plus: ${previousCount} → ${newCount} (Δ${delta}) (click ${clicks})`
    );
    if (newCount <= previousCount) {
      stableRounds += 1;
      console.log(`[PAGINATION] Charger plus: stable (${stableRounds}/${stableRoundsToStop}).`);
      if (stableRounds >= stableRoundsToStop) {
        break;
      }
    } else {
      stableRounds = 0;
      previousCount = newCount;
    }
  }

  return { clicks, finalCount: previousCount };
}

function normalizePaginationBaseUrl(inputUrl) {
  try {
    const parsed = new URL(inputUrl, BASE);
    parsed.searchParams.delete("page");
    return parsed.toString();
  } catch {
    return inputUrl;
  }
}

function buildPaginationUrl(baseUrl, pageNum) {
  try {
    const parsed = new URL(baseUrl, BASE);
    parsed.searchParams.set("page", String(pageNum));
    return parsed.toString();
  } catch {
    return baseUrl;
  }
}

async function scrapeCategoryAllPages(page, storeUrl, storeId, {
  extractPage,
  storeName,
  debugDir,
} = {}) {
  const items = [];
  let storeInitialized = false;
  let lastResponseStatus = null;

  const baseUrl = normalizePaginationBaseUrl(storeUrl);
  console.log("➡️  Go to:", baseUrl);
  const response = await gotoWithRetries(page, baseUrl, {
    attempts: 3,
    waitUntil: "domcontentloaded",
    networkIdleTimeout: 30000,
  });
  lastResponseStatus = response ? response.status() : null;
  await logNavigationDebug(page, { pageNum: 1, responseStatus: lastResponseStatus });

  const watchdogIntervalMs = 10000;
  const watchdogTimer = setInterval(() => {
    void (async () => {
      try {
        const url = page.url();
        const count = await page.locator(SELECTORS.card).count();
        console.log(`[WATCHDOG] still alive url=${url} cards=${count}`);
      } catch (err) {
        console.warn(`[WATCHDOG] still alive check failed: ${err?.message || err}`);
      }
    })();
  }, watchdogIntervalMs);

  try {
    const pageNum = 1;
    if (hasReachedTimeLimit()) {
      console.log(`[PAGINATION] Stop page ${pageNum}: limite de temps atteinte.`);
      return items;
    }

    await (async () => {
      if (hasReachedTimeLimit()) {
        console.log(`[PAGINATION] Stop page ${pageNum}: limite de temps atteinte.`);
        return;
      }

      try {
        await Promise.race([
          (async () => {
            if (pageNum > 1) {
              if (reachedEnd || clickFailed) {
                return;
              }
              let changed = false;
              const maxClickRetries = 3;

              for (let attempt = 1; attempt <= maxClickRetries; attempt += 1) {
                const signatureBefore = await getListingSignature(page);
                console.log("[PAGINATION] Scroll bas pour pagination UI.");
                await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
                await page.waitForTimeout(800);
                const { clicked, reason } = await clickPaginationNext(page, pageNum);
                if (!clicked) {
                  if (reason === "disabled-target" || reason === "missing-target" || reason === "missing-nav") {
                    console.log(`[PAGINATION] Stop page ${pageNum}: cible pagination absente/désactivée.`);
                    reachedEnd = true;
                    if (debugDir) {
                      await savePageDebugArtifacts(page, debugDir, {
                        pageNum,
                        label: "pagination-missing-target",
                        responseStatus: lastResponseStatus,
                      });
                    }
                  } else {
                    clickFailed = true;
                  }
                  break;
                }

                await withAwaitLog("waitForLoadState domcontentloaded (pagination click)", () =>
                  page.waitForLoadState("domcontentloaded", { timeout: 30000 })
                ).catch(() => {});
                await withAwaitLog("waitProductsStable (pagination click)", () => waitProductsStable(page, 60000));
                const signatureChanged = await withAwaitLog(
                  "waitForListingSignatureChange (pagination click)",
                  () => waitForListingSignatureChange(page, signatureBefore, { timeout: 25000 })
                );
                await withAwaitLog("closeInterferingPopups (pagination click)", () =>
                  closeInterferingPopups(page)
                );
                if (signatureChanged) {
                  changed = true;
                  break;
                }
                console.warn(
                  `[PAGINATION] Page ${pageNum}: signature inchangée après clic (${attempt}/${maxClickRetries}).`
                );
                await withAwaitLog("pagination click retry wait 1500ms", () => page.waitForTimeout(1500));
              }

              if (!changed) {
                console.warn(
                  `[PAGINATION] Stop page ${pageNum}: signature inchangée après ${maxClickRetries} essais.`
                );
                if (debugDir) {
                  await savePageDebugArtifacts(page, debugDir, {
                    pageNum,
                    label: "pagination-signature-stuck",
                    responseStatus: lastResponseStatus,
                  });
                }
                return;
              }
            }

            if (!storeInitialized) {
              const m = storeUrl.match(/[?&]store=(\d+)/);
              const storeIdFromUrl = m ? m[1] : null;
              if (storeIdFromUrl || storeId) {
                const targetStoreId = storeIdFromUrl || storeId;
                console.log(
                  `[STORE] Store déjà défini via l'URL (${targetStoreId}) → aucune sélection UI.`
                );
                let validated = await withAwaitLog(
                  `waitForStoreApplied (url store ${targetStoreId})`,
                  () => waitForStoreApplied(page, targetStoreId, storeName)
                );
                if (!validated) {
                  console.warn(
                    `[STORE] Store non confirmé via l'URL (${targetStoreId}).`
                  );
                }
              }
              storeInitialized = true;
            }

            const isStable = await withAwaitLog(
              `waitProductsStableWithRetries page ${pageNum}`,
              () =>
                waitProductsStableWithRetries(page, {
                  timeout: 60000,
                  retries: 2,
                  pageNum,
                  debugDir,
                  responseStatus: lastResponseStatus,
                })
            );
            if (!isStable) {
              console.log(`[PAGINATION] Page ${pageNum}: produits non détectés après retries.`);
              return;
            }

            const realCardsReady = await withAwaitLog(
              `waitForRealCards page ${pageNum}`,
              () => waitForRealCards(page, { timeout: 45000, minRealCards: 5 })
            );
          }
        }
        storeInitialized = true;
      }

            await withAwaitLog(`scrollUntilCardsStop page ${pageNum}`, () =>
              scrollUntilCardsStop(page)
            );
            await lazyWarmup(page);

            let pageResult = await withAwaitLog(
              `extractPage page ${pageNum}`,
              () => extractPage(pageNum)
            );
            const placeholderCandidate =
              pageNum > 1 &&
              pageResult.cardsDetected === 1 &&
              !pageResult.hasTitle &&
              !pageResult.hasLink;

            if (placeholderCandidate) {
              console.warn(`[PAGINATION] Page ${pageNum}: placeholder détecté, retry après reload.`);
              await withAwaitLog("reload page (placeholder)", () =>
                page.reload({ waitUntil: "domcontentloaded" }).catch(() => {})
              );
              await waitForNetworkIdleOrTimeout(page, "after reload placeholder");
              await withAwaitLog("waitProductsStable after reload", () => waitProductsStable(page, 60000));
              await withAwaitLog("waitForRealCards after reload", () =>
                waitForRealCards(page, { timeout: 45000, minRealCards: 5 })
              );
              await withAwaitLog("scrollUntilCardsStop after reload", () =>
                scrollUntilCardsStop(page)
              );
              await lazyWarmup(page);
              pageResult = await withAwaitLog(
                `extractPage retry page ${pageNum}`,
                () => extractPage(pageNum)
              );
            }

            const {
              records,
              totalProducts,
              rawCount,
              productKeys,
              cardsDetected,
              withAnyPrice,
              withBothPrices,
              deals50,
              hasTitle,
              hasLink,
            } = pageResult;
            console.log(
              `[PAGINATION] Page ${pageNum}: items extraits=${cardsDetected ?? rawCount ?? 0} ` +
              `cardsDetected=${cardsDetected ?? 0} withAnyPrice=${withAnyPrice ?? 0} ` +
              `withBothPrices=${withBothPrices ?? 0} deals50=${deals50 ?? 0}`
            );

      let realCardStats = await getRealCardStats(page);
      const placeholderCandidate =
        realCardStats.cardsDetected <= 2 && realCardStats.realCards === 0;

      if (placeholderCandidate) {
        console.warn(`[PAGINATION] Page ${pageNum}: placeholder détecté, reload unique.`);
        await withAwaitLog("reload page (placeholder)", () =>
          page.reload({ waitUntil: "domcontentloaded" }).catch(() => {})
        );
        await waitForNetworkIdleOrTimeout(page, "after reload placeholder");
        await withAwaitLog("waitProductsStable after reload", () => waitProductsStable(page, 60000));
        await withAwaitLog("waitForRealCards after reload", () =>
          waitForRealCards(page, { timeout: 45000, minRealCards: 5 })
        );
        realCardStats = await getRealCardStats(page);
        if (realCardStats.cardsDetected <= 2 && realCardStats.realCards === 0) {
          if (debugDir) {
            await savePageDebugArtifacts(page, debugDir, {
              pageNum,
              label: "pagination-placeholder",
              responseStatus: lastResponseStatus,
            });
          }
          console.log(`[PAGINATION] Stop page ${pageNum}: placeholder persistant.`);
          return;
        }
      }

      await withAwaitLog(`clickLoadMoreUntilNoGrowth page ${pageNum}`, () =>
        clickLoadMoreUntilNoGrowth(page, {
          cardSelector: SELECTORS.card,
          stableRoundsToStop: 3,
          maxTotalMs: 360000,
        })
      );
      await lazyWarmup(page);
      await autoScrollLoadAllProducts(page, autoScrollConfig);

      const pageResult = await withAwaitLog(
        `extractPage page ${pageNum}`,
        () => extractPage(pageNum)
      );

      const {
        records,
        totalProducts,
        rawCount,
        cardsDetected,
        withAnyPrice,
        withBothPrices,
        deals50,
      } = pageResult;
      console.log(
        `[PAGINATION] Page ${pageNum}: items extraits=${cardsDetected ?? rawCount ?? 0} ` +
        `cardsDetected=${cardsDetected ?? 0} withAnyPrice=${withAnyPrice ?? 0} ` +
        `withBothPrices=${withBothPrices ?? 0} deals50=${deals50 ?? 0}`
      );

      items.push(...records);
    })();
  } finally {
    clearInterval(watchdogTimer);
  }

  return items;
}

// ---------- MAIN ----------
async function scrapeStore(store) {
  const normalizedStore = {
    storeId: store?.storeId != null ? String(store.storeId) : store?.id != null ? String(store.id) : null,
    storeName: store?.storeName ?? store?.city ?? store?.name ?? "",
  };
  const storeId = args.storeId != null ? String(args.storeId) : normalizedStore.storeId;
  const storeNorm = normalizeStoreId(storeId);
  const storeIdForUrl = storeNorm || (storeId != null ? String(storeId) : "");
  const city = normalizedStore.storeName || null;
  const cliStoreName = args.storeName ? String(args.storeName) : "";
  const storeName = cliStoreName || normalizedStore.storeName || "";
  if (hasReachedTimeLimit()) {
    console.log(
      `[SCRAPER] Limite atteinte avant le magasin ${storeId ?? "?"}. Arrêt du lancement de ce magasin.`
    );
    return;
  }
  console.log(`[SCRAPER] Magasin ${storeId ?? "?"} – ${storeName || city || "Nom inconnu"} : début`);

  const { OUT_BASE, jsonPath: OUT_JSON } = resolveOutputPaths(
      storeId ?? "",
      storeName || city || ""
    );
    const debugDir = path.join(OUT_BASE, "debug");

  console.log(`OUT_BASE=${OUT_BASE}`);
  console.log(`💾  JSON → ${OUT_JSON}`);

  const browser = await chromium.launch({ headless: HEADLESS, args: ["--disable-dev-shm-usage"] });
  const context = await browser.newContext({ locale: "fr-CA" });
  context.setDefaultTimeout(0);
  const page = await context.newPage();
  page.setDefaultNavigationTimeout(0);

  const storeContext = { storeId: storeNorm || storeId, city: storeName || city || null };

  try {
    await page.route("**/*medallia*", (route) => route.abort());
    await page.route("**/resources.digital-cloud.medallia.ca/**", (route) => route.abort());
    await fsExtra.ensureDir(OUT_BASE);

    const baseUrl = DEFAULT_BASE;
    const storeUrl = `${baseUrl}?store=${storeIdForUrl}`;
    console.log(`⚙️  Options → liquidation_price=${INCLUDE_LIQUIDATION_PRICE ? "on":"off"}, regular_price=${INCLUDE_REGULAR_PRICE ? "on":"off"}`);

    const allDeals = [];
    const dedupeKeys = new Set();

    const registerRecord = (record) => {
      const key = buildStableDedupKey(record);
      if (key && dedupeKeys.has(key)) return false;
      if (key) dedupeKeys.add(key);
      allDeals.push(record);
      return true;
    };

    const extractProductsOnPage = async (skipGuards, pageNum) => {
      const cards = await scrapeListing(page, { skipGuards });
      const pageIsClearance = /\/liquidation\.html/i.test(await page.url());
      const productKeysSet = new Set();
      const records = [];
      const debugSamples = [];
      const hasTitle = cards.some((card) => Boolean(card?.name || card?.title));
      const hasLink = cards.some((card) => Boolean(card?.link));
      const stats = {
        cardsDetected: cards.length,
        withAnyPrice: 0,
        withBothPrices: 0,
        deals50: 0,
      };

      for (const card of cards) {
        const availabilityKeys = buildCtKeysFromAvailability(card.availability);
        const productNumberRaw =
          card.product_number_raw ??
          extractCtProductNumberRaw(card.link, card.name, card.title) ??
          availabilityKeys.productNumberRaw ??
          null;
        const normalizedProductNumber =
          normalizeCtProductNumber(productNumberRaw) ??
          availabilityKeys.productNumber ??
          normalizeCtProductNumber(card.product_number ?? card.productNumber ?? null);
        const productKey =
          card.product_key ||
          card.productKey ||
          makeCtProductKey(productNumberRaw ?? availabilityKeys.productNumberRaw ?? null) ||
          availabilityKeys.productKey;

        if (productNumberRaw) card.product_number_raw = productNumberRaw;
        if (normalizedProductNumber) card.product_number = normalizedProductNumber;
        if (productKey) card.product_key = productKey;

        buildProductIdentifiers(card).forEach((key) => productKeysSet.add(key));

        const regularPriceForCheck = extractPrice(
          card.price_original_raw ??
          card.price_original ??
          card.regular_price ??
          null
        );
        const salePriceForCheck = extractPrice(
          card.price_sale_raw ??
          card.price_sale ??
          card.sale_price ??
          null
        );

        if (regularPriceForCheck != null || salePriceForCheck != null) {
          stats.withAnyPrice += 1;
          if (regularPriceForCheck != null && salePriceForCheck != null) {
            stats.withBothPrices += 1;
          }
        }

        const discountPercent = computeDiscountPercent(
          regularPriceForCheck,
          salePriceForCheck
        );

        if (discountPercent != null && discountPercent >= 50) {
          stats.deals50 += 1;
        }

        if (debugSamples.length < 10) {
          const saleRaw = card.price_sale_raw ?? card.price_sale ?? null;
          const wasRaw = card.price_original_raw ?? card.price_original ?? null;
          const saleNum = extractPrice(saleRaw ?? undefined);
          const regularNum = extractPrice(wasRaw ?? undefined);
          const discount = computeDiscountPercent(regularNum, saleNum);

          debugSamples.push({
            title: card.name || card.title || null,
            saleRaw: saleRaw || null,
            wasRaw: wasRaw || null,
            sale: saleNum ?? null,
            regular: regularNum ?? null,
            discount: discount ?? null,
            url: card.link || null,
          });
        }

        if (discountPercent == null || discountPercent < 50) {
          continue;
        }
        const record = createRecordFromCard(
          { ...card, discount_percent: discountPercent },
          pageIsClearance,
          storeContext
        );
        if (!record) continue;
        if (record.title || record.price != null || record.image) {
          records.push(record);
        }
      }

      const needsPriceDebug =
        stats.cardsDetected > 0 &&
        (stats.withAnyPrice === 0 || (pageNum === 1 && stats.deals50 === 0));

      if (needsPriceDebug) {
        const pageLabel = pageNum != null ? `page ${pageNum}` : "page";
        console.log(`[DEBUG] ${pageLabel} – premières cartes (max 10):`);
        debugSamples.forEach((sample, index) => {
          console.log(
            [
              `[DEBUG][${pageLabel}][${index + 1}]`,
              `title=${sample.title ?? "n/a"}`,
              `saleRaw=${sample.saleRaw ?? "n/a"}`,
              `wasRaw=${sample.wasRaw ?? "n/a"}`,
              `sale=${sample.sale ?? "n/a"}`,
              `regular=${sample.regular ?? "n/a"}`,
              `discount%=${sample.discount ?? "n/a"}`,
              `url=${sample.url ?? "n/a"}`,
            ].join(" | ")
          );
        });

        if (debugDir) {
          await savePagePriceDebugArtifacts(page, debugDir, pageNum ?? 0);
        }
      }

      return {
        records,
        totalProducts: stats.cardsDetected,
        rawCount: stats.cardsDetected,
        productKeys: productKeysSet,
        accepted: records.length,
        cardsDetected: stats.cardsDetected,
        withAnyPrice: stats.withAnyPrice,
        withBothPrices: stats.withBothPrices,
        deals50: stats.deals50,
        hasTitle,
        hasLink,
      };
    };

    const itemsAllPages = await scrapeCategoryAllPages(page, storeUrl, storeNorm || storeId, {
      extractPage: (pageNum) => extractProductsOnPage(true, pageNum),
      storeName: storeName || city || "",
      debugDir,
    });

    let deals = itemsAllPages.filter((x) => (x.discount_percent ?? 0) >= 50);
    deals = dedupeDeals(deals);

    let accepted = 0;
    for (const deal of deals) {
      if (registerRecord(deal)) accepted += 1;
    }

    console.log(
      `✅ ${accepted} deal(s) >= 50% agrégés sur ${itemsAllPages.length} item(s) pour ${storeUrl}`
    );
    if (accepted === 0) {
      console.log("ℹ️  Aucun deal >= 50% trouvé sur l'ensemble des pages de liquidation.");
    }

    console.log(
      `[SCRAPER] Fin du scraping pour le magasin ${storeId ?? "?"} – ${allDeals.length} deal(s) total.`
    );

    const results = allDeals.map((out) => ({ ...out, image_url: out.image_url ?? out.image ?? null }));

    await fsExtra.remove(OUT_JSON);

    fs.writeFileSync(OUT_JSON, JSON.stringify(results, null, 2));
    console.log(`💾  JSON → ${OUT_JSON}`);

    console.log(`[SCRAPER] Magasin ${storeId ?? "?"} – terminé`);
  } catch (error) {
    console.error(`[SCRAPER] ERREUR magasin ${storeId ?? "?"} –`, error);
  } finally {
    await browser.close();
  }
}

const CONCURRENCY = 4; // 4 magasins en parallèle

async function run() {
  console.log(`[SCRAPER] ${storesToProcess.length} magasins à traiter, ${CONCURRENCY} en parallèle.`);

  for (let i = 0; i < storesToProcess.length; i += CONCURRENCY) {
    if (hasReachedTimeLimit()) {
      console.log(
        `[SCRAPER] Limite atteinte avant le lancement du batch ${i / CONCURRENCY + 1}. Arrêt anticipé du shard.`
      );
      break;
    }
    const batch = storesToProcess.slice(i, i + CONCURRENCY);

    console.log(
      `[SCRAPER] Batch ${i / CONCURRENCY + 1} – magasins index ${i} à ${i + batch.length - 1}`
    );

    await Promise.all(
      batch.map((store) => {
        if (hasReachedTimeLimit()) {
          console.log(
            `[SCRAPER] Limite atteinte avant le magasin ${store.storeId}. Arrêt du lancement de ce magasin.`
          );
          return Promise.resolve();
        }

        return scrapeStore(store).catch((err) => {
          console.error("[SCRAPER] Erreur dans un magasin :", err);
        });
      })
    );
  }

  console.log("[SCRAPER] Shard done - exiting.");
}

run().catch((err) => {
  console.error("[SCRAPER] Erreur fatale :", err);
  process.exit(1);
});
