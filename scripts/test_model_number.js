import { chromium } from "playwright";
import { extractModelDataFromPage } from "../lib/ctModelNumber.js";

const URLS = [
  "https://www.canadiantire.ca/en/pdp/mastercraft-20v-max-li-ion-cordless-drill-driver-0542435p.html",
  "https://www.canadiantire.ca/en/pdp/thermacell-mr300-mosquito-repellent-0598385p.html",
  "https://www.canadiantire.ca/en/pdp/black-decker-20v-max-lithium-cordless-drill-0542461p.html",
];

async function run() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ locale: "fr-CA" });
  context.setDefaultTimeout(0);

  const failures = [];

  try {
    for (const url of URLS) {
      const page = await context.newPage();
      page.setDefaultNavigationTimeout(0);
      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120000 });
        const data = await extractModelDataFromPage(page);
        console.log(`[TEST] ${url} →`, data);
        if (!data.model_number) {
          failures.push(url);
        }
      } finally {
        await page.close().catch(() => {});
      }
    }
  } finally {
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }

  if (failures.length) {
    throw new Error(`model_number manquant pour: ${failures.join(", ")}`);
  }
}

run().catch((error) => {
  console.error("[TEST] Échec du test model_number:", error);
  process.exit(1);
});
