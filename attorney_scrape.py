
"""
Scraper for Family Law Software's professional directory.

Outputs (CSV columns):
- state, name, address, county, phone

Features:
- Async Playwright + concurrency across states (--concurrency N).
- Incremental CSV writing.
- Limit details per state (--max-details-per-state) for a quick first pass/testing.
- Waits for Vue render & "Search is in progress" spinner.

Performance notes:
- Some states can return 200+ results. That will take time because every profile
  requires a details click and page transition.
- Future recommendation for large states (≥200 results):
  1) Index downstream storage by state to speed later lookups.
  2) Explore per-state worker expansion 

Usage:
  python attorney_scrape.py --states CA,NY,TX --out sample.csv --concurrency 3
  python attorney_scrape.py --out all.csv --concurrency 6 --max-details-per-state 200
"""

import asyncio
import csv
import random
import re
import time
from pathlib import Path
from urllib.parse import urlencode
from typing import List, Dict, Optional

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# 50 states + DC
STATE_CODES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS",
    "KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC",
    "ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]

BASE = "https://site.familylawsoftware.com/client/professional_directory/"
RESULTS_URL = BASE + "submit.html"
DETAILS_PATH_RE = re.compile(r"/client/professional_directory/pro_details\.html", re.I)

# Only these 5 columns will be written
FIELDNAMES_OUT = ["state", "name", "address", "county", "phone"]

HEAVY_STATE_THRESHOLD = 200  # when hit, warn it will take time + suggest indexing/worker expansion

def search_url(state_code: str, profession: str = "attorney") -> str:
    params = {"profession": profession, "state": state_code, "timestamp": int(time.time() * 1000)}
    return RESULTS_URL + "?" + urlencode(params)

class AsyncCSVWriter:
    def __init__(self, path: Path, fieldnames: List[str]):
        self.path = path
        self.fieldnames = fieldnames
        self._lock = asyncio.Lock()
        self._file = None
        self._writer = None

    async def __aenter__(self):
        exists = self.path.exists()
        self._file = self.path.open("a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=self.fieldnames)
        if not exists or self.path.stat().st_size == 0:
            self._writer.writeheader()
            self._file.flush()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._file:
            self._file.flush()
            self._file.close()

    async def write_row(self, row: Dict):
        clean = {k: row.get(k, "") for k in self.fieldnames}
        async with self._lock:
            self._writer.writerow(clean)
            self._file.flush()

async def wait_until_results_ready(page, timeout_ms=120_000) -> int:
    """
    Wait for #vueContent_proFind, wait for 'Search is in progress' to hide,
    then wait for the 'More info...' link count to stabilize.

    Returns: stabilized count (0 allowed).
    """
    await page.wait_for_selector("#vueContent_proFind", timeout=timeout_ms)
    container = page.locator("#vueContent_proFind")

    spinner = container.get_by_text("Search is in progress", exact=False)
    try:
        await spinner.wait_for(state="visible", timeout=5000)
        await spinner.wait_for(state="hidden", timeout=timeout_ms)
    except PlaywrightTimeout:
        # spinner never showed; OK
        pass

    start = time.monotonic()
    last_count = -1
    stable_since = None

    while (time.monotonic() - start) * 1000 < timeout_ms:
        links = container.locator("a.btn.btn-link").filter(has_text=re.compile(r"More info", re.I))
        try:
            count = await links.count()
        except Exception:
            count = 0

        try:
            txt = (await container.inner_text()).lower()
        except Exception:
            txt = ""
        if "there are currently no" in txt:
            return 0

        if count > 0:
            if count == last_count:
                if stable_since and (time.monotonic() - stable_since) >= 1.0:
                    return count
                if stable_since is None:
                    stable_since = time.monotonic()
            else:
                last_count = count
                stable_since = None

        await asyncio.sleep(0.25)

    raise PlaywrightTimeout("Results did not become ready in time")

def _extract_field(text: str, label: str) -> str:
    m = re.search(rf"{re.escape(label)}\s*:\s*(.+)", text, flags=re.I)
    return m.group(1).strip() if m else ""

async def scrape_detail_page(page) -> Dict:
    """
    On a details page, pull just what we need.
    Returns: dict with name, address, county, phone
    """
    await page.wait_for_selector("#vueContent_proDetails", timeout=30_000)
    container = page.locator("#vueContent_proDetails")

    p_nodes = container.locator("p")
    p_texts = []
    try:
        cnt = await p_nodes.count()
        for i in range(cnt):
            try:
                p_texts.append((await p_nodes.nth(i).inner_text()).strip())
            except Exception:
                pass
    except Exception:
        pass

    name = p_texts[0].strip() if p_texts else ""

    address = ""
    for t in p_texts[1:]:
        if re.search(r"\d", t) and ("," in t or re.search(r"\b\d{5}(-\d{4})?\b", t)):
            address = t.strip()
            break

    body_text = (await container.inner_text()).strip()
    county   = _extract_field(body_text, "County")
    phone    = _extract_field(body_text, "Phone")

    return {"name": name, "address": address, "county": county, "phone": phone}

async def scrape_state(context, state_code: str, writer: AsyncCSVWriter,
                       max_details_per_state: Optional[int],
                       polite_min=0.10, polite_max=0.30, verbose_prefix=""):
    page = await context.new_page()
    list_url = search_url(state_code)

    try:
        await page.goto(list_url, timeout=60_000)
        await page.wait_for_load_state("domcontentloaded")
        # DO NOT await these; they are setters returning None.
        page.set_default_timeout(60_000)
        page.set_default_navigation_timeout(60_000)

        count = await wait_until_results_ready(page, timeout_ms=120_000)

        if count == 0:
            print(f"{verbose_prefix}[INFO] {state_code}: 0 profiles")
            return

        print(f"{verbose_prefix}[INFO] {state_code}: found {count} profiles")

        if count >= HEAVY_STATE_THRESHOLD:
            print(
                f"{verbose_prefix}[NOTE] {state_code}: {count} profiles — this will take time.\n"
                f"{verbose_prefix}       Consider indexing downstream data by state and exploring "
                f"per-state worker expansion (e.g., sharding by alphabet or pagination) for counts ≥ {HEAVY_STATE_THRESHOLD}. "
                f"For a first pass, use --max-details-per-state."
            )

        # Details pass
        container = page.locator("#vueContent_proFind")
        links = container.locator("a.btn.btn-link").filter(has_text=re.compile(r"More info", re.I))
        total = await links.count()

        if max_details_per_state is not None:
            total = min(total, max_details_per_state)

        for i in range(total):
            # Re-fetch locators after each navigation
            container = page.locator("#vueContent_proFind")
            links = container.locator("a.btn.btn-link").filter(has_text=re.compile(r"More info", re.I))

            n_now = await links.count()
            if n_now < (i + 1):
                try:
                    _ = await wait_until_results_ready(page, timeout_ms=60_000)
                    links = container.locator("a.btn.btn-link").filter(has_text=re.compile(r"More info", re.I))
                except PlaywrightTimeout:
                    print(f"{verbose_prefix}[WARN] {state_code}: list re-rendered; skipping remaining")
                    break

            links_n = links.nth(i)
            await links_n.scroll_into_view_if_needed()

            # Prefer explicit navigation; fall back if needed
            try:
                async with page.expect_navigation(url=DETAILS_PATH_RE, wait_until="domcontentloaded", timeout=30_000):
                    await links_n.click()
            except PlaywrightTimeout:
                try:
                    await links_n.click()
                    await page.wait_for_selector("#vueContent_proDetails", timeout=30_000)
                except PlaywrightTimeout:
                    print(f"{verbose_prefix}[WARN] {state_code} item {i+1}: details never appeared; skipping")
                    continue

            try:
                detail = await scrape_detail_page(page)
                row = {"state": state_code, **detail}  # only these 5 will be written
                await writer.write_row(row)
            except Exception as e:
                print(f"{verbose_prefix}[WARN] {state_code} item {i+1}: detail scrape failed: {e}")

            # Go back and wait for list
            try:
                await page.go_back(wait_until="domcontentloaded")
                await wait_until_results_ready(page, timeout_ms=60_000)
            except Exception as e:
                print(f"{verbose_prefix}[WARN] {state_code}: go_back failed ({e}); reloading")
                await page.goto(list_url, timeout=60_000)
                await wait_until_results_ready(page, timeout_ms=60_000)

            await asyncio.sleep(random.uniform(polite_min, polite_max))

    finally:
        await page.close()

async def state_worker(worker_id: int, browser, states_q: asyncio.Queue, writer: AsyncCSVWriter,
                       max_details_per_state: Optional[int]):
    context = await browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36 data-research/1.0"),
        viewport={"width": 1280, "height": 900},
    )
    # Setters are sync (no await).
    context.set_default_timeout(60_000)
    context.set_default_navigation_timeout(60_000)

    prefix = f"[W{worker_id}] "
    try:
        while True:
            state_code = await states_q.get()
            try:
                print(f"{prefix}Scraping {state_code} …")
                await scrape_state(
                    context, state_code, writer,
                    max_details_per_state=max_details_per_state,
                    verbose_prefix=prefix
                )
                print(f"{prefix}{state_code} done")
            except Exception as e:
                print(f"{prefix}[ERROR] {state_code} failed: {e}")
            finally:
                states_q.task_done()
                await asyncio.sleep(0.2)
    finally:
        await context.close()

async def amain(output_csv: str, states: List[str], concurrency: int,
                headless: bool, slowmo: int,
                max_details_per_state: Optional[int]):

    out_path = Path(output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    async with AsyncCSVWriter(out_path, FIELDNAMES_OUT) as writer:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless, slow_mo=slowmo)

            states_q = asyncio.Queue()
            for s in states:
                await states_q.put(s)

            workers = []
            concurrency = max(1, min(concurrency, len(states)))
            for i in range(concurrency):
                workers.append(asyncio.create_task(
                    state_worker(i+1, browser, states_q, writer, max_details_per_state)
                ))

            await states_q.join()

            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            await browser.close()

def parse_states_arg(arg: str) -> List[str]:
    if not arg:
        return STATE_CODES
    parts = [s.strip().upper() for s in arg.split(",") if s.strip()]
    valid = [s for s in parts if s in STATE_CODES]
    if not valid:
        raise SystemExit("No valid state codes provided.")
    return valid

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="Scrape Family Law Software professional directory (attorneys) to CSV.",
        epilog=(
            f"Note: Large states (≥{HEAVY_STATE_THRESHOLD} results) take time. "
            "We recommend indexing downstream data by state and exploring per-state "
            "worker expansion; for quick coverage, use --max-details-per-state."
        ),
    )
    ap.add_argument("--out", default="attorneys.csv", help="Output CSV path")
    ap.add_argument("--states", default="", help="Comma-separated list (e.g., CA,NY,TX). Blank = all 50 + DC.")
    ap.add_argument("--concurrency", type=int, default=3, help="Parallel states to scrape at once")
    ap.add_argument("--headful", action="store_true", help="Visible browser (default headless)")
    ap.add_argument("--slowmo", type=int, default=0, help="Slow motion ms (debugging)")
    ap.add_argument("--max-details-per-state", type=int, default=None, help="Cap number of details clicked per state")
    args = ap.parse_args()

    states = parse_states_arg(args.states)
    asyncio.run(amain(
        output_csv=args.out,
        states=states,
        concurrency=args.concurrency,
        headless=not args.headful,
        slowmo=args.slowmo,
        max_details_per_state=args.max_details_per_state
    ))
