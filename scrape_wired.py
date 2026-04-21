import json
import time
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service


def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--single-process")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-images")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.binary_location = "/usr/bin/chromium"
    options.page_load_strategy = "none"

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(15)
    return driver


def generate_session_id():
    return f"wired_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def scrape_wired_simple(driver, min_articles=50):
    print(f"Loading Wired.com...")
    driver.get("https://www.wired.com/")
    time.sleep(4)

    articles = []
    seen = set()

    selectors = [
        'a[data-testid="SummaryItemWrapper"]',
        "a.summary-item__href",
        'a[href*="/story/"]',
    ]

    print("Scrolling...")
    for _ in range(3):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)

    print("Collecting links...")
    for sel in selectors:
        try:
            elems = driver.find_elements(By.CSS_SELECTOR, sel)
            for e in elems:
                try:
                    href = e.get_attribute("href")
                    txt = e.text.strip()
                    if href and "wired.com/story/" in href and href not in seen:
                        if txt and len(txt) > 5:
                            articles.append({"url": href, "title": txt[:200]})
                            seen.add(href)
                except:
                    continue
        except:
            continue

    print(f"Found {len(articles)} from homepage")

    if len(articles) < min_articles:
        driver.get("https://www.wired.com/category/story/")
        time.sleep(3)

        for _ in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

        for sel in selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, sel)
                for e in elems:
                    try:
                        href = e.get_attribute("href")
                        txt = e.text.strip()
                        if href and "wired.com/story/" in href and href not in seen:
                            if txt and len(txt) > 5:
                                articles.append({"url": href, "title": txt[:200]})
                                seen.add(href)
                    except:
                        continue
            except:
                continue

    print(f"Total: {len(articles)}")

    result = []
    urls_to_fetch = articles[: min(55, len(articles))]

    print("Fetching author info...")
    driver.set_page_load_timeout(10)

    for i, art in enumerate(urls_to_fetch):
        try:
            driver.get(art["url"])
            time.sleep(0.6)

            author = "By Unknown"
            try:
                m = driver.find_element(By.CSS_SELECTOR, 'meta[name="author"]')
                c = m.get_attribute("content")
                if c:
                    author = "By " + c.strip()
            except:
                try:
                    m = driver.find_element(
                        By.CSS_SELECTOR, 'meta[property="article:author"]'
                    )
                    c = m.get_attribute("content")
                    if c:
                        author = "By " + c.strip()
                except:
                    pass

            desc = ""
            try:
                d = driver.find_element(By.CSS_SELECTOR, 'meta[name="description"]')
                desc = d.get_attribute("content") or ""
            except:
                pass

            result.append(
                {
                    "url": art["url"],
                    "title": art["title"],
                    "author": author,
                    "description": desc,
                    "scraped_at": datetime.now().isoformat(),
                    "source": "Wired.com",
                }
            )

            if (i + 1) % 5 == 0:
                print(f"  Processed {i + 1}")

        except:
            result.append(
                {
                    "url": art["url"],
                    "title": art["title"],
                    "author": "By Unknown",
                    "description": "",
                    "scraped_at": datetime.now().isoformat(),
                    "source": "Wired.com",
                }
            )

    while len(result) < min_articles and len(articles) > len(result):
        remaining = [a for a in articles if a["url"] not in [r["url"] for r in result]]
        if remaining:
            art = remaining[0]
            result.append(
                {
                    "url": art["url"],
                    "title": art["title"],
                    "author": "By Unknown",
                    "description": "",
                    "scraped_at": datetime.now().isoformat(),
                    "source": "Wired.com",
                }
            )
        else:
            break

    return result


def main():
    print("=" * 50)
    print("WIRED SCRAPER - FAST")
    print("=" * 50)

    driver = setup_driver()
    try:
        articles = scrape_wired_simple(driver, 50)
        print(f"\nTotal: {len(articles)} articles")

        known = sum(1 for a in articles if a["author"] != "By Unknown")
        print(f"With author: {known}")

        output = {
            "session_id": generate_session_id(),
            "timestamp": datetime.now().isoformat(),
            "articles_count": len(articles),
            "articles": articles,
        }

        with open("wired_articles.json", "w") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print("Saved!")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()

