import json
import time
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType

def setup_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    options.binary_location = '/usr/bin/chromium'
    
    driver = webdriver.Chrome(options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def generate_session_id():
    return f"wired_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def scrape_wired_articles(driver, min_articles=50):
    print(f"Navigating to Wired.com...")
    driver.get("https://www.wired.com/")
    
    wait = WebDriverWait(driver, 15)
    
    print("Waiting for page to load...")
    time.sleep(random.uniform(2, 4))
    
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    
    articles = []
    seen_urls = set()
    
    article_selectors = [
        'a[data-testid="SummaryItemWrapper"]',
        'a.summary-item__href',
        'article a',
        '.summary-item a',
        'a[href*="/story/"]',
        'a[href*="/review/"]',
        'h3 a',
        '.card a',
        '.FeedItem a',
        'a.post-preview',
        'a[href*="wired.com/story"]',
    ]
    
    print("Scrolling to load more content...")
    for scroll in range(5):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(1.5, 3))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
        time.sleep(random.uniform(1, 2))
    
    print(f"Looking for articles using multiple selectors...")
    
    all_links = []
    for selector in article_selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elements:
                try:
                    href = elem.get_attribute('href')
                    if href and 'wired.com' in href and ('/story/' in href or '/review/' in href) and href not in seen_urls:
                        all_links.append(elem)
                        seen_urls.add(href)
                except:
                    continue
        except:
            continue
    
    print(f"Found {len(all_links)} unique article links")
    
    for idx, link_elem in enumerate(all_links[:min_articles + 20]):
        try:
            article_data = {}
            
            article_data['url'] = link_elem.get_attribute('href')
            
            try:
                article_data['title'] = link_elem.text.strip()
                if not article_data['title']:
                    parent = link_elem.find_element(By.XPATH, "./ancestor::article")
                    title_elem = parent.find_element(By.CSS_SELECTOR, 'h3, h2, .summary-title')
                    article_data['title'] = title_elem.text.strip()
            except:
                article_data['title'] = f"Article {idx + 1}"
            
            if not article_data['title'] or len(article_data['title']) < 5:
                article_data['title'] = f"Wired Article {idx + 1}"
            
            article_data['scraped_at'] = datetime.now().isoformat()
            article_data['source'] = "Wired.com"
            
            try:
                parent = link_elem.find_element(By.XPATH, "./ancestor::article")
                try:
                    desc_elem = parent.find_element(By.CSS_SELECTOR, '.summary-description, .summarydek, p')
                    article_data['description'] = desc_elem.text.strip()
                except:
                    article_data['description'] = ""
                
                try:
                    author_elem = parent.find_element(By.CSS_SELECTOR, '.summary-author, .byline, [class*="author"]')
                    author_text = author_elem.text.strip()
                    if author_text and not author_text.startswith('By'):
                        author_text = "By" + author_text
                    article_data['author'] = author_text if author_text else "ByUnknown"
                except:
                    article_data['author'] = "ByUnknown"
                    
            except:
                article_data['description'] = ""
                article_data['author'] = "ByUnknown"
            
            if article_data['url'] and '/story/' in article_data['url'] or '/review/' in article_data['url']:
                articles.append(article_data)
                print(f"  [{len(articles)}] {article_data['title'][:50]}...")
            
            if len(articles) >= min_articles:
                break
                
        except Exception as e:
            print(f"  Error processing article {idx}: {str(e)[:50]}")
            continue
    
    return articles

def main():
    print("=" * 60)
    print("WIRED.COM ARTICLE SCRAPER")
    print("=" * 60)
    
    driver = None
    try:
        driver = setup_driver()
        
        articles = scrape_wired_articles(driver, min_articles=50)
        
        if len(articles) < 20:
            print(f"\nWarning: Only got {len(articles)} articles. Trying alternative method...")
            
            driver.get("https://www.wired.com/category/story/")
            time.sleep(3)
            
            for scroll in range(8):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
            
            links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/story/"]')
            seen = set()
            
            for link in links:
                try:
                    url = link.get_attribute('href')
                    if url and url not in seen and 'wired.com/story/' in url:
                        seen.add(url)
                        if len(articles) >= 55:
                            break
                        articles.append({
                            'title': link.text.strip() or f"Wired Article {len(articles) + 1}",
                            'url': url,
                            'description': '',
                            'author': 'ByUnknown',
                            'scraped_at': datetime.now().isoformat(),
                            'source': 'Wired.com'
                        })
                except:
                    continue
        
        print(f"\n{'=' * 60}")
        print(f"Total articles collected: {len(articles)}")
        
        output = {
            "session_id": generate_session_id(),
            "timestamp": datetime.now().isoformat(),
            "articles_count": len(articles),
            "articles": articles[:55]
        }
        
        output_file = "wired_articles.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"Data saved to: {output_file}")
        print(f"{'=' * 60}")
        
        print("\nSample articles:")
        for i, art in enumerate(output['articles'][:3]):
            print(f"\n{i+1}. {art.get('title', 'N/A')}")
            print(f"   URL: {art.get('url', 'N/A')}")
            print(f"   Author: {art.get('author', 'N/A')}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        if driver:
            driver.quit()
            print("\nBrowser closed.")

if __name__ == "__main__":
    main()
