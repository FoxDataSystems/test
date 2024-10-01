import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import time
from requests.exceptions import Timeout
import logging
import os

# Create LOGS folder if it doesn't exist
if not os.path.exists("LOGS"):
    os.makedirs("LOGS")

# Set up logging
log_file = os.path.join("LOGS", f"sharkninja_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Constants
DB_NAME = "Sharkninja.db"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
TIMEOUT_SECONDS = 5

def extract_id_from_url(url):
    try:
        start_index = url.index("zid") + 3
        return url[start_index:]
    except ValueError:
        return None

def fetch_urls_from_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM urls")
    urls = cursor.fetchall()
    conn.close()
    return [url[0] for url in urls]

def categorize_url(url):
    if "ninjakitchen.fr" in url:
        return "FR", "Ninja"
    elif "sharkclean.fr" in url:
        return "FR", "Shark"
    elif "ninjakitchen.es" in url:
        return "ES", "Ninja"
    elif "sharkclean.es" in url:
        return "ES", "Shark"
    elif "ninjakitchen.nl" in url or "ninjakitchen.be" in url:
        return "NL" if "ninjakitchen.nl" in url else "BE", "Ninja"
    elif "sharkclean.nl" in url or "sharkclean.be" in url:
        return "NL" if "sharkclean.nl" in url else "BE", "Shark"
    else:
        return None, None

def group_urls_by_category(urls):
    grouped_urls = {}
    for url in urls:
        country, brand = categorize_url(url)
        if country and brand:
            key = f"{country}{brand}"
            if key not in grouped_urls:
                grouped_urls[key] = []
            grouped_urls[key].append(url)
    return grouped_urls

def check_availability(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        out_of_stock_button = soup.find("button", class_="js-btn_out-of-stock", title="Niet op voorraad")
        out_of_stock_button_fr = soup.find("button", class_="js-btn_out-of-stock", title="Stock épuisé")
        add_to_cart_button = soup.find("button", title="Ajouter au panier")
        add_to_cart_button_nl = soup.find("button", title="Toevoegen aan winkelmandje")
        product_name_tag = soup.find("h1", class_="js-product-title js-make-bold")
        price_tag = soup.find("div", attrs={"data-testing-id": "current-price"})

        if product_name_tag:
            product_name = product_name_tag.get_text(strip=True)
            product_type = "Ninja" if "ninja" in product_name.lower() else "Shark"
            zid_part = extract_id_from_url(url)
            current_price = price_tag.text.strip() if price_tag else "N/A"
            current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if out_of_stock_button or out_of_stock_button_fr:
                return (zid_part, product_name, current_date, url, "OUT", product_type, current_price)
            elif add_to_cart_button or add_to_cart_button_nl:
                return (zid_part, product_name, current_date, url, "IN", product_type, current_price)
            else:
                return (zid_part, product_name, current_date, url, "IN", product_type, current_price)
    except Timeout:
        logging.warning(f"Timeout occurred for URL: {url}")
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
    return None

def save_to_db(products):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    for product in products:
        try:
            sku, product_name, date, url, status, product_type, current_price = product
            country, brand = categorize_url(url)

            cursor.execute("INSERT OR IGNORE INTO Countries (CountryCode) VALUES (?)", (country,))
            cursor.execute("SELECT CountryID FROM Countries WHERE CountryCode = ?", (country,))
            country_id = cursor.fetchone()[0]

            cursor.execute("INSERT OR IGNORE INTO Brands (BrandName) VALUES (?)", (brand,))
            cursor.execute("SELECT BrandID FROM Brands WHERE BrandName = ?", (brand,))
            brand_id = cursor.fetchone()[0]

            cursor.execute("INSERT OR IGNORE INTO Products (SKU, ProductName) VALUES (?, ?)", (sku, product_name))
            cursor.execute("SELECT ProductID FROM Products WHERE SKU = ?", (sku,))
            product_id = cursor.fetchone()[0]

            cursor.execute("INSERT OR IGNORE INTO URLs (URL) VALUES (?)", (url,))
            cursor.execute("SELECT rowid FROM URLs WHERE URL = ?", (url,))
            url_id = cursor.fetchone()[0]

            cursor.execute("INSERT OR IGNORE INTO SKU_URL (SKUID, URLID) VALUES (?, ?)", (product_id, url_id))

            cursor.execute("""
            INSERT OR REPLACE INTO ProductStatus 
            (ProductID, CountryID, BrandID, Date, Status, Type, CurrentPrice)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (product_id, country_id, brand_id, date, status, product_type, current_price))

        except Exception as e:
            logging.error(f"Error saving data for SKU {sku}: {e}")
            cursor.execute("""
            INSERT INTO FailedInserts 
            (SKU, ProductName, Country, Brand, Date, URL, Status, Type, CurrentPrice, ErrorMessage)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (sku, product_name, country, brand, date, url, status, product_type, current_price, str(e)))

    conn.commit()
    conn.close()

def save_prices_to_db(products):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    for product in products:
        sku, _, date_str, url, _, _, price_str = product
        country, _ = categorize_url(url)
        
        date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        formatted_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
        
        price_str = price_str.replace('€', '').strip()
        current_price = float(price_str.replace(',', '.'))
        
        cursor.execute("SELECT ProductID FROM Products WHERE SKU = ?", (sku,))
        product_id = cursor.fetchone()[0]
        cursor.execute("SELECT CountryID FROM Countries WHERE CountryCode = ?", (country,))
        country_id = cursor.fetchone()[0]

        cursor.execute("""
            SELECT Price, EntryDate FROM Prices
            WHERE ProductID = ? AND CountryID = ?
            ORDER BY EntryDate DESC LIMIT 1
        """, (product_id, country_id))
        last_price_record = cursor.fetchone()
        
        if last_price_record:
            last_price, last_date = last_price_record
            last_date = datetime.strptime(last_date, "%Y-%m-%d")
            
            if current_price != last_price:
                yesterday = date_obj - timedelta(days=1)
                cursor.execute("""
                    INSERT INTO Prices (ProductID, CountryID, Price, EntryDate, Reason)
                    VALUES (?, ?, ?, ?, ?)
                """, (product_id, country_id, f"{last_price:.2f}", yesterday.strftime("%Y-%m-%d"), "Last recorded price"))
                
                cursor.execute("""
                    INSERT INTO Prices (ProductID, CountryID, Price, EntryDate, Reason)
                    VALUES (?, ?, ?, ?, ?)
                """, (product_id, country_id, f"{current_price:.2f}", formatted_date, "New scraped price"))
        else:
            cursor.execute("""
                INSERT INTO Prices (ProductID, CountryID, Price, EntryDate, Reason)
                VALUES (?, ?, ?, ?, ?)
            """, (product_id, country_id, f"{current_price:.2f}", formatted_date, "First recorded price"))
    
    conn.commit()
    conn.close()

def main():
    logging.info("Starting Sharkninja scraper")
    while True:
        try:
            urls = fetch_urls_from_database()
            grouped_urls = group_urls_by_category(urls)

            for category, category_urls in grouped_urls.items():
                logging.info(f"Processing URLs for {category}...")
                products = []
                skipped_urls = []

                for url in category_urls:
                    result = check_availability(url)
                    if result:
                        products.append(result)
                    else:
                        skipped_urls.append(url)

                if skipped_urls:
                    logging.warning(f"Rerunning {len(skipped_urls)} skipped URLs for {category}...")
                    for url in skipped_urls:
                        result = check_availability(url)
                        if result:
                            products.append(result)

                if products:
                    save_to_db(products)
                    save_prices_to_db(products)
                    logging.info(f"Updated {len(products)} products for {category}")
                else:
                    logging.info(f"No products found for {category}")

            # Sleep for a day before the next run
            logging.info("Sleeping for 24 hours before the next run...")
            time.sleep(86400)  # 24 hours in seconds

        except Exception as e:
            logging.error(f"An error occurred in the main loop: {e}")
            time.sleep(3600)  # Sleep for an hour before retrying

if __name__ == "__main__":
    main()