import os
import logging
from datetime import datetime
from datetime import timedelta
import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
import time
from requests.exceptions import Timeout
import functools

# Set up logging
current_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(current_dir, 'LOGS')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'stock_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(filename=log_file, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def extract_id_from_url(url):
    try:
        start_index = url.index("zid") + 3
        zid_part = url[start_index:]
        return zid_part
    except ValueError:
        return None

def retry_on_db_locked(max_attempts=5, retry_delay=1):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < max_attempts - 1:
                        logging.warning(f"Database locked, retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        raise
            raise sqlite3.OperationalError("Unable to access the database after multiple attempts")
        return wrapper
    return decorator

@retry_on_db_locked()
def get_db_connection(db_name="Sharkninja.db"):
    conn = sqlite3.connect(db_name, timeout=20)
    return conn

@retry_on_db_locked()
def get_or_create_id(table_name, column_name, value, db_name="Sharkninja.db"):
    conn = get_db_connection(db_name)
    try:
        cursor = conn.cursor()
        
        if table_name == "Countries":
            id_column = "CountryID"
        elif table_name.endswith('s'):
            id_column = f"{table_name[:-1]}ID"
        else:
            id_column = f"{table_name}ID"
        
        cursor.execute(f"SELECT {id_column} FROM {table_name} WHERE {column_name} = ?", (value,))
        result = cursor.fetchone()
        
        if result:
            id = result[0]
        else:
            cursor.execute(f"INSERT INTO {table_name} ({column_name}) VALUES (?)", (value,))
            id = cursor.lastrowid
        
        conn.commit()
        return id
    finally:
        conn.close()

@retry_on_db_locked()
def get_or_create_product_id(sku, product_name, db_name="Sharkninja.db"):
    conn = get_db_connection(db_name)
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT ProductID FROM Products WHERE SKU = ?", (sku,))
        result = cursor.fetchone()
        
        if result:
            product_id = result[0]
        else:
            # cursor.execute("INSERT INTO Products (SKU, ProductName) VALUES (?, ?)", (sku, product_name))
            #product_id = cursor.lastrowid
            print("no id")
        conn.commit()
        return product_id
    finally:
        conn.close()

@retry_on_db_locked()
def save_to_db(df, language, brand, db_name="Sharkninja.db"):
    conn = get_db_connection(db_name)
    try:
        cursor = conn.cursor()
        
        country_id = get_or_create_id("Countries", "CountryCode", language)
        brand_id = get_or_create_id("Brands", "BrandName", brand)
        
        for _, row in df.iterrows():
            product_id = get_or_create_product_id(row['SKU'], row['Product Name'])
            
            cursor.execute("""
                INSERT OR REPLACE INTO ProductStatus 
                (ProductID, CountryID, BrandID, Date, Status, Type, CurrentPrice)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (product_id, country_id, brand_id, row['Date'], row['Status'], row['Type'], row['Current Price']))
        
        conn.commit()
    finally:
        conn.close()

@retry_on_db_locked()
def save_prices_to_db(df, language, db_name="Sharkninja.db"):
    conn = get_db_connection(db_name)
    try:
        cursor = conn.cursor()
        
        country_id = get_or_create_id("Countries", "CountryCode", language)
        
        for _, row in df.iterrows():
            product_id = get_or_create_product_id(row['SKU'], row['Product Name'])
            date_obj = datetime.strptime(row['Date'], "%Y-%m-%d %H:%M:%S")
            formatted_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
            
            price_str = row['Current Price'].replace('€', '').strip()
            current_price = float(price_str.replace(',', '.'))
            
            # Get the last price record for this product and country
            cursor.execute("""
                SELECT Price FROM Prices
                WHERE ProductID = ? AND CountryID = ?
                ORDER BY EntryDate DESC LIMIT 1
            """, (product_id, country_id))
            last_price_record = cursor.fetchone()
            
            if last_price_record is None or current_price != last_price_record[0]:
                # Insert the new price only if it's different or there's no previous record
                cursor.execute("""
                    INSERT INTO Prices (ProductID, CountryID, Price, EntryDate, Reason)
                    VALUES (?, ?, ?, ?, ?)
                """, (product_id, country_id, current_price, formatted_date, "New price recorded"))
                
                logging.info(f"New price recorded for ProductID {product_id}. Price: {current_price}")
            else:
                logging.info(f"Price unchanged for ProductID {product_id}. Current: {current_price}")
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error in save_prices_to_db: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

def check_availability(url):
    try:
        response = requests.get(url, headers=headers, timeout=3)
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
        
    except (Timeout, requests.RequestException) as e:
        logging.error(f"Error fetching {url}: {e}")
    
    return None

def process_urls(urls):
    out_of_stock_products = []
    in_stock_products = []
    skipped_urls = []
    processed_products = set()

    for url in urls:
        result = check_availability(url)
        if result:
            if result[4] == "OUT" and result[0] not in processed_products:
                out_of_stock_products.append(result)
                processed_products.add(result[0])
            elif result[4] == "IN" and result[0] not in processed_products:
                in_stock_products.append(result)
                processed_products.add(result[0])
        else:
            skipped_urls.append(url)

    return out_of_stock_products, in_stock_products, skipped_urls, processed_products

def fetch_urls_from_database(db_name="Sharkninja.db"):
    conn = get_db_connection(db_name)
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
    if "ninjakitchen.be" in url:
        return "BE", "Ninja"
    elif "sharkclean.be" in url:
        return "BE", "Shark"
    elif "ninjakitchen.nl" in url:
        return "NL", "Ninja"
    elif "sharkclean.nl" in url:
        return "NL", "Shark"
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

def check_stock(grouped_urls):
    for category, urls in grouped_urls.items():
        language = category[:2]
        brand = category[2:]
        
        logging.info(f"Checking stock for {language} {brand}")
        
        out_of_stock_df = pd.DataFrame(columns=["SKU", "Product Name", "Date", "URL", "Status", "Type", "Current Price"])
        in_stock_df = pd.DataFrame(columns=["SKU", "Product Name", "Date", "URL", "Status", "Type", "Current Price"])

        logging.info("Processing URLs...")
        (out_of_stock_products, in_stock_products, skipped_urls, processed_products,) = process_urls(urls)

        remaining_skipped = skipped_urls  # Initialize remaining_skipped here

        if skipped_urls:
            logging.info(f"Rerunning {len(skipped_urls)} skipped URLs...")
            (additional_out_of_stock, additional_in_stock, remaining_skipped, processed_products,) = process_urls(skipped_urls)

            out_of_stock_products.extend(additional_out_of_stock)
            in_stock_products.extend(additional_in_stock)

        if remaining_skipped:
            logging.info(f"Re-rerunning {len(remaining_skipped)} skipped URLs...")
            (additional_out_of_stock, additional_in_stock, final_skipped, processed_products,) = process_urls(remaining_skipped)

            out_of_stock_products.extend(additional_out_of_stock)
            in_stock_products.extend(additional_in_stock)
            remaining_skipped = final_skipped

        if out_of_stock_products:
            out_of_stock_df = pd.DataFrame(
                out_of_stock_products,
                columns=["SKU", "Product Name", "Date", "URL", "Status", "Type", "Current Price"],
            )
            save_to_db(out_of_stock_df, language, brand)
            logging.info(f"Updated {len(out_of_stock_products)} out-of-stock products for {language} {brand}")
        else:
            logging.info(f"All products are in stock for {language} {brand}")

        if in_stock_products:
            in_stock_df = pd.DataFrame(
                in_stock_products,
                columns=["SKU", "Product Name", "Date", "URL", "Status", "Type", "Current Price"],
            )
            save_to_db(in_stock_df, language, brand)
            logging.info(f"Updated {len(in_stock_products)} in-stock products for {language} {brand}")
        else:
            logging.info(f"No products are in stock for {language} {brand}")

        all_products_df = pd.concat([out_of_stock_df, in_stock_df], ignore_index=True)
        if not all_products_df.empty:
            save_to_db(all_products_df, language, brand)
            save_prices_to_db(all_products_df, language)
            logging.info(f"Saved product status and prices for {len(all_products_df)} products for {language} {brand}")
        else:
            logging.warning(f"No products found to save for {language} {brand}")

        if remaining_skipped:
            logging.warning(f"The following URLs were skipped for {language} {brand}:")
            for url in remaining_skipped:
                logging.warning(url)
            logging.warning(f"{len(remaining_skipped)} URLs were skipped after retrying for {language} {brand}")
            logging.warning(f"{len(skipped_urls)} URLs were skipped after retrying for {language} {brand}")
        else:
            logging.info(f"No URLs were skipped in the end for {language} {brand}")

        logging.info(f"Finished checking {language} {brand}")
        logging.info("-----------------------------------")

def main():
    logging.info("Starting stock check")
    urls = fetch_urls_from_database()
    grouped_urls = group_urls_by_category(urls)
    check_stock(grouped_urls)
    logging.info("Finished stock check for all URLs")

if __name__ == "__main__":
    main()