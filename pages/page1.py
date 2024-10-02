from navigation import make_sidebar
import streamlit as st
import pandas as pd
from datetime import datetime
from datetime import timedelta
import io
from bs4 import BeautifulSoup
import requests
import time
import plotly.express as px
import plotly.graph_objects as go
from requests.exceptions import Timeout
import logging
import os
import pymssql

# Create LOGS folder if it doesn't exist
if not os.path.exists('LOGS'):
    os.makedirs('LOGS')

# Set up logging
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = f'LOGS/sharkninja_log_{current_time}.log'

logging.basicConfig(
    level=logging.INFO,  # Changed to INFO to capture more details
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=log_file,
    filemode='w'
)
logger = logging.getLogger(__name__)

# Custom CSS to enhance the app's appearance
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
st.set_page_config(layout="wide")
st.markdown("""
    <style>
    [data-testid="stSidebarNav"] {display: none;}
    </style>
    """, unsafe_allow_html=True)
st.markdown("""
<style>
    .stRadio > label {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
        margin-right: 10px;
    }
    .stRadio > label:hover {
        background-color: #e0e2e6;
    }
            
    .stDataFrame {
        border: 1px solid #e0e2e6;
        border-radius: 5px;
    }
    .main .block-container {
        padding-top: 2rem;
    }
    .stDownloadButton > button {
        background-color: #FFffff;
        color: black;
        width: 300px;
        position: relative;
        right:157px;
        border: 2px;
        border-radius: 5px;
        font-size: 17px;
        font-weight: 400;
        text-align: center;
        cursor: pointer;
        transition: all 0.3s ease;
            }
    .stButton > button:hover {
        background-color: #eeeeff;
        color: #000;
    }
    .stButton > button:active {
        background-color: #ffffff;
        color: white;
    }
    .stButton > button:focus {
        background-color: #eeeeff;
        color: white;
    }
</style>
""", unsafe_allow_html=True)
make_sidebar()

# Database connection parameters
DB_HOST = 'stockscraper-server.database.windows.net'
DB_NAME = 'stockscraper-database'
DB_USER = 'stockscraper-server-admin'
DB_PASSWORD = 'uc$DjSo7J6kqkoak'

# Function to get a database connection
def get_db_connection():
    return pymssql.connect(server=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)

# Function to create tables (if they don't exist)
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Create Countries table
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Countries' and xtype='U')
        CREATE TABLE Countries (
            CountryID INT IDENTITY(1,1) PRIMARY KEY,
            CountryCode NVARCHAR(2) UNIQUE
        )
        """)

        # Create Brands table
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Brands' and xtype='U')
        CREATE TABLE Brands (
            BrandID INT IDENTITY(1,1) PRIMARY KEY,
            BrandName NVARCHAR(50) UNIQUE
        )
        """)

        # Create Products table
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Products' and xtype='U')
        CREATE TABLE Products (
            ProductID INT IDENTITY(1,1) PRIMARY KEY,
            SKU NVARCHAR(50) UNIQUE,
            ProductName NVARCHAR(255)
        )
        """)

        # Create ProductStatus table
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ProductStatus' and xtype='U')
        CREATE TABLE ProductStatus (
            StatusID INT IDENTITY(1,1) PRIMARY KEY,
            ProductID INT,
            CountryID INT,
            BrandID INT,
            Date DATETIME,
            Status NVARCHAR(10),
            Type NVARCHAR(50),
            CurrentPrice DECIMAL(10, 2),
            FOREIGN KEY (ProductID) REFERENCES Products(ProductID),
            FOREIGN KEY (CountryID) REFERENCES Countries(CountryID),
            FOREIGN KEY (BrandID) REFERENCES Brands(BrandID)
        )
        """)

        conn.commit()
        logging.info("Tables created successfully")
    except Exception as e:
        logging.error(f"Error creating tables: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# Function to save data to the database
def save_to_db(df):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for _, row in df.iterrows():
            # Insert or get CountryID
            cursor.execute("SELECT CountryID FROM Countries WHERE CountryCode = %s", (row['Country'],))
            result = cursor.fetchone()
            if result:
                country_id = result[0]
            else:
                cursor.execute("INSERT INTO Countries (CountryCode) VALUES (%s)", (row['Country'],))
                country_id = cursor.lastrowid

            # Insert or get BrandID
            cursor.execute("SELECT BrandID FROM Brands WHERE BrandName = %s", (row['Brand'],))
            result = cursor.fetchone()
            if result:
                brand_id = result[0]
            else:
                cursor.execute("INSERT INTO Brands (BrandName) VALUES (%s)", (row['Brand'],))
                brand_id = cursor.lastrowid

            # Insert or get ProductID
            cursor.execute("SELECT ProductID FROM Products WHERE SKU = %s", (row['SKU'],))
            result = cursor.fetchone()
            if result:
                product_id = result[0]
            else:
                cursor.execute("INSERT INTO Products (SKU, ProductName) VALUES (%s, %s)", (row['SKU'], row['Product Name']))
                product_id = cursor.lastrowid

            # Insert into ProductStatus
            cursor.execute("""
            INSERT INTO ProductStatus (ProductID, CountryID, BrandID, Date, Status, Type, CurrentPrice)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (product_id, country_id, brand_id, row['Date'], row['Status'], row['Type'], row['Current Price']))

        conn.commit()
        logging.info(f"Successfully saved {len(df)} records to database")
    except Exception as e:
        logging.error(f"Error saving data to database: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Function to read data from the database
def read_from_db(country, brand):
    conn = get_db_connection()
    query = """
    SELECT p.SKU, p.ProductName, ps.Date, ps.Status, ps.Type, ps.CurrentPrice, c.CountryCode, b.BrandName
    FROM ProductStatus ps
    JOIN Products p ON ps.ProductID = p.ProductID
    JOIN Countries c ON ps.CountryID = c.CountryID
    JOIN Brands b ON ps.BrandID = b.BrandID
    WHERE c.CountryCode = %s AND b.BrandName = %s
    """
    df = pd.read_sql(query, conn, params=(country, brand))
    conn.close()
    return df

def extract_id_from_url(url):
    """Extracts the part of the URL that comes after 'zid'."""
    with st.spinner("bezig met extract"):
        try:
            start_index = url.index("zid") + 3
            zid_part = url[start_index:]
            return zid_part
        except ValueError:
            return None




def get_dataframe_init(country, brand):
    conn = get_db_connection()
    query = """
    SELECT p.SKU, ps.Date as LatestDate, ps.Status
    FROM Products p
    JOIN ProductStatus ps ON p.ProductID = ps.ProductID
    JOIN Countries c ON ps.CountryID = c.CountryID
    JOIN Brands b ON ps.BrandID = b.BrandID
    JOIN (
        SELECT ps.ProductID, MAX(ps.Date) as LatestDate
        FROM ProductStatus ps
        JOIN Countries c ON ps.CountryID = c.CountryID
        JOIN Brands b ON ps.BrandID = b.BrandID
        WHERE c.CountryCode = %s AND b.BrandName = %s AND ps.Status IN ('IN', 'OUT')
        GROUP BY ps.ProductID
    ) latest ON ps.ProductID = latest.ProductID AND ps.Date = latest.LatestDate
    WHERE c.CountryCode = %s AND b.BrandName = %s
    ORDER BY ps.Date DESC
    """
    
    df = pd.read_sql(query, conn, params=(country, brand, country, brand))
    conn.close()
    return df



def categorize_url(url):
    if "ninjakitchen.fr" in url:
        return "FR", "Ninja"
    elif "sharkclean.fr" in url:
        return "FR", "Shark"
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

def export_to_excel(out_of_stock_df, in_stock_df, skipped_df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        out_of_stock_df.to_excel(writer, sheet_name="Out of Stock", index=False)
        in_stock_df.to_excel(writer, sheet_name="In Stock", index=False)
        try:
            skipped_df.to_excel(writer, sheet_name="Skipped URLS", index=False)
        except:
            print("nice")
    return output.getvalue()


def get_current_out_of_stock(country, brand):
    conn = get_db_connection()
    query = """
    WITH ranked_status AS (
        SELECT 
            p.SKU, 
            ps.Date, 
            ps.Status,
            ROW_NUMBER() OVER (PARTITION BY p.SKU ORDER BY ps.Date DESC) as rn,
            SUM(CASE WHEN ps.Status = 'IN' THEN 1 ELSE 0 END) OVER (PARTITION BY p.SKU ORDER BY ps.Date DESC) as in_stock_count
        FROM Products p
        JOIN ProductStatus ps ON p.ProductID = ps.ProductID
        JOIN Countries c ON ps.CountryID = c.CountryID
        JOIN Brands b ON ps.BrandID = b.BrandID
        WHERE c.CountryCode = %s AND b.BrandName = %s
    ),
    current_status AS (
        SELECT SKU, Date, Status
        FROM ranked_status
        WHERE rn = 1
    ),
    earliest_out_date AS (
        SELECT 
            SKU, 
            MIN(Date) as EarliestOutDate
        FROM ranked_status
        WHERE Status = 'OUT' AND in_stock_count = 0
        GROUP BY SKU
    )
    SELECT 
        cs.SKU, 
        eod.EarliestOutDate as LastOutOfStockDate,
        DATEDIFF(day, eod.EarliestOutDate, GETDATE()) as DaysOutOfStock
    FROM current_status cs
    JOIN earliest_out_date eod ON cs.SKU = eod.SKU
    WHERE cs.Status = 'OUT'
    ORDER BY DaysOutOfStock DESC
    """
    
    df = pd.read_sql(query, conn, params=(country, brand))
    conn.close()
    
    df['LastOutOfStockDate'] = pd.to_datetime(df['LastOutOfStockDate'])
    return df

def check_availability(urls):
    out_of_stock_products = []
    in_stock_products = []
    skipped_urls = []
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_urls = len(urls)
    timeout_seconds = 5

    with requests.Session() as session:
        progress_placeholder = st.empty()

        for index, url in enumerate(urls, start=1):
            try:
                start_time = time.time()
                response = session.get(url, headers=headers, timeout=timeout_seconds)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")

                out_of_stock_button = soup.find("button", class_="js-btn_out-of-stock", title="Niet op voorraad")
                out_of_stock_button_fr = soup.find("button", class_="js-btn_out-of-stock", title="Stock épuisé")

                add_to_cart_button = soup.find("button", title="Ajouter au panier")
                add_to_cart_button_nl = soup.find("button", title="Toevoegen aan winkelmandje")

                product_name_tag = soup.find("h1", class_="js-product-title js-make-bold")
                
                price_tag = soup.find("div", attrs={"data-testing-id": "current-price"})
                current_price = price_tag.text.strip() if price_tag else "N/A"

                if product_name_tag:
                    product_name = product_name_tag.get_text(strip=True)
                    product_type = "Ninja" if "ninja" in product_name.lower() else "Shark"
                    zid_part = extract_id_from_url(url)

                    if out_of_stock_button or out_of_stock_button_fr:
                        out_of_stock_products.append(
                            (zid_part, product_name, current_date, url, "OUT", product_type, current_price)
                        )
                    elif add_to_cart_button or add_to_cart_button_nl:
                        in_stock_products.append(
                            (zid_part, product_name, current_date, url, "IN", product_type, current_price)
                        )
                    else:
                        in_stock_products.append(
                            (zid_part, product_name, current_date, url, "IN", product_type, current_price)
                        )

            except Timeout:
                skipped_urls.append(url)
            except requests.RequestException as e:
                st.error(f"Error fetching {url}: {e}")

            if time.time() - start_time > timeout_seconds:
                skipped_urls.append(url)

        progress_placeholder.empty()

    return out_of_stock_products, in_stock_products, skipped_urls

def process_urls(urls, existing_products=None):
    if existing_products is None:
        existing_products = set()

    out_of_stock_products = []
    in_stock_products = []
    skipped_urls = []
    total_urls = len(urls)

    progress_bar = st.progress(0)
    status_text = st.empty()

    for index, url in enumerate(urls, start=1):
        progress_bar.progress(index / total_urls)
        status_text.text(f"Processing {index} out of {total_urls}, Skipped: {len(skipped_urls)}")

        result = check_availability([url])
        if result[0]:
            product = result[0][0]
            if product[0] not in existing_products:
                out_of_stock_products.append(product)
                existing_products.add(product[0])
        elif result[1]:
            product = result[1][0]
            if product[0] not in existing_products:
                in_stock_products.append(product)
                existing_products.add(product[0])
        else:
            skipped_urls.append(url)

    progress_bar.empty()
    status_text.empty()

    return out_of_stock_products, in_stock_products, skipped_urls, existing_products




def get_out_of_stock_history(country, brand):
    conn = get_db_connection()
    query = """
    WITH status_changes AS (
        SELECT 
            p.SKU, 
            ps.Date AS DateTime,
            ps.Status,
            LAG(ps.Status) OVER (PARTITION BY p.SKU ORDER BY ps.Date) AS prev_status
        FROM Products p
        JOIN ProductStatus ps ON p.ProductID = ps.ProductID
        JOIN Countries c ON ps.CountryID = c.CountryID
        JOIN Brands b ON ps.BrandID = b.BrandID
        WHERE c.CountryCode = %s AND b.BrandName = %s
    ),
    out_of_stock_periods AS (
        SELECT 
            a.SKU, 
            a.DateTime AS OutOfStockDate,
            MIN(b.DateTime) AS BackInStockDate
        FROM status_changes a
        LEFT JOIN status_changes b ON a.SKU = b.SKU 
            AND b.DateTime > a.DateTime 
            AND b.Status = 'IN'
        WHERE a.Status = 'OUT' AND (a.prev_status IS NULL OR a.prev_status = 'IN')
        GROUP BY a.SKU, a.DateTime
    )
    SELECT 
        SKU, 
        OutOfStockDate,
        BackInStockDate,
        CASE 
            WHEN BackInStockDate IS NOT NULL THEN 
                DATEDIFF(day, OutOfStockDate, BackInStockDate)
            ELSE 
                DATEDIFF(day, OutOfStockDate, GETDATE())
        END AS DaysOutOfStock
    FROM out_of_stock_periods
    ORDER BY SKU, OutOfStockDate DESC
    """
    
    df = pd.read_sql(query, conn, params=(country, brand))
    conn.close()
    
    df['OutOfStockDate'] = pd.to_datetime(df['OutOfStockDate'])
    df['BackInStockDate'] = pd.to_datetime(df['BackInStockDate'])
    
    df['Status'] = df['BackInStockDate'].apply(lambda x: 'Historical' if pd.notnull(x) else 'Currently out of stock')
    
    return df

def add_logo():
        st.markdown(
            """
        <style>
            [data-testid="stSidebarContent"] {
                background-image: url(https://lever-client-logos.s3.amazonaws.com/5d04777b-cdde-4bc0-9cee-a61a406921c7-1528214915992.png);
                background-repeat: no-repeat;
                background-size: 80%;
                background-position: 20px 80px;
                padding-top: 100px;
            }
        </style>
        """,
            unsafe_allow_html=True,
        )
# add_logo()
st.title("Product Status Dashboard")
# Consolidated radio buttons
#st.markdown(f"### Actions for {brand} ({country})")

col1, col2, col3 = st.columns(3)
with col1:
    country = st.radio("Select Country", ["NL", "BE", "FR"])
    country_code = country.split()[0][:2].upper()
    language = country_code
with col2:
    brand = st.radio("Select Brand", ["Shark", "Ninja"])
    brand_name = brand.split()[0]
# col1, col2 = st.columns(2)
with col3:
    # st.radio("Select Brands", [""])
    st.markdown( f"""
        <div style="
            background-color: #f0f2f6;
            padding: 10px;
            border-radius: 5px;
            margin-right: 10px;
        ">
            Actions for {country}{brand}
        </div>
        """, unsafe_allow_html=True)
    
# Render the HTML in Streamlit
    
    
with col3:
    if st.button("Export to Excel", key="export_excel"):
        try:
            out_of_stock_df = read_from_db(country_code, brand_name)
            in_stock_df = read_from_db(country_code, brand_name)
            try:
                skipped_df = read_from_db(f"skipped_urls_{country_code}_{brand_name}")
            except Exception as e:
                skipped_df = pd.DataFrame()

            excel_data = export_to_excel(out_of_stock_df, in_stock_df, skipped_df)

            st.download_button(
                label="Download Excel file",
                data=excel_data,
                file_name=f"products_availability{language}{brand}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as e:
            st.error(f"An error occurred while exporting to Excel: {e}")



# Stock checking logic

col1, col2 = st.columns(2)

with col1:
    country = language #st.radio("Select Country", ["NL", "BE", "FR"])
    country_code = country.split()[0][:2].upper()

with col2:
    brand = brand #st.radio("Select Brand", ["Shark", "Ninja"])
    brand_name = brand.split()[0]

st.markdown("---")

# Create tabs
tab1, tab2, tab3 = st.tabs(["Current Status","Currently Out of Stock", "Out of Stock History"])

with tab1:
        df_outstock = get_dataframe_init(country_code, brand_name)
        df_instock = get_dataframe_init(country_code, brand_name)

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Out of Stock")
            filtered_df_out = df_outstock[df_outstock['Status']== 'OUT']
            st.dataframe(filtered_df_out, width=2000)
           

        with col2:
            st.subheader("In Stock")
            filtered_df_in = df_outstock[df_outstock['Status']== 'IN']
            st.dataframe(filtered_df_in, width=2000)
   

        st.markdown("---")

        # Display summary statistics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total SKUs", len(filtered_df_out) + len(filtered_df_in))

        with col2:
            st.metric("Out of Stock", len(filtered_df_out))

        with col3:
            st.metric("In Stock", len(filtered_df_in))

with tab2:
    # Fetch and display current out of stock dataframe
    df_current_out_of_stock = get_current_out_of_stock(country_code, brand_name)

    st.subheader("📅 Currently Out of Stock")
    edited_df_current_out_of_stock = st.data_editor(
        df_current_out_of_stock,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "LastOutOfStockDate": st.column_config.DatetimeColumn(
                "Last Out of Stock Date",
                format="DD-MM-YYYY",
                step=60,
            ),
        },
    )

    st.markdown("---")

    # Display summary statistics for current out of stock
    col1, col2 = st.columns(2)

    with col1:
        st.metric("Total Currently Out of Stock SKUs", len(df_current_out_of_stock))

    with col2:
        if not df_current_out_of_stock.empty:
            latest_out_of_stock = df_current_out_of_stock['LastOutOfStockDate'].max().strftime('%d-%m-%Y')
            st.metric("Latest Out of Stock Date", latest_out_of_stock)
        else:
            st.metric("Latest Out of Stock Date", "N/A")

with tab3:
    st.subheader("Out of Stock History Analysis")

    df_history = get_out_of_stock_history(country_code, brand_name)

    # 1. Summary metrics


    # 2. Time series of out-of-stock incidents
    
    
    #st.subheader("Detailed Out of Stock History")
    st.dataframe(df_history.sort_values('OutOfStockDate', ascending=False), use_container_width=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        total_incidents = len(df_history)
        st.metric("Total Out of Stock Incidents", total_incidents)
    with col2:
        current_out_of_stock = df_history['Status'].value_counts().get('Currently out of stock', 0)
        st.metric("Currently Out of Stock", current_out_of_stock)
    with col3:
        avg_duration = df_history['DaysOutOfStock'].mean()
        st.metric("Average Duration (Days)", f"{avg_duration:.1f}")
    with col4:
        max_duration = df_history['DaysOutOfStock'].max()
        st.metric("Max Duration (Days)", max_duration)


# Add a footer
st.markdown("---")
