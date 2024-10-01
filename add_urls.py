import streamlit as st
import sqlite3
from navigation import make_sidebar
import pandas as pd
make_sidebar()

def setup_database(db_name="Sharkninja.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS urls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT UNIQUE
    )
    ''')
    conn.commit()
    conn.close()

def add_url_to_database(url, db_name="Sharkninja.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    try:
        cursor.execute("INSERT INTO urls (url) VALUES (?)", (url,))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # URL already exists
        return False
    finally:
        conn.close()

def get_all_urls_from_database(db_name="Sharkninja.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    cursor.execute("SELECT url FROM urls")
    urls = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return urls

def search_urls(search_term, db_name="Sharkninja.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    cursor.execute("SELECT url FROM urls WHERE url LIKE ?", (f'%{search_term}%',))
    urls = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return urls

def remove_url_from_database(url, db_name="Sharkninja.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM urls WHERE url = ?", (url,))
    conn.commit()
    
    removed = cursor.rowcount > 0
    conn.close()
    return removed

def main():
    st.title("URL Database Manager")

    # Ensure the database and table are set up
    setup_database()

    tab1, tab2 = st.tabs(["Add URLs", "Search and Remove URLs"])

    with tab1:
        st.subheader("Add Multiple URLs")
        urls_input = st.text_area("Enter URLs (one per line):", height=200)
        
        if st.button("Add URLs"):
            if urls_input:
                urls = urls_input.split('\n')
                success_count = 0
                already_exist_count = 0
                
                for url in urls:
                    url = url.strip()
                    if url:  # Skip empty lines
                        if add_url_to_database(url):
                            success_count += 1
                        else:
                            already_exist_count += 1
                
                st.success(f"Added {success_count} new URL(s) successfully!")
                if already_exist_count > 0:
                    st.warning(f"{already_exist_count} URL(s) already existed in the database.")
            else:
                st.warning("Please enter at least one URL.")

        st.subheader("Current URLs in Database")
        urls = get_all_urls_from_database()
        url_list = []
        if urls:
            for url in urls:
                url_list.append(url)
               #st.text(url)
        else:
            st.info("No URLs in the database yet.")
        st.dataframe(url_list, width=2000)

    with tab2:
        st.subheader("Search and Remove URLs")
        search_term = st.text_input("Enter search term:")
        
        if search_term:
            results = search_urls(search_term)
            if results:
                st.success(f"Found {len(results)} matching URL(s):")
                for url in results:
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.text(url)
                    with col2:
                        if st.button("Remove", key=url):
                            if remove_url_from_database(url):
                                st.success(f"Removed URL: {url}")
                                st.rerun()
                            else:
                                st.error(f"Failed to remove URL: {url}")
            else:
                st.info("No matching URLs found.")

if __name__ == "__main__":
    main()
