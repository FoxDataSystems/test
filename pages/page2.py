from navigation import make_sidebar
import streamlit as st
import pymssql
import pandas as pd
import plotly.express as px
from datetime import date, datetime
import io

st.set_page_config(layout="wide", page_title="SKU Price Manager")
st.markdown("""
    <style>
    [data-testid="stSidebarNav"] {display: none;}
    </style>
    """, unsafe_allow_html=True)
make_sidebar()

# Database connection parameters
DB_HOST = 'stockscraper-server.database.windows.net'
DB_NAME = 'stockscraper-database'
DB_USER = 'stockscraper-server-admin'
DB_PASSWORD = 'uc$DjSo7J6kqkoak'

class PriceManager:
    def __init__(self):
        self.conn = pymssql.connect(server=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        self.cursor = self.conn.cursor()

    def upsert_price(self, sku, price, entry_date, reason, country):
        # Get ProductID and CountryID
        self.cursor.execute("SELECT ProductID FROM Products WHERE SKU = %s", (sku,))
        product_id = self.cursor.fetchone()
        if not product_id:
            self.cursor.execute("INSERT INTO Products (SKU, ProductName) VALUES (%s, %s)", (sku, f"Product {sku}"))
            product_id = self.cursor.lastrowid
        else:
            product_id = product_id[0]

        self.cursor.execute("SELECT CountryID FROM Countries WHERE CountryCode = %s", (country,))
        country_id = self.cursor.fetchone()
        if not country_id:
            self.cursor.execute("INSERT INTO Countries (CountryCode) VALUES (%s)", (country,))
            country_id = self.cursor.lastrowid
        else:
            country_id = country_id[0]

        # Insert or update price
        self.cursor.execute('''
            MERGE INTO Prices AS target
            USING (VALUES (%s, %s, %s, %s, %s)) AS source (ProductID, CountryID, Price, EntryDate, Reason)
            ON target.ProductID = source.ProductID AND target.CountryID = source.CountryID AND target.EntryDate = source.EntryDate
            WHEN MATCHED THEN
                UPDATE SET Price = source.Price, Reason = source.Reason
            WHEN NOT MATCHED THEN
                INSERT (ProductID, CountryID, Price, EntryDate, Reason)
                VALUES (source.ProductID, source.CountryID, source.Price, source.EntryDate, source.Reason)
        ''', (product_id, country_id, price, entry_date, reason))
        self.conn.commit()

    def get_price_history(self, sku, country=None, days=None):
        query = '''
            SELECT p.EntryDate, p.Price, p.Reason, c.CountryCode as country 
            FROM Prices p
            JOIN Products pr ON p.ProductID = pr.ProductID
            JOIN Countries c ON p.CountryID = c.CountryID
            WHERE pr.SKU = %s
        '''
        params = [sku]
        if country:
            query += " AND c.CountryCode = %s"
            params.append(country)
        if days:
            query += f" AND p.EntryDate >= DATEADD(day, -{days}, GETDATE())"
        query += " ORDER BY p.EntryDate DESC"
        return pd.read_sql(query, self.conn, params=params)

    def delete_entry(self, sku, entry_date, country):
        self.cursor.execute('''
            DELETE FROM Prices 
            WHERE ProductID = (SELECT ProductID FROM Products WHERE SKU = %s)
            AND CountryID = (SELECT CountryID FROM Countries WHERE CountryCode = %s)
            AND EntryDate = %s
        ''', (sku, country, entry_date))
        self.conn.commit()
        return self.cursor.rowcount

    def search_skus(self, term):
        return pd.read_sql("SELECT DISTINCT SKU FROM Products WHERE SKU LIKE %s",
                           self.conn, params=(f'%{term}%',))['SKU'].tolist()

    def export_data(self):
        query = '''
            SELECT pr.SKU, p.Price, p.EntryDate, p.Reason, c.CountryCode as Country
            FROM Prices p
            JOIN Products pr ON p.ProductID = pr.ProductID
            JOIN Countries c ON p.CountryID = c.CountryID
        '''
        df = pd.read_sql(query, self.conn)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Prices')
        return output.getvalue()

    def get_price_changes_by_date(self, search_date, country):
        query = '''
            SELECT pr.SKU, p.Price, p.Reason
            FROM Prices p
            JOIN Products pr ON p.ProductID = pr.ProductID
            JOIN Countries c ON p.CountryID = c.CountryID
            WHERE p.EntryDate = %s AND c.CountryCode = %s
        '''
        return pd.read_sql(query, self.conn, params=(search_date, country))

    def __del__(self):
        self.conn.close()

def main():
    pm = PriceManager()

    st.title('SKU Price Manager')

    col1, col2 = st.columns([1, 3])

    with col1:
        country = st.radio("Select Country:", ["NL", "BE", "FR"])
        if st.button('Export to Excel'):
            st.download_button(
                label="Download Excel file",
                data=pm.export_data(),
                file_name="price_database.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    with col2:
        tab1, tab2, tab3, tab4 = st.tabs(
            ["Manage Prices", "Price History", "Search by Date", "Delete Entries"])

        with tab1:
            st.subheader("Manage Prices")
        
            # Initialize session state
            if 'adding_new_sku' not in st.session_state:
                st.session_state.adding_new_sku = False
        
            # Button to toggle between adding new SKU and selecting existing SKU
            if st.button('Add New SKU' if not st.session_state.adding_new_sku else 'Select Existing SKU'):
                st.session_state.adding_new_sku = not st.session_state.adding_new_sku
        
            # Display either selectbox or text input based on state
            if st.session_state.adding_new_sku:
                sku = st.text_input('Enter new SKU:', key='new_sku_input')
            else:
                sku = st.selectbox('Select SKU:', [''] + pm.search_skus(''), key='manage_sku')
        
            price = st.number_input('Price (€):', min_value=0.0, format='%.2f')
            reason = st.text_input('Reason for change:')
            entry_date = st.date_input("Date:", value=date.today())
        
            if st.button('Submit'):
                if sku and price and reason:
                    pm.upsert_price(sku, price, entry_date, reason, country)
                    st.success(f'Price updated for SKU {sku}: €{price:.2f} on {entry_date}')
                    # Reset to selectbox mode after successful submission
                    st.session_state.adding_new_sku = False
                    st.rerun()
                else:
                    st.error('Please fill all fields.')

        with tab2:
            st.subheader("Price History")

            col1, col2 = st.columns([1, 2])

            with col1:
                lookup_sku = st.selectbox('Select or Enter SKU:', [''] + pm.search_skus(''), key='history_sku')
                show_all = st.checkbox("Show all countries")

            if lookup_sku:
                df = pm.get_price_history(lookup_sku, None if show_all else country)
                df_30_days = pm.get_price_history(lookup_sku, None if show_all else country, days=30)

                if not df.empty:
                    with col2:
                        if not df_30_days.empty:
                            lowest_price_30_days = df_30_days['Price'].min()
                            st.metric("Lowest price (last 30 days)", f"€{lowest_price_30_days:.2f}")

                    st.plotly_chart(
                        px.line(df, x='EntryDate', y='Price', color='country' if show_all else None,
                                title=f'Price History for {lookup_sku}')
                        .update_layout(yaxis_title='Price (€)', xaxis_title='Date'),
                        use_container_width=True
                    )

                    st.dataframe(
                        df.style.format({'Price': '€{:.2f}'})
                           .set_properties(**{'text-align': 'left'}),
                        use_container_width=True
                    )
                else:
                    st.info(f"No price history found for {lookup_sku}")

        with tab3:
            st.subheader("Search by Date")
            search_date = st.date_input(
                "Select date to search for price changes:", key='search_date')

            if st.button('Search Price Changes'):
                changes_df = pm.get_price_changes_by_date(search_date, country)
                if not changes_df.empty:
                    st.write(f"Price changes on {search_date}:")
                    st.dataframe(changes_df.style.format({'Price': '€{:.2f}'}))
                else:
                    st.info(f"No price changes found on {search_date}")

        with tab4:
            st.subheader("Delete Entries")
            del_sku = st.selectbox('Select SKU:', [''] + pm.search_skus(''), key='delete_sku')
        
            if del_sku:
                df = pm.get_price_history(del_sku, country)
                if not df.empty:
                    st.write(f"Current entries for {del_sku} in {country}:")
        
                    # Add the 'delete' column
                    df['delete'] = False
        
                    # Use st.data_editor for inline editing and row selection
                    edited_df = st.data_editor(
                        df,
                        hide_index=True,
                        column_config={
                            "EntryDate": st.column_config.DateColumn("Date"),
                            "Price": st.column_config.NumberColumn("Price (€)", format="€%.2f"),
                            "Reason": "Reason",
                            "delete": st.column_config.CheckboxColumn("Delete?")
                        },
                        disabled=["EntryDate", "Price", "Reason"],
                        key="editor"
                    )
        
                    # Filter rows marked for deletion
                    rows_to_delete = edited_df[edited_df['delete'] == True]
        
                    if not rows_to_delete.empty:
                        if st.button("Delete Selected Entries"):
                            for _, row in rows_to_delete.iterrows():
                                pm.delete_entry(del_sku, row['EntryDate'], country)
                            st.success(f"Deleted {len(rows_to_delete)} entries for SKU {del_sku}")
                            st.rerun()
                    else:
                        st.info("Select entries to delete by checking the 'Delete?' column")
                else:
                    st.info(f"No entries found for SKU {del_sku} in {country}")

if __name__ == "__main__":
    main()
