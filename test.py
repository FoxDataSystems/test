import pymssql

# Database connection parameters
server = "stockscraper-server.database.windows.net"
database = "stockscraper-database"
username = "stockscraper-server-admin"
password = "uc$DjSo7J6kqkoak"

try:
    # Establish a connection
    conn = pymssql.connect(server=server, user=username, password=password, database=database)
    
    # Create a cursor
    cursor = conn.cursor()
    
    # Execute a simple query
    cursor.execute("SELECT @@VERSION")
    
    # Fetch the result
    row = cursor.fetchone()
    
    # Print the result
    print("Connection successful!")
    print("SQL Server version:", row[0])

    # Close the cursor and connection
    cursor.close()
    conn.close()

except pymssql.Error as e:
    print("Error connecting to the database:")
    print(str(e))
