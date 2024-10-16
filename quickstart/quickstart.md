import jaydebeapi
import os

# Set the path to your Informix JDBC driver (.jar file)
jdbc_driver_path = '/opt/IBM/Informix_JDBC_Driver/lib/ifxjdbc.jar'

# Database connection details
database = 'nolbooco'  # Your Informix database
hostname = '175.196.7.17'  # Host of your Informix server
port = '1526'  # Port where Informix is running
username = 'informix'  # Informix username
password = 'eusr2206'  # Informix password
server = 'nbmain'  # Server name in Informix

# JDBC connection URL for Informix
jdbc_url = f"jdbc:informix-sqli://{hostname}:{port}/{database}:INFORMIXSERVER={server};"

# JDBC class name (Informix JDBC driver class)
jdbc_driver_class = 'com.informix.jdbc.IfxDriver'

# Set locale (optional, if needed by your database)
os.environ['DB_LOCALE'] = 'en_US.819'
os.environ['CLIENT_LOCALE'] = 'en_us.utf8'

# Try to connect using the JDBC driver via JayDeBeApi
try:
    # Establish connection using the JDBC driver
    conn = jaydebeapi.connect(
        jdbc_driver_class,  # JDBC driver class
        jdbc_url,  # JDBC URL
        [username, password],  # Credentials
        jdbc_driver_path  # Path to the JDBC driver .jar file
    )
    
    # Create a cursor object using the connection
    cursor = conn.cursor()
    
    # Sample query (modify based on your requirements)
    cursor.execute("SELECT * FROM cm_item_master")
    
    # Fetch and print the first result
    result = cursor.fetchall()
    print("Query Result:")
    for row in result:
        print(row)
    
    # Close the connection
    cursor.close()
    conn.close()

    print("Connection closed successfully")

except Exception as e:
    print(f"Failed to connect to Informix: {e}")
