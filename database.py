import logging
import mysql.connector
from mysql.connector import Error
from config import Config

logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Establishes a connection to the MySQL database using the configuration
    specified in the Config class.

    Returns:
        conn: A MySQL database connection object.

    Raises:
        mysql.connector.Error: If there is an error connecting to the database.
    """
    try:
        # Debug: Print DB_CONFIG values
        logger.debug("Connecting with the following DB_CONFIG:")
        for key, value in Config.DB_CONFIG.items():
            logger.debug(f"{key}: {value}")

        conn = mysql.connector.connect(**Config.DB_CONFIG)
        if conn.is_connected():
            logger.info("Successfully connected to the database")
        return conn
    except Error as e:
        logger.info(f"Error connecting to database: {e}")
        raise

def fetch_all_rows(query, args=None):
    """
    Executes a given SQL query and fetches all rows from the database.

    Args:
        query (str): The SQL query to execute.
        args (tuple): Optional arguments to pass with the query.

    Returns:
        list: A list of dictionaries containing the query results.

    Raises:
        mysql.connector.Error: If there is an error executing the query.
    """
    try:
        # Use a context manager to handle resource cleanup
        with get_db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(query, args or ())
                rows = cursor.fetchall()
                print("Query executed successfully")
                return rows
    except Error as e:
        print(f"Error executing query: {e}")
        raise

# Example usage
if __name__ == "__main__":
    try:
        query = "SELECT * FROM latest_sensor_meta_data"
        results = fetch_all_rows(query)
        for row in results:
            print(row)
    except Exception as e:
        print(f"An error occurred: {e}")
