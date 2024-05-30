import os
import sqlite3
import logging
from sqlite3 import Error
from logging.handlers import RotatingFileHandler
import subprocess
import toml
import tqdm

#TODO: Make asynchronous

# Configure logging
LOG_FILENAME = 'audible_downloader.log'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_LEVEL = logging.DEBUG  # Set to DEBUG to capture detailed logs

# Set up a specific logger with our desired output level
logger = logging.getLogger('AudibleDownloaderLogger')
logger.setLevel(LOG_LEVEL)

# Add the log message handler to the logger with utf-8 encoding
handler = RotatingFileHandler(LOG_FILENAME, maxBytes=1024*1024*5, backupCount=5, encoding='utf-8')
formatter = logging.Formatter(LOG_FORMAT)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Define database and table name
DATABASE = 'audible_library.db'
TABLE_NAME = 'books'
TEST_MODE = True  # Set to True to activate test mode

def log_and_print(message, level=logging.INFO, always_print=False):
    if TEST_MODE or always_print:
        print(message)
    if level == logging.DEBUG:
        logger.debug(message)
    elif level == logging.INFO:
        logger.info(message)
    elif level == logging.WARNING:
        logger.warning(message)
    elif level == logging.ERROR:
        logger.error(message)
    elif level == logging.CRITICAL:
        logger.critical(message)

def create_connection(db_file):
    """ Create a database connection to a SQLite database """
    conn = None
    try:
        if not os.path.exists(db_file):
            log_and_print(f"Database file '{db_file}' does not exist. Creating a new database file.", logging.INFO, always_print=True)
        conn = sqlite3.connect(db_file)
        log_and_print("Connected to SQLite database", always_print=True)
    except Error as e:
        log_and_print(f"Error connecting to database: {e}", logging.ERROR, always_print=True)
    return conn

def get_books_to_download(conn):
    """ Get all books that are in the library, not downloaded, and not finished """
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT ASIN, Title FROM {TABLE_NAME} WHERE Downloaded = 0 AND Finished = 0 AND Status = 'Library'")
        books = cur.fetchall()
        return books
    except Error as e:
        log_and_print(f"Error querying database: {e}", logging.ERROR, always_print=True)
        return []

def update_book_downloaded_status(conn, asin):
    """ Update the downloaded status of a book """
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE {TABLE_NAME} SET Downloaded = 1 WHERE ASIN = ?", (asin,))
        conn.commit()
        log_and_print(f"Updated downloaded status for book with ASIN '{asin}'", logging.INFO)
    except Error as e:
        log_and_print(f"Error updating downloaded status for book with ASIN '{asin}': {e}", logging.ERROR, always_print=True)

def download_book(asin, title):
    """ Download a book using audible-cli """
    try:
        log_and_print(f"Downloading book '{title}' with ASIN '{asin}'", logging.INFO, always_print=True)
        result = subprocess.run(['audible', 'download', '--asin', asin, '--aax', '--output-dir', 'Library'], check=True, capture_output=True, text=True)
        log_and_print(f"Downloaded book '{title}' with ASIN '{asin}'", logging.INFO, always_print=True)
        log_and_print(result.stdout, logging.DEBUG)
        return True
    except subprocess.CalledProcessError as e:
        log_and_print(f"Error downloading book '{title}' with ASIN '{asin}': {e}", logging.ERROR, always_print=True)
        log_and_print(e.stdout, logging.ERROR)
        log_and_print(e.stderr, logging.ERROR)
        return False

def main():
    log_and_print("Starting Audible Downloader", logging.INFO, always_print=True)
    
    try:
        config_dir = os.environ.get('AUDIBLE_CONFIG_DIR', os.path.expanduser('~/.audible'))
        config_path = os.path.join(config_dir, 'config.toml')

        if not os.path.exists(config_path):
            raise FileNotFoundError("Audible config file not found.")

        config = toml.load(config_path)
        profile_name = config['APP']['primary_profile']
        profile = config['profile'][profile_name]
        auth_file_path = os.path.join(config_dir, profile['auth_file'])

        if not os.path.exists(auth_file_path):
            raise FileNotFoundError("Audible auth file not found.")

        # Set the AUDIBLE_CONFIG_DIR environment variable
        os.environ['AUDIBLE_CONFIG_DIR'] = config_dir

        conn = create_connection(DATABASE)
        if conn is not None:
            try:
                books = get_books_to_download(conn)
                if not books:
                    log_and_print("No books to download", logging.INFO, always_print=True)
                    return

                if TEST_MODE:
                    books = books[:1]  # Limit to one book in test mode

                # Create the Library folder if it doesn't exist
                os.makedirs('Library', exist_ok=True)

                for asin, title in tqdm.tqdm(books, desc="Downloading books", unit="book"):
                    if download_book(asin, title):
                        if not TEST_MODE:
                            update_book_downloaded_status(conn, asin)

            except Error as e:
                log_and_print(f"Error during processing: {e}", logging.ERROR, always_print=True)
            finally:
                conn.close()
                log_and_print("Database connection closed", logging.INFO, always_print=True)
        else:
            log_and_print("Error! Cannot create the database connection.", logging.ERROR, always_print=True)

    except Exception as e:
        log_and_print(f"Configuration or authentication error: {e}", logging.ERROR, always_print=True)

if __name__ == '__main__':
    main()
