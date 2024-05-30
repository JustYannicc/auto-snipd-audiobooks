import os
import sqlite3
import logging
from sqlite3 import Error
from logging.handlers import RotatingFileHandler
import audible
import toml
import asyncio
import re
import json

#FIXME: dont itterate the raw output file names. just name them wishlist_raw_response.json and library_raw_response.json and overwrite them if there is new data. 
#FIXME: The finished tag is not being set correctly. It is always false.
#FIXME: the wishlist is not being fetched correctly. The wishlist is not being fetched at all.

# Configure logging
LOG_FILENAME = 'audible_checker.log'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_LEVEL = logging.DEBUG  # Set to DEBUG to capture detailed logs

# Set up a specific logger with our desired output level
logger = logging.getLogger('AudibleCheckerLogger')
logger.setLevel(LOG_LEVEL)

# Add the log message handler to the logger with utf-8 encoding
handler = RotatingFileHandler(LOG_FILENAME, maxBytes=1024*1024*5, backupCount=5, encoding='utf-8')
formatter = logging.Formatter(LOG_FORMAT)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Define database and table name
DATABASE = 'audible_library.db'
TABLE_NAME = 'books'
TEST_MODE = False  # Change to True for more verbosity

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

def create_table(conn):
    """ Create a table if it doesn't exist """
    try:
        cur = conn.cursor()
        cur.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            ASIN TEXT PRIMARY KEY,
            Author TEXT,
            Title TEXT,
            Description TEXT,
            Length TEXT,
            EPUB_Column TEXT,
            Downloaded BOOLEAN DEFAULT 0,
            Cover_URL TEXT,
            Finished BOOLEAN DEFAULT 0,
            Status TEXT
        )
        ''')
        conn.commit()
        log_and_print("Table created or already exists", always_print=True)
    except Error as e:
        log_and_print(f"Error creating table: {e}", logging.ERROR, always_print=True)

def insert_or_update_book(conn, book):
    """ Insert or update a book in the table """
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT Author, Description, Length, Cover_URL, Title, Status FROM {TABLE_NAME} WHERE ASIN = ?", (book["ASIN"],))
        result = cur.fetchone()
        if result:
            current_author, current_description, current_length, current_cover_url, title, status = result
            # Determine if an update is needed
            needs_update = False

            if status == "Wishlist" and book["Status"] == "Library":
                needs_update = True
                log_and_print(f"Status update needed for book '{book['Title']}' from 'Wishlist' to 'Library'", logging.DEBUG)
            if current_author == "Unknown" and book["Author"] != "Unknown":
                needs_update = True
                log_and_print(f"Author update needed for book '{book['Title']}' from 'Unknown' to '{book['Author']}'", logging.DEBUG)
            if current_description in ["No description available", "", None] and book["Description"] not in ["No description available", "", None]:
                needs_update = True
                log_and_print(f"Description update needed for book '{book['Title']}' from '{current_description}' to '{book['Description']}'", logging.DEBUG)
            if current_length in ["Unknown", "", None] and book["Length"] not in ["Unknown", "", None]:
                needs_update = True
                log_and_print(f"Length update needed for book '{book['Title']}' from '{current_length}' to '{book['Length']}'", logging.DEBUG)
            if not current_cover_url and book["Cover_URL"]:
                needs_update = True
                log_and_print(f"Cover URL update needed for book '{book['Title']}' from '' to '{book['Cover_URL']}'", logging.DEBUG)

            if needs_update:
                cur.execute(f'''
                UPDATE {TABLE_NAME}
                SET Author = ?, Description = ?, Length = ?, EPUB_Column = ?, Downloaded = ?, Cover_URL = ?, Finished = ?, Status = ?
                WHERE ASIN = ?
                ''', (book["Author"], 
                      book["Description"],
                      book["Length"], 
                      book["EPUB_Column"], 
                      False,  # Set Downloaded to False always
                      book["Cover_URL"], 
                      book["Finished"], 
                      book["Status"], 
                      book["ASIN"]))
                conn.commit()
                log_and_print(f"Book '{book['Title']}' updated in database", logging.INFO)
                return False  # Update occurred
            else:
                return None  # No update needed
        else:
            cur.execute(f'''
            INSERT INTO {TABLE_NAME} (ASIN, Author, Title, Description, Length, EPUB_Column, Downloaded, Cover_URL, Finished, Status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (book["ASIN"],
                  book["Author"], 
                  book["Title"], 
                  book["Description"],
                  book["Length"], 
                  book["EPUB_Column"], 
                  False,  # Set Downloaded to False always
                  book["Cover_URL"], 
                  book["Finished"], 
                  book["Status"]))
            conn.commit()
            log_and_print(f"Book '{book['Title']}' added to database", logging.INFO)
            return True  # Insert occurred
    except Error as e:
        log_and_print(f"Error inserting/updating book '{book['Title']}']: {e}", logging.ERROR, always_print=True)
        return False

def strip_markdown(text):
    """ Strip markdown tags from a text """
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

async def fetch_all_items(client, path, response_groups):
    items = []
    page = 1  # Start pagination from page 1
    while True:
        try:
            params = {
                "response_groups": response_groups,
                "num_results": 50,
                "page": page
            }
            response = await client.get(
                path=path,
                params=params
            )
            if TEST_MODE:
                filename = f"{path}_raw_response.json"
                with open(filename, "w", encoding='utf-8') as f:
                    json.dump(response, f, ensure_ascii=False, indent=4)

            if not response or 'items' not in response or not response['items']:
                break

            items.extend(response['items'])
            if len(response['items']) < 50:
                break
            page += 1
        except Exception as e:
            log_and_print(f"Error fetching page {page} from {path}: {e}", logging.ERROR, always_print=True)
            log_and_print(f"Request params: {params}", logging.ERROR, always_print=True)
            if 'response' in locals():
                log_and_print(f"Response content: {response}", logging.ERROR, always_print=True)
                if TEST_MODE:
                    filename = f"{path}_error_response.json"
                    with open(filename, "w", encoding='utf-8') as f:
                        json.dump(response, f, ensure_ascii=False, indent=4)
            break
    return items

async def fetch_audible_details(client):
    """ Fetch audiobook details asynchronously using the Audible API """
    try:
        library_items = await fetch_all_items(
            client, "library",
            "contributors, media, price, reviews, product_attrs, "
            "product_extended_attrs, product_desc, product_plan_details, "
            "product_plans, rating, sample, sku, series, ws4v, origin, "
            "relationships, review_attrs, categories, badge_types, "
            "category_ladders, claim_code_url, is_downloaded, pdf_url, "
            "is_returnable, origin_asin, percent_complete, provided_review"
        )

        wishlist_items = await fetch_all_items(
            client, "wishlist",
            "contributors, media, product_attrs, product_desc"
        )

        return library_items + wishlist_items
    except Exception as e:
        log_and_print(f"Error fetching details: {e}", logging.ERROR, always_print=True)
        return None
    
async def main_async(auth):
    async with audible.AsyncClient(auth=auth) as client:
        conn = create_connection(DATABASE)
        if conn is not None:
            try:
                create_table(conn)
                
                # Fetch all details in a single request
                all_details = await fetch_audible_details(client)
                if not all_details:
                    log_and_print("Failed to fetch details from Audible", logging.ERROR, always_print=True)
                    return
                
                for book_details in all_details:
                    asin = book_details.get('asin', 'Unknown ASIN')
                    author = ', '.join([author['name'] for author in book_details.get('authors', [])]) if book_details.get('authors') else 'Unknown'
                    title = book_details.get('title', 'Unknown Title')
                    description = strip_markdown(book_details.get('merchandising_summary', 'No description available'))
                    length = str(book_details.get('runtime_length_min', 'Unknown'))
                    cover_url = book_details.get('images', {}).get('cover', {}).get('sizes', {}).get('600', '') if book_details.get('images') else ''
                    finished = book_details.get('listening_status') == 'Completed'
                    status = 'Library' if book_details.get('is_downloaded') else 'Wishlist'
                    
                    book = {
                        "ASIN": asin,
                        "Author": author,
                        "Title": title,
                        "Description": description,
                        "Length": length,
                        "EPUB_Column": "",
                        "Downloaded": False,
                        "Cover_URL": cover_url,
                        "Finished": finished,
                        "Status": status
                    }
                    insert_or_update_book(conn, book)
                
                # Update books with missing authors
                cur = conn.cursor()
                cur.execute(f"SELECT ASIN, Author FROM {TABLE_NAME} WHERE Author = 'Unknown'")
                books_missing_authors = cur.fetchall()
                
                for asin, _ in books_missing_authors:
                    book_details = await fetch_audible_details(client, asin)
                    if book_details:
                        author = ', '.join([author['name'] for author in book_details.get('authors', [])]) if book_details.get('authors') else 'Unknown'
                        cur.execute(f"UPDATE {TABLE_NAME} SET Author = ? WHERE ASIN = ?", (author, asin))
                        conn.commit()

            except Error as e:
                log_and_print(f"Error querying database: {e}", logging.ERROR, always_print=True)
            finally:
                conn.close()
                log_and_print("Database connection closed", logging.INFO, always_print=True)
        else:
            log_and_print("Error! Cannot create the database connection.", logging.ERROR, always_print=True)

if __name__ == '__main__':
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

        auth = audible.Authenticator.from_file(auth_file_path)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(main_async(auth))

    except Exception as e:
        log_and_print(f"Authentication error: {e}", logging.ERROR, always_print=True)
