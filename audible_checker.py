import audible
import sqlite3
import logging
import os
import pyotp
from sqlite3 import Error
from logging.handlers import RotatingFileHandler
import re
import json

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
AUTH_FILE = 'audible_auth.json'
TEST_MODE = False  # Change to True for more verbosity
DEBUG_RAW_RESPONSE = False  # Change to True to log raw API responses

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
            Author TEXT,
            Title TEXT PRIMARY KEY,
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
        cur.execute(f"SELECT Author, Description, Length, Cover_URL, Title, Status FROM {TABLE_NAME} WHERE Title = ?", (book["Title"],))
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
            if current_description == "No description available" and book["Description"] != "No description available":
                needs_update = True
                log_and_print(f"Description update needed for book '{book['Title']}' from 'No description available' to '{book['Description']}'", logging.DEBUG)
            if not current_length and book["Length"]:
                needs_update = True
                log_and_print(f"Length update needed for book '{book['Title']}' from '' to '{book['Length']}'", logging.DEBUG)
            if not current_cover_url and book["Cover_URL"]:
                needs_update = True
                log_and_print(f"Cover URL update needed for book '{book['Title']}' from '' to '{book['Cover_URL']}'", logging.DEBUG)

            if needs_update:
                cur.execute(f'''
                UPDATE {TABLE_NAME}
                SET Author = ?, Description = ?, Length = ?, EPUB_Column = ?, Downloaded = ?, Cover_URL = ?, Finished = ?, Status = ?
                WHERE Title = ?
                ''', (book["Author"], 
                      book["Description"],
                      book["Length"], 
                      book["EPUB_Column"], 
                      False,  # Set Downloaded to False always
                      book["Cover_URL"], 
                      book["Finished"], 
                      book["Status"], 
                      book["Title"]))
                conn.commit()
                log_and_print(f"Book '{book['Title']}' updated in database", logging.INFO)
                return False  # Update occurred
            else:
                return None  # No update needed
        else:
            cur.execute(f'''
            INSERT INTO {TABLE_NAME} (Author, Title, Description, Length, EPUB_Column, Downloaded, Cover_URL, Finished, Status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (book["Author"], 
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


def fetch_audible_data(auth):
    """ Fetch library and wishlist data from Audible """
    try:
        client = audible.Client(auth=auth)
        
        library = client.get(
            "1.0/library",
            num_results=1000,
            response_groups="product_desc, product_attrs, contributors, media"
        )
        if DEBUG_RAW_RESPONSE:
            with open('library_raw_response.json', 'w') as f:
                json.dump(library, f)
        log_and_print("Fetched library data", always_print=True)
        
        wishlist = client.get(
            "1.0/wishlist",
            num_results=50,
            response_groups="product_desc, product_attrs, contributors, media"
        )
        if DEBUG_RAW_RESPONSE:
            with open('wishlist_raw_response.json', 'w') as f:
                json.dump(wishlist, f)
        log_and_print("Fetched wishlist data", always_print=True)
        
        return library, wishlist
    except Exception as e:
        log_and_print(f"Error fetching data from Audible: {e}", logging.ERROR, always_print=True)
        return None, None


def parse_books(data, status, downloaded=False):
    """ Parse book data """
    books = []
    key = 'items' if 'items' in data else 'products' if 'products' in data else None
    if not key:
        log_and_print(f"Expected key 'items' or 'products' not found in data: {data}", logging.ERROR, always_print=True)
        return books

    for item in data[key]:
        # Ensure authors are correctly fetched from 'authors' key if 'contributors' is missing
        contributors = item.get("contributors", [])
        if not contributors:
            contributors = item.get("authors", [])
        
        author = ", ".join([contrib["name"] for contrib in contributors if contrib.get("role") == "Author" or "role" not in contrib])
        
        description = item.get("summary") or item.get("merchandising_summary", "No description available")
        description = re.sub('<[^<]+?>', '', description)  # Remove HTML tags
        
        listening_status = item.get("listening_status")
        finished = listening_status and listening_status.get("status", "") == "Finished"
        
        book = {
            "Author": author if author else "Unknown",
            "Title": item["title"],
            "Description": description,
            "Length": item.get("runtime_length_min", "Unknown"),
            "EPUB_Column": "",  # Placeholder for EPUB reference
            "Downloaded": False,  # Always set to False as per requirement
            "Cover_URL": item.get("product_images", {}).get("500", None),  # Cover URL
            "Finished": finished,
            "Status": status
        }
        books.append(book)
    return books


def custom_captcha_callback(captcha_url):
    print(f"Please solve the CAPTCHA here: {captcha_url}")
    solution = input("Enter CAPTCHA solution: ")
    log_and_print(f"CAPTCHA solved with: {solution}")
    return solution

def custom_otp_callback():
    otp_secret = os.getenv('OTP_SECRET')
    if not otp_secret:
        otp_code = input("Enter the OTP code sent to your device: ")
    else:
        otp = pyotp.TOTP(otp_secret)
        otp_code = otp.now()
    log_and_print(f"OTP code provided: {otp_code}")
    return otp_code

def custom_cvf_callback():
    cvf_code = input("Enter the CVF code sent to your email or phone: ")
    log_and_print(f"CVF code provided: {cvf_code}")
    return cvf_code

def custom_approval_callback():
    print("Approval alert detected! Amazon has sent you an email.")
    input("Please approve the email and press Enter to continue...")
    log_and_print("Approval alert handled.")

def authenticate(username, password, country_code):
    if os.path.exists(AUTH_FILE):
        # Load authentication data from file
        try:
            auth = audible.Authenticator.from_file(AUTH_FILE)
            log_and_print("Authenticated with Audible using saved credentials", always_print=True)
            return auth
        except Exception as e:
            log_and_print(f"Error loading authentication data: {e}", logging.ERROR, always_print=True)

    # If authentication data is not available, perform login
    try:
        auth = audible.Authenticator.from_login(
            username,
            password,
            locale=country_code,
            captcha_callback=custom_captcha_callback,
            otp_callback=custom_otp_callback,
            cvf_callback=custom_cvf_callback,
            approval_callback=custom_approval_callback
        )
        auth.to_file(AUTH_FILE)
        log_and_print("Authenticated with Audible and saved credentials", always_print=True)
        return auth
    except audible.exceptions.AuthFlowError as e:
        log_and_print(f"Authentication flow error: {e}", logging.ERROR, always_print=True)
    except audible.exceptions.Unauthorized as e:
        log_and_print(f"Unauthorized access: {e}", logging.ERROR, always_print=True)
    except audible.exceptions.NoRefreshToken as e:
        log_and_print(f"No refresh token available: {e}", logging.ERROR, always_print=True)
    except Exception as e:
        log_and_print(f"Unexpected error during authentication: {e}", logging.ERROR, always_print=True)

    return None

def main():
    # Audible API credentials and setup
    USERNAME = os.getenv('AUDIBLE_USERNAME')
    PASSWORD = os.getenv('AUDIBLE_PASSWORD')
    COUNTRY_CODE = os.getenv('AUDIBLE_COUNTRY_CODE', 'us')  # Default to 'us' if not set

    if not USERNAME or not PASSWORD:
        log_and_print("Audible credentials not found in environment variables", logging.ERROR, always_print=True)
        return

    auth = authenticate(USERNAME, PASSWORD, COUNTRY_CODE)
    if auth is None:
        return

    conn = create_connection(DATABASE)
    if conn is not None:
        create_table(conn)
        
        library, wishlist = fetch_audible_data(auth)
        
        books_added = 0
        books_updated = 0
        wishlist_items_added = 0  # Counter for wishlist items added

        library_titles = set()
        
        if library:
            library_books = parse_books(library, status="Library", downloaded=True)
            for book in library_books:
                library_titles.add(book["Title"])
                result = insert_or_update_book(conn, book)
                if result is True:
                    books_added += 1
                elif result is False:
                    books_updated += 1
        
        if wishlist:
            wishlist_books = parse_books(wishlist, status="Wishlist", downloaded=False)
            for book in wishlist_books:
                if book["Title"] not in library_titles:
                    result = insert_or_update_book(conn, book)
                    if result is True:
                        books_added += 1
                        wishlist_items_added += 1  # Increment wishlist items added counter
                    elif result is False:
                        books_updated += 1
        
        conn.close()
        log_and_print("Database connection closed", always_print=True)
        log_and_print(f"Number of wishlist items added: {wishlist_items_added}", always_print=True)  # Log wishlist items added
        log_and_print(f"Script execution complete. {books_added} books added and {books_updated} books updated in the database.", always_print=True)
    else:
        log_and_print("Error! Cannot create the database connection.", logging.ERROR, always_print=True)






if __name__ == '__main__':
    main()
