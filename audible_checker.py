import os
import sqlite3
import logging
from sqlite3 import Error
from logging.handlers import RotatingFileHandler
import json
import subprocess
from unittest import result

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
TEST_MODE = True  # Change to True for more verbosity

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

def fetch_audible_data():
    """ Fetch library and wishlist data using audible-cli """
    try:
        # Ensure the config file and auth file are properly set up
        config_dir = os.path.expanduser("~/.audible")
        config_file = os.path.join(config_dir, "config.toml")
        
        # Ensure the config directory exists
        os.makedirs(config_dir, exist_ok=True)

        # Check if the config file exists
        if not os.path.exists(config_file):
            error_message = (
                "Config file not found. Please set up your Audible configuration using the audible-cli by running 'audible quickstart'."
            )
            log_and_print(error_message, logging.ERROR, always_print=True)
            return None, None

        # Profile name used in the configuration
        profile_name = "primary"

        library_command = ["audible", "-P", profile_name, "library", "list"]
        wishlist_command = ["audible", "-P", profile_name, "wishlist", "list"]

        library_result = subprocess.run(library_command, capture_output=True, text=True)
        wishlist_result = subprocess.run(wishlist_command, capture_output=True, text=True)

        if TEST_MODE:
            log_and_print(f"Library command result: {library_result}", logging.DEBUG, always_print=True)
            log_and_print(f"Wishlist command result: {wishlist_result}", logging.DEBUG, always_print=True)

            # Save raw responses to files
            with open("library_raw_response.json", "w") as f:
                f.write(library_result.stdout)
            with open("wishlist_raw_response.json", "w") as f:
                f.write(wishlist_result.stdout)

        if library_result.returncode != 0:
            if "Provided profile not found in config" in library_result.stderr:
                log_and_print(
                    f"Profile '{profile_name}' not found in config. Please ensure you've run 'audible quickstart' and set up your profiles correctly.",
                    logging.ERROR,
                    always_print=True
                )
            else:
                log_and_print(f"Error fetching library data: {library_result.stderr}", logging.ERROR, always_print=True)
            return None, None

        if wishlist_result.returncode != 0:
            if "Provided profile not found in config" in wishlist_result.stderr:
                log_and_print(
                    f"Profile '{profile_name}' not found in config. Please ensure you've run 'audible quickstart' and set up your profiles correctly.",
                    logging.ERROR,
                    always_print=True
                )
            else:
                log_and_print(f"Error fetching wishlist data: {wishlist_result.stderr}", logging.ERROR, always_print=True)
            return None, None

        library_output = library_result.stdout.strip()
        wishlist_output = wishlist_result.stdout.strip()

        if TEST_MODE:
            log_and_print(f"Raw library output: {library_output}", logging.DEBUG, always_print=True)
            log_and_print(f"Raw wishlist output: {wishlist_output}", logging.DEBUG, always_print=True)

        if not library_output:
            log_and_print("Library command returned no output", logging.ERROR, always_print=True)
            return None, None

        if not wishlist_output:
            log_and_print("Wishlist command returned no output", logging.ERROR, always_print=True)
            return None, None

        # Parse the plain text output into a structured format
        def parse_output(output):
            books = []
            for line in output.split('\n'):
                parts = line.split(': ', 2)
                if len(parts) == 3:
                    asin, author, title = parts
                    books.append({
                        "ASIN": asin,
                        "Author": author,
                        "Title": title,
                        "Description": "",
                        "Length": "",
                        "EPUB_Column": "",
                        "Downloaded": False,
                        "Cover_URL": "",
                        "Finished": False,
                        "Status": ""
                    })
            return books

        library_data = parse_output(library_output)
        wishlist_data = parse_output(wishlist_output)

        log_and_print("Fetched library and wishlist data using audible-cli", always_print=True)
        return library_data, wishlist_data
    except Exception as e:
        log_and_print(f"Error fetching data using audible-cli: {e}", logging.ERROR, always_print=True)
        return None, None


def parse_books(data, status, downloaded=False):
    """ Parse book data """
    books = []

    if not isinstance(data, list):
        log_and_print(f"Expected a list of books, but got: {data}", logging.ERROR, always_print=True)
        return books

    for item in data:
        author = item.get("Author", "Unknown")
        title = item.get("Title", "Unknown Title")
        description = item.get("Description", "No description available")
        length = item.get("Length", "Unknown")
        cover_url = item.get("Cover_URL", None)
        finished = item.get("Finished", False)

        book = {
            "Author": author,
            "Title": title,
            "Description": description,
            "Length": length,
            "EPUB_Column": "",  # Placeholder for EPUB reference
            "Downloaded": downloaded,  # Set Downloaded based on input parameter
            "Cover_URL": cover_url,
            "Finished": finished,
            "Status": status
        }
        books.append(book)
    return books

def main():
    conn = create_connection(DATABASE)
    if conn is not None:
        create_table(conn)
        
        library, wishlist = fetch_audible_data()
        
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
