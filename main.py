import logging
from robocorp.tasks import task
from api.utils import export_dataframe_to_excel
from api.web_scraping import WebScraper
from api.text_processing import TextProcessing
from api.constants import LOGGING_INFO, SEARCH_KEYWORD


@task
def extract_news_data():
    # Set logging level
    logging.basicConfig(level=LOGGING_INFO)

    # Initialize WebScraper and TextProcessing
    scraper = WebScraper()
    text_processing = TextProcessing()

    try:
        logging.info(f"Starting news extraction with keyword: {SEARCH_KEYWORD}")

        # Perform the web scraping
        df = scraper.start_scraping(SEARCH_KEYWORD)

        # Check if the DataFrame has any data
        if not df.empty:
            logging.info("Successfully scraped data. Proceeding with text processing.")

            # Perform text processing
            df = text_processing.post_process_texts(df)

            # Export the processed data to an Excel file
            export_dataframe_to_excel(df)
            logging.info("Data processing complete. Data exported to Excel.")
        else:
            logging.warning("No data was scraped. Skipping text processing and export.")

    except Exception as e:
        logging.error(f"An error occurred during the news extraction process: {e}")
        # Optionally: send an alert, save a log file, or perform other recovery actions

