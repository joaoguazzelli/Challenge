import re
import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from api.constants import OUTPUT_FOLDER, MONTHS_PERIOD

# Constants
IMG_FOLDER_NAME = 'IMGS'
DATE_NOT_FOUND = 'DateTime Not Found'
ERROR_DATE_PROCESSING = 'Error processing date'

def parse_date(timestamp: str) -> str:
    """
    Parses a timestamp string into an ISO formatted date string.
    
    Args:
        timestamp (str): The timestamp string to parse.

    Returns:
        str: The parsed ISO formatted date string, or an error message if parsing fails.
    """
    timestamp = timestamp.strip()
    today_datetime = datetime.now()

    try:
        # Handle minutes ago
        if 'min ago' in timestamp.lower() or 'mins ago' in timestamp.lower():
            minutes_ago = int(re.findall(r'\d+', timestamp)[0])
            return (today_datetime - timedelta(minutes=minutes_ago)).isoformat(timespec='minutes')

        # Handle hours ago
        if 'hour ago' in timestamp.lower() or 'hours ago' in timestamp.lower():
            hours_ago = int(re.findall(r'\d+', timestamp)[0])
            return (today_datetime - timedelta(hours=hours_ago)).isoformat(timespec='minutes')

        # Handle yesterday
        if 'yesterday' in timestamp.lower():
            return (today_datetime - timedelta(days=1)).isoformat(timespec='minutes')

        # Handle other dates, add current year if not present
        if timestamp and not re.search(r'\b\d{4}\b', timestamp):
            timestamp = f'{timestamp}, {today_datetime.year}'

        # Parse the formatted timestamp
        return datetime.strptime(timestamp, '%B %d, %Y').isoformat(timespec='minutes')

    except ValueError as e:
        logging.warning(f"Error processing date '{timestamp}': {e}")
        return ERROR_DATE_PROCESSING


def download_image(news_url: str, img_src: str) -> Optional[str]:
    """
    Downloads an image from a given URL and saves it to the output folder.
    
    Args:
        news_url (str): The URL of the news article.
        img_src (str): The source URL of the image.

    Returns:
        Optional[str]: The filename of the downloaded image, or None if download failed.
    """
    img_folder = Path(OUTPUT_FOLDER) / IMG_FOLDER_NAME
    img_folder.mkdir(parents=True, exist_ok=True)

    img_filename = f'{news_url.split("/")[-1]}.png'
    img_path = img_folder / img_filename

    try:
        response = requests.get(img_src)
        response.raise_for_status()
        with open(img_path, 'wb') as handler:
            handler.write(response.content)
        return img_filename
    except (requests.RequestException, OSError) as e:
        logging.error(f"Error downloading image from {img_src}: {e}")
        return None


def should_continue_scraping_based_on_time(df: pd.DataFrame) -> bool:
    """
    Determines whether scraping should continue based on the earliest DateTime in the DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the news data.

    Returns:
        bool: True if scraping should continue, False otherwise.
    """
    # Filter out rows with invalid dates
    valid_dates_df = df[(df['DateTime'] != DATE_NOT_FOUND) &
                        (~df['DateTime'].str.contains(ERROR_DATE_PROCESSING))]

    if valid_dates_df.empty:
        logging.debug("No valid dates found, continuing scraping.")
        return True

    earliest_datetime_str = valid_dates_df.iloc[-1]['DateTime']
    logging.debug(f'Earliest DateTime found: {earliest_datetime_str}')

    try:
        earliest_datetime = datetime.fromisoformat(earliest_datetime_str)
        current_date = datetime.now()
        diff_in_months = (current_date.year - earliest_datetime.year) * 12 + current_date.month - earliest_datetime.month

        if MONTHS_PERIOD == 0 and diff_in_months > 0:
            return False
        if MONTHS_PERIOD > 0 and diff_in_months >= MONTHS_PERIOD:
            return False

    except ValueError as e:
        logging.error(f"Error parsing earliest DateTime '{earliest_datetime_str}': {e}")
        return True  # Continue if there's an issue with date parsing

    return True


def export_dataframe_to_excel(df: pd.DataFrame) -> None:
    """
    Exports the DataFrame to an Excel file in the output folder.
    
    Args:
        df (pd.DataFrame): The DataFrame to export.
    """
    output_path = Path(OUTPUT_FOLDER) / f'Execution_{datetime.now().strftime("%Y%m%d-%H%M%S")}.xlsx'
    try:
        df.to_excel(output_path, index=False)
        logging.info(f"Data exported to {output_path}")
    except Exception as e:
        logging.error(f"Error exporting DataFrame to Excel: {e}")
