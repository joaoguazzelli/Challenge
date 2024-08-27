import re
import logging
import pandas as pd
from api.constants import SEARCH_KEYWORD
from typing import Optional

class TextProcessing:
    
    def __init__(self):
        """Initialize the TextProcessing class with a logger."""
        self.logger = logging.getLogger(__name__)

    def count_search_phrases(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Count occurrences of the search keyword in the Title and Description columns.
        
        Args:
            df (pd.DataFrame): The DataFrame containing 'Title' and 'Description' columns.

        Returns:
            pd.DataFrame: The DataFrame with an additional column '#Search Phrase Matches'.
        """
        if not {'Title', 'Description'}.issubset(df.columns):
            self.logger.error("DataFrame must contain 'Title' and 'Description' columns.")
            return df
        
        search_pattern = re.compile(rf'\b{SEARCH_KEYWORD}\b', re.IGNORECASE)
        
        # Combine Title and Description once for performance
        df['Combined_Text'] = df['Title'] + ' ' + df['Description']
        
        # Count occurrences of the search keyword
        df['#Search Phrase Matches'] = df['Combined_Text'].apply(
            lambda text: len(search_pattern.findall(text))
        )
        
        # Drop the temporary Combined_Text column
        df.drop(columns=['Combined_Text'], inplace=True)
        
        self.logger.info(f"Processed search phrase matching for keyword '{SEARCH_KEYWORD}'.")
        return df

    def check_money_text(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Check for the presence of money-related text in the Title and Description columns.
        
        Args:
            df (pd.DataFrame): The DataFrame containing 'Title' and 'Description' columns.

        Returns:
            pd.DataFrame: The DataFrame with an additional column 'Contains Money'.
        """
        if not {'Title', 'Description'}.issubset(df.columns):
            self.logger.error("DataFrame must contain 'Title' and 'Description' columns.")
            return df
        
        # Compile the money-related patterns
        money_pattern = re.compile(r'(\$[0-9]+\,?[0-9]*\.?[0-9]*)|([0-9]+ dollars)|([0-9]+ USD)', re.IGNORECASE)
        
        # Combine Title and Description once for performance
        df['Combined_Text'] = df['Title'] + ' ' + df['Description']
        
        # Check for the presence of money-related text
        df['Contains Money'] = df['Combined_Text'].apply(
            lambda text: bool(money_pattern.search(text))
        )
        
        # Drop the temporary Combined_Text column
        df.drop(columns=['Combined_Text'], inplace=True)
        
        self.logger.info("Processed money-related text detection.")
        return df

    def post_process_texts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply post-processing to the DataFrame by adding columns for search phrase matches
        and money-related text detection.
        
        Args:
            df (pd.DataFrame): The DataFrame containing 'Title' and 'Description' columns.

        Returns:
            pd.DataFrame: The DataFrame with additional columns '#Search Phrase Matches' and 'Contains Money'.
        """
        try:
            self.logger.info('Starting post-processing of texts.')
            df = self.count_search_phrases(df)
            df = self.check_money_text(df)
            self.logger.info('Post-processing completed successfully.')
        except Exception as e:
            self.logger.error(f"Error during post-processing: {e}")
        return df
