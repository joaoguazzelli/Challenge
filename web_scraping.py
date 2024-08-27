import logging
import pandas as pd
from selenium import webdriver
from RPA.Browser.Selenium import Selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from contextlib import contextmanager

from api.utils import parse_date, download_image, should_continue_scraping_based_on_time
from api.constants import BASE_URL, TIMEOUT_SECONDS, CATEGORY_FILTER

class ScrapingError(Exception):
    """Custom exception class for Web Scraping errors."""
    pass


class WebScraper:

    def __init__(self, timeout=TIMEOUT_SECONDS):
        """Initialize the WebScraper with default settings."""
        self.browser = None
        self.log = logging.getLogger(__name__)
        self.scrape_continues = True
        self.timeout = timeout

    def configure_browser_options(self):
        """Configure browser options for headless Chrome."""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-web-security')
        options.add_argument('--start-maximized')
        options.add_argument('--remote-debugging-port=9222')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        return options

    @contextmanager
    def managed_browser(self):
        """Context manager to ensure browser is properly closed."""
        self.launch_browser()
        try:
            yield
        finally:
            if self.browser:
                self.browser.close_browser()

    @retry(reraise=True, retry=retry_if_exception_type(ScrapingError),
           stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def launch_browser(self):
        """Launch the browser with the configured options."""
        try:
            self.browser = Selenium()
            self.browser.set_selenium_timeout(self.timeout)
            self.log.debug('Initializing Chrome Browser and loading URL')
            self.browser.open_available_browser(browser_selection='Chrome',
                                                url=BASE_URL,
                                                options=self.configure_browser_options())
            self.log.debug('Page loaded successfully')
        except Exception as error:
            logging.exception("Error launching browser: %s", error)
            raise ScrapingError

    def close_popup_overlays(self):
        """Close any overlay popups that appear on the page."""
        try:
            WebDriverWait(self.browser.driver, self.timeout).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '#onetrust-accept-btn-handler')))
        except Exception as e:
            self.log.info('No advertisement popup detected to close: %s', e)

        try:
            self.browser.click_element_when_clickable('class=fancybox-item.fancybox-close')
        except Exception as e:
            self.log.info('No fancybox popup detected to close: %s', e)

    @retry(reraise=True, retry=retry_if_exception_type(ScrapingError),
           stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def apply_filter(self) -> None:
        """Apply the category filter on the search results page."""
        try:
            logging.info(f'Applying filter for category: {CATEGORY_FILTER}')
            self.browser.driver.refresh()
            self.close_popup_overlays()

            self.browser.click_element_when_visible('class=SearchResultsModule-filters-open')
            self.browser.click_element_when_visible('class=SearchFilter-content')

            category_elements = self.browser.find_elements('class=CheckboxInput-label')

            for category in category_elements:
                if CATEGORY_FILTER in str(category.text).lower():
                    category.click()
                    logging.info(f'Filter applied for category: {category.text}')

            self.browser.click_element_when_visible('class=SearchResultsModule-filters-applyButton')
            logging.info(f'Filter for category {CATEGORY_FILTER} applied successfully')
        except Exception as error:
            logging.exception("Error applying filter: %s", error)
            raise ScrapingError

    @retry(reraise=True, retry=retry_if_exception_type(ScrapingError),
           stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def navigate_to_next_page(self) -> None:
        """Navigate to the next page of search results."""
        logging.info('Navigating to the next results page')
        try:
            self.browser.click_element_when_visible('class=Pagination-nextPage')
        except Exception as error:
            logging.exception("Error navigating to next page: %s", error)
            self.browser.driver.refresh()
            raise ScrapingError

    @retry(reraise=True, retry=retry_if_exception_type(ScrapingError),
           stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def execute_search(self, query: str) -> None:
        """Execute a search on the web page."""
        try:
            logging.info('Executing search on the web page')

            self.close_popup_overlays()

            self.browser.click_element_when_visible('class=SearchOverlay-search-button')
            self.browser.input_text_when_element_is_visible('class=SearchOverlay-search-input', query)
            self.browser.click_element_when_visible('class=SearchOverlay-search-submit')

            logging.info('Sorting results by newest')
            dropdown = self.browser.find_element('class=Select-input')
            select_element = Select(dropdown)
            select_element.select_by_visible_text('Newest')
            logging.info('Sorting applied successfully')
        except Exception as error:
            logging.exception("Error executing search: %s", error)
            self.browser.go_to(BASE_URL)
            raise ScrapingError

    @retry(reraise=True, retry=retry_if_exception_type(ScrapingError),
           stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
    def fetch_results(self) -> pd.DataFrame:
        """Fetch and parse the search results into a DataFrame."""
        try:
            self.browser.driver.refresh()
            self.close_popup_overlays()

            WebDriverWait(self.browser.driver, self.timeout).until(
                EC.visibility_of_element_located((By.CLASS_NAME, 'SearchResultsModule-results')))

            search_module = self.browser.find_element('class=SearchResultsModule-results')
            news_elements = search_module.find_elements(by=By.CLASS_NAME, value='PagePromo')
            logging.info(f'Fetched {len(news_elements)} articles from the search results')

            processed_articles = []
            for article in news_elements:
                article_data = {}

                article_data['URL'] = str(article.find_element(by=By.CLASS_NAME, value='Link').get_attribute('href'))
                article_data['Title'] = str(article.get_attribute('data-gtm-region'))

                try:
                    article_data['Description'] = str(article.find_element(by=By.CLASS_NAME, value='PagePromo-description').text)
                except Exception as error:
                    article_data['Description'] = ''

                try:
                    article_data['Image'] = download_image(article_data['URL'],
                                                           article.find_element(by=By.CLASS_NAME, value='Image').get_attribute('src'))
                except Exception:
                    article_data['Image'] = 'Image Not Available'

                try:
                    article_data['DateTime'] = parse_date(article.find_element(by=By.CLASS_NAME, value='Timestamp-template-now').text)
                except Exception:
                    try:
                        article_data['DateTime'] = parse_date(article.find_element(by=By.CLASS_NAME, value='Timestamp-template').text)
                    except Exception:
                        article_data['DateTime'] = 'DateTime Not Available'

                processed_articles.append(article_data)

            logging.info('Successfully processed the search results')
            return pd.DataFrame(processed_articles)
        except Exception as error:
            logging.exception("Error fetching results: %s", error)
            raise ScrapingError

    def start_scraping(self, query: str):
        """Main function to start the web scraping process."""
        with self.managed_browser():
            logging.info('Starting the web scraping process')
            self.execute_search(query)

            logging.info('Attempting to apply category filter')
            try:
                self.apply_filter()
            except ScrapingError as error:
                logging.exception("Filter application failed: %s", error)
                logging.info('Proceeding without filter.')

            page_counter = 1
            collected_data = pd.DataFrame()

            while self.scrape_continues:
                logging.info(f'Processing results on page {page_counter}')
                try:
                    page_data = self.fetch_results()
                    collected_data = pd.concat([collected_data, page_data], ignore_index=True)

                    self.scrape_continues = should_continue_scraping_based_on_time(page_data)
                    if self.scrape_continues:
                        self.navigate_to_next_page()
                        page_counter += 1
                except ScrapingError as error:
                    logging.exception("Error processing page %d: %s", page_counter, error)
                    self.scrape_continues = False

            return collected_data
