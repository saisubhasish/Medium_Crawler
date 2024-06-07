import time
import concurrent.futures

from typing import List
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from langchain_core.documents import Document

class ChromeLoader:
    """Scrape HTML pages from URLs using a headless instance of Chrome."""

    def __init__(self, urls: List[str]):
        self.urls = urls
        self.check_selenium_installed()

    def check_selenium_installed(self):
        """
        Checks if selenium is installed.

        Raises:
            ImportError: If selenium is not installed, raise an ImportError with a message
                indicating that selenium is required and how to install it.
        """
        try:
            import selenium
        except ImportError:
            raise ImportError(
                "selenium is required for ChromeLoader. "
                "Please install it with `pip install selenium`."
            )

    def get_selenium_driver(self, url: str) -> str:
        """
        Retrieves the Selenium driver and loads the specified URL.

        Args:
            url (str): The URL to load in the driver.

        Returns:
            str: The page source of the loaded URL.

        Raises:
            Exception: If an error occurs while loading the URL.

        """
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument('log-level=3')

        driver = webdriver.Chrome(options=chrome_options)

        try:
            driver.get(url)
            # Scroll to the bottom of the page to load lazy-loaded content
            for _ in range(100):
                time.sleep(0.5)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            results = driver.page_source
        except Exception as e:
            results = f"Error: {e}"
        finally:
            driver.quit()

        return results

    def lazy_load(self, url: str) -> Document:
        """
        Loads the HTML content of a web page using Selenium driver and returns a Document object.

        Args:
            url (str): The URL of the web page to load.

        Returns:
            Document: A Document object containing the page content and metadata.

        """
        html_content = self.get_selenium_driver(url)
        metadata = {"source": url}
        return Document(page_content=html_content, metadata=metadata)

    def load(self) -> List[Document]:
        """
        Loads the documents from the specified URLs using lazy loading.

        Returns:
            A list of Document objects representing the loaded documents.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
            futures = [executor.submit(self.lazy_load, url) for url in self.urls]
            concurrent.futures.wait(futures)

            documents = [future.result() for future in futures]

        return documents
