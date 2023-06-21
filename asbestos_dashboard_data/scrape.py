import datetime
import os
import re
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from loguru import logger
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from . import DATA_DIR
from .data.asbestos import extract_asbestos_data


@contextmanager
def cwd(path):
    oldpwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpwd)


def load_chromedriver_path():
    """Try to find the latest install chromedriver path"""

    try:
        # Try to install the path
        path = ChromeDriverManager().install()
    except:
        chromedriver_path = Path("~/.wdm/drivers/chromedriver/mac64/").expanduser()
        if not chromedriver_path.exists():
            raise ValueError("Cannot find working chromedriver installation")

        last_modified = sorted(
            [f for f in chromedriver_path.glob("*")], key=os.path.getmtime
        )[-1]
        path = str(last_modified / "chromedriver")

    return path


@contextmanager
def wait_for_new_window(driver, timeout=30):
    handles_before = driver.window_handles
    yield
    WebDriverWait(driver, timeout).until(
        lambda driver: len(handles_before) != len(driver.window_handles)
    )


def get_webdriver(browser, dirname, debug=False):
    """Get the webdriver."""

    if browser == "chrome":
        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")
        if not debug:
            options.add_argument("--headless")

        # Setup the download directory for PDFs
        profile = {
            "plugins.plugins_list": [
                {"enabled": False, "name": "Chrome PDF Viewer"}
            ],  # Disable Chrome's PDF Viewer
            "download.default_directory": dirname,
            "download.extensions_to_open": "applications/pdf",
        }
        options.add_experimental_option("prefs", profile)

        # Initialize with options
        service = Service(load_chromedriver_path())
        driver = webdriver.Chrome(service=service, options=options)
    else:
        raise ValueError("Unknown browser type, should be 'chrome'")

    return driver


@dataclass
class DatabaseScraper:
    """A class to download and parse a PDF."""

    browser: str = "chrome"
    debug: bool = False
    ndays: int = 7

    def __post_init__(self):

        today = datetime.datetime.today()  #
        d = datetime.timedelta(days=self.ndays)
        start = today - d

        self.end_date = today.strftime("%m-%d-%Y")
        self.start_date = start.strftime("%m-%d-%Y")

        logger.info(
            f"Downloading raw asbestos data for {self.start_date} to {self.end_date}"
        )

    def _init(self, dirname):
        """Initialization function."""

        # Get the driver
        self.driver = get_webdriver(self.browser, dirname, debug=self.debug)

    def run(self):
        """Scrape remote PDFs."""

        with tempfile.TemporaryDirectory() as tmpdir:

            # Change the path
            with cwd(tmpdir):

                # Initialize if we need to
                if not hasattr(self, "driver"):
                    self._init(tmpdir)

                self.driver.get("https://citizenserve.com/philagov")

                link_text = "Reports"
                WebDriverWait(self.driver, 10).until(
                    EC.visibility_of_element_located((By.PARTIAL_LINK_TEXT, link_text))
                )

                a = self.driver.find_element(By.PARTIAL_LINK_TEXT, link_text)
                a.click()

                link_text = "Electronic Asbestos Notifications Report"
                WebDriverWait(self.driver, 10).until(
                    EC.visibility_of_element_located((By.PARTIAL_LINK_TEXT, link_text))
                )

                with wait_for_new_window(self.driver):
                    a = self.driver.find_element(By.PARTIAL_LINK_TEXT, link_text)
                    a.click()

                self.driver.switch_to.window(self.driver.window_handles[1])
                WebDriverWait(self.driver, 10).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "#Param_0"))
                )
                start_input = self.driver.find_element(By.CSS_SELECTOR, "#Param_0")
                end_input = self.driver.find_element(By.CSS_SELECTOR, "#Param_1")

                start_input.clear()
                start_input.send_keys(self.start_date)
                end_input.clear()
                end_input.send_keys(self.end_date)

                self.driver.find_element(By.LINK_TEXT, "SUBMIT").click()
                WebDriverWait(self.driver, 10).until(
                    EC.visibility_of_element_located(
                        (By.CSS_SELECTOR, ".icon-external-link")
                    )
                )

                self.driver.execute_script("javascript:exportToExcel();")
                excel_file = None
                try:
                    download_dir = Path(tmpdir)
                    # Initialize
                    excel_files = list(download_dir.glob("*.xlsx"))
                    total_sleep = 0
                    while not len(excel_files) and total_sleep <= 20:
                        time.sleep(1)
                        total_sleep += 1
                        excel_files = list(download_dir.glob("*.xlsx"))

                    if len(excel_files):

                        # Extract the clean data
                        excel_file = excel_files[0]
                        clean_data = extract_asbestos_data(filename=excel_file)

                        # The existing latest
                        raw_data_files = sorted(
                            Path(DATA_DIR / "raw").glob("Citizen*.xlsx"),
                            key=lambda f: os.path.getmtime(f),
                        )
                        filename = raw_data_files[-1]

                        # Combine and save
                        logger.info(
                            "Saving new raw database file to data/raw/CitizenserveReport-Latest.xlsx"
                        )
                        raw_data = pd.concat(
                            [
                                pd.read_excel(excel_file, sheet_name=0),
                                pd.read_excel(filename, sheet_name=0),
                            ]
                        ).drop_duplicates(subset=["Permit #"])
                        raw_data.to_excel(
                            DATA_DIR / "raw" / "CitizenserveReport-Latest.xlsx",
                            index=False,
                        )

                        # Return
                        return clean_data
                    else:
                        raise ValueError("Excel download failed")
                finally:

                    # Remove the file after we are done!
                    if excel_file is not None and excel_file.exists():
                        excel_file.unlink()


def wait_for_element(driver, css_selector, time_limit=10):

    # Wait explicitly until search results load
    WebDriverWait(driver, time_limit).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, css_selector)),
    )


def _get_url(driver, permit_number):

    start_url = "https://www.citizenserve.com/Portal/PortalController?Action=showSearchPage&ctzPagePrefix=Portal_&installationID=173&original_iid=0&original_contactID=0"
    driver.get(start_url)

    # Initial dropdown for permits
    dropdown_selector = "select.form-control"
    wait_for_element(driver, dropdown_selector)
    dropdown = driver.find_element(By.CSS_SELECTOR, dropdown_selector)
    dropdown_select = Select(dropdown)
    dropdown_select.select_by_visible_text("Permits")

    # Get the input element for the permit number
    input_selector = "#PermitNumber"
    wait_for_element(driver, input_selector)
    input_tag = driver.find_element(By.CSS_SELECTOR, input_selector)

    # Clear any existing entry
    input_tag.clear()

    # Input our permit number
    input_tag.send_keys(permit_number)

    # Click select
    driver.find_element(By.CSS_SELECTOR, "#submitRow button").click()

    # The link
    link_selector = "#resultContent > table > tbody > tr > td:nth-child(1) > a"
    wait_for_element(driver, link_selector)
    a = driver.find_element(By.CSS_SELECTOR, link_selector)
    url = a.get_attribute("href")

    # Extract
    match = re.match("javascript:openURLLink\(.*(PortalController.*)%.*\)", url).group(
        1
    )
    return f"https://www.citizenserve.com/Portal/{match}"


def scrape_permit_urls(permit_numbers, log_freq=10):
    """Scrape the permit URLs."""

    # Get the driver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    service = Service(load_chromedriver_path())
    driver = webdriver.Chrome(service=service, options=options)

    out = []
    for i, permit_number in enumerate(permit_numbers):

        if i % log_freq == 0:
            logger.info(i)

        try:
            url = _get_url(driver, permit_number)
        except Exception as e:
            logger.exception(f"exception occurred for permit number = {permit_number}")
            raise e
        out.append((permit_number, url))

        time.sleep(1)

    return pd.DataFrame(out, columns=["permit_number", "permit_url"])


def update_permit_urls(data):
    """Update permit URLs."""

    # Get the permit numbers too
    if "permit_url" in data.columns:
        missing = data["permit_url"].isnull()
        permit_numbers = data.loc[missing, "permit_number"].unique()
    else:
        permit_numbers = data["permit_number"].unique()

    # Get the permit URLs
    if len(permit_numbers):
        logger.info(f"Scraping permit URL data for {len(permit_numbers)} permits")
        url_data = scrape_permit_urls(permit_numbers)
        logger.info("  ...done")

        # Merge
        out = pd.merge(
            data, url_data, on="permit_number", how="left", suffixes=("", "_y")
        )
        if "permit_url_y" in out.columns:
            out["permit_url"] = out["permit_url"].fillna(out["permit_url_y"])
            out = out.drop(labels=["permit_url_y"], axis=1)

        # Save
        out[["permit_url", "permit_number"]].drop_duplicates().to_csv(
            DATA_DIR / "interim" / "permit-number-urls.csv", index=False
        )
    else:
        out = data

    return out
