from bs4 import BeautifulSoup
import pandas as pd
import xml.etree.ElementTree as ET
from selenium_driverless import webdriver
import logging
import sys
import re
import time
import asyncio

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'referer': 'https://www.skroutz.gr/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
}

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("skroutz_shop_find.log", mode='w', encoding='utf-8'),
                        logging.StreamHandler(sys.stdout)]
                    )


def export_text(text):
    match = re.search(r'Διεύθυνση έδρας:\s*(.+)', text)
    if match:
        address = match.group(1)
        match = re.search(r'[^,]+,\s*([^,]+),\s*([^,]+)$', address)
        if match:
            city, nomos = match.groups()
            return city, nomos
    else:
        return None, None


async def get_page_content(url):
    for i in range(2):
        try:
            options = webdriver.ChromeOptions()
            async with webdriver.Chrome(options=options) as driver:

                await driver.get(url, wait_load=True)
                await asyncio.sleep(5)  # Use asyncio.sleep for asynchronous sleep
                source = await driver.page_source
                soup = BeautifulSoup(source, 'html.parser')

                product = soup.find('div', class_=lambda value: value and value.startswith('company-info'))
                if product:
                    location = product.text.strip()
                    name_element = soup.find('h1', class_="page-title")
                    name = name_element.text.strip() if name_element else None
                    return name, location
                else:
                    if i == 1:
                        logging.info(f"The URL: {url} couldn't open after {i + 1} attempts. Skipping to the next URL")
                        return None, None
                    logging.info(f"No products found in {i + 1} attempt for {url}. Lets retry.")
                    await asyncio.sleep(5)

        except Exception as e:
            logging.error(f'Error processing page content: {e}')
            if i == 1:
                logging.info(f"The URL: {url} couldn't open after {i + 1} attempts. Skipping to the next URL.")
                return None, None
            logging.info(f"No products found in {i + 1} attempt for {url}. Lets retry!")
            await asyncio.sleep(5)
    return None, None


async def process_url(loc_url):
    try:
        name, location_text = await get_page_content(loc_url)

        if location_text:
            city, nomos = export_text(location_text)
            if city and nomos:
                logging.info(f'Shop {name} is in {city}, {nomos}')
                return {'Name': name, 'City': city, 'Prefecture': nomos}
    except Exception as e:
        logging.error(f"Error processing {loc_url}: {e}")
    return None


async def process_urls_batch(urls):
    tasks = []
    for url in urls:
        tasks.append(process_url(url))
    return await asyncio.gather(*tasks)


async def main():
    try:
        # Load and parse the XML file
        xml_file = "shop_sitemap.xml"
        tree = ET.parse(xml_file)
        root = tree.getroot()

        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        data = []

        urls = [url_element.text for url_element in root.findall('ns:url/ns:loc', namespace)
                if 'https://www.skroutz.gr/shop/by/' not in url_element.text
                and url_element.text != 'https://www.skroutz.gr/shop']

        batch_size = 1

        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            results = await process_urls_batch(batch)
            data.extend([result for result in results if result])
            time.sleep(10)

        if data:
            df = pd.DataFrame(data)
            df.to_excel("Shops_location.xlsx", index=False)
            logging.info(f"Successfully processed {len(data)} shops.")
        else:
            logging.info("No matching locations found.")

    except Exception as e:
        logging.error(f"Main error: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Program error: {e}")
