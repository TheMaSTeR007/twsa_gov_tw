from scrapy.cmdline import execute
from urllib.parse import urlencode
from lxml.html import fromstring
from typing import Iterable
from scrapy import Request
import pandas as pd
import unicodedata
import random
import string
import scrapy
import time
import json
import evpn
import os
import re


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def parse_text_to_dict(text):
    """
    Parses a given text string to extract key-value pairs, ensuring social media details are handled separately.

    Args:
        text (str): The input string containing labeled data.

    Returns:
        dict: A dictionary with the extracted key-value pairs.
    """
    # Dynamic regex for headers and their values
    pattern = r"([A-Za-z\u4e00-\u9fff]+):\s*(.+?(?=(?:\n[\u4e00-\u9fff]+:)|\Z))"

    # Extract all key-value pairs
    matches = re.findall(pattern, text, re.DOTALL)

    # Process the results into a dictionary
    data = {}
    for key, value in matches:
        value = value.strip()
        if ", " in value and "," not in value:  # Handle comma-separated values like emails
            value = ' | '.join(v.strip() for v in value.split(","))
        elif "," in value and ", " not in value:  # Handle comma-separated values like emails
            value = ' | '.join(v.strip() for v in value.split(","))
        elif "\n" in value:  # Handle multi-line values like social media links
            value = ' | '.join(v.strip() for v in value.splitlines())
        data[key] = value

    # Handle specific case for "Social Media Details" being part of another value
    if "網址" in data and "Social Media Details" in data["網址"]:
        value_text = data.get("網址", 'N/A').split('Social Media Details')
        # Use regex to clean unwanted characters
        data["網址"] = re.sub(pattern=r"^[|:]+|[|:]+$", repl="", string=value_text[0].strip())
        data["Social Media Details"] = re.sub(pattern=r"^[|:]+|[|:]+$", repl="", string=value_text[1].strip())
    return data


class TwsaOrgTaiwanSpider(scrapy.Spider):
    name = "twsa_org_taiwan"

    def __init__(self):
        self.start = time.time()
        super().__init__()
        print('Connecting to VPN (TAIWAN)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (TAIWAN)
        self.api.connect(country_id='108')  # TAIWAN country code for vpn
        time.sleep(10)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        self.final_data_list = list()  # List of data to make DataFrame then Excel

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename = fr"{self.excel_path}/{self.name}.xlsx"  # Filename with Scrape Date

        self.browsers = ["chrome110", "edge99", "safari15_5"]

        self.cookies = {
            '__utmz': '177061027.1732859996.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
            'ASP.NET_SessionId': 'nyvyymgagbo2npxqugdy4tj5',
            '__utma': '177061027.528475930.1732859996.1733742110.1733804794.11',
            '__utmc': '177061027',
            '__utmt': '1',
            '__utmb': '177061027.2.10.1733804794',
        }
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'http://web.twsa.org.tw',
            'Referer': 'http://web.twsa.org.tw/alert/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        self.cookies_form_data = {
            '__utmz': '177061027.1732859996.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
            'ASP.NET_SessionId': 'nyvyymgagbo2npxqugdy4tj5',
            '__utmc': '177061027',
            '__utma': '177061027.528475930.1732859996.1733813366.1733819802.14',
            '__utmb': '177061027.2.10.1733819802',
        }
        self.headers_form_data = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'http://web.twsa.org.tw',
            'Referer': 'http://web.twsa.org.tw/alert/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

        self.form_data = json.loads(open(file=r'../Form_Data_Files/form_data.json', mode='r', encoding='utf-8').read())
        self.form_data_detail_page = json.loads(open(file=r'../Form_Data_Files/form_data_detail.json', mode='r', encoding='utf-8').read())
        # self.form_data_viewstate = self.form_data.get('__VIEWSTATE', 'N/A')

    def start_requests(self) -> Iterable[Request]:
        # Use scrapy.FormRequest for a POST request
        yield scrapy.FormRequest(url="http://web.twsa.org.tw/alert/", cookies=self.cookies, headers=self.headers, formdata=self.form_data,
                                 callback=self.parse, dont_filter=True)

    def parse(self, response, **kwargs):
        parsed_tree = fromstring(response.text)

        alerts_table = parsed_tree.xpath('//table//a[contains(@id, "MainContent_ucAlertList_rptAlert_lbtnMore_")]/@href')
        for event_target_string in alerts_table[:10]:
            match = re.search(pattern=r"'(.*)',", string=event_target_string)  # Find matches
            if match:  # Extract and print values if a match is found
                event_target = match.group(1)  # First value

                # Send Request on detail page
                new_form_data = self.form_data_detail_page.copy()
                # Changing event_target and view_state value in form-data to get desired response
                print(event_target)
                new_form_data['__EVENTTARGET'] = event_target

                # Convert the form data to a URL-encoded string
                yield scrapy.FormRequest(url="http://web.twsa.org.tw/alert/", method='POST', cookies=self.cookies_form_data, headers=self.headers_form_data, formdata=new_form_data, callback=self.parse_detail_page, dont_filter=True, errback=self.handle_error)
            else:
                print("No match found.")
            # break

    def handle_error(self, failure):
        print('Handle error...', failure)
        self.logger.error(f"Request failed: {failure}")

    def parse_detail_page(self, response, **kwargs):
        print('parse detail...')
        print(response.status)
        parsed_tree = fromstring(response.text)

        trial_string = ' '.join(parsed_tree.xpath('//div[@class="generalForm"]/fieldset[@class="register"]/fieldset[@class="register"]/p//text()'))
        print(remove_extra_spaces(trial_string))

        data_dict = dict()
        outer_labels = [text.strip() for text in parsed_tree.xpath('//div[@class="generalForm"]/fieldset[@class="register"]/p/label//text()')]
        outer_values = [text.strip() for text in parsed_tree.xpath('//div[@class="generalForm"]/fieldset[@class="register"]/p/label/following-sibling::font//text()')]
        for outer_label, outer_value in zip(outer_labels, outer_values):
            data_dict[outer_label] = outer_value

        # inner_labels = [text.strip() for text in parsed_tree.xpath('//div[@class="generalForm"]/fieldset[@class="register"]/fieldset[@class="register"]/p/label//text()')]
        # inner_values = [text.strip() for text in parsed_tree.xpath('//div[@class="generalForm"]/fieldset[@class="register"]/fieldset[@class="register"]/p/label/following-sibling::font//text()')]
        # for inner_label, inner_value in zip(inner_labels, inner_values):
        #     data_dict[inner_label] = inner_value

        all_inner_text = parsed_tree.xpath('//div[@class="generalForm"]/fieldset[@class="register"]/fieldset[@class="register"]/p//text()')


        print(all_inner_text)
        # print(parse_text_to_dict(all_inner_text))

        # print(data_dict)

    def close(self, reason):
        print('closing spider...')
        # print("Converting List of Dictionaries into DataFrame, then into Excel file...")
        # try:
        #     print("Creating Native sheet...")
        #     data_df = pd.DataFrame(self.final_data_list)
        #     # data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning
        #     data_df.insert(loc=0, column='id', value=range(1, len(data_df) + 1))  # Add 'id' column at position 1
        #     # data_df.set_index(keys='id', inplace=True)  # Set 'id' as index for the Excel output
        #     with pd.ExcelWriter(path=self.filename, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
        #         data_df.to_excel(excel_writer=writer, index=False)
        #
        #     print("Native Excel file Successfully created.")
        # except Exception as e:
        #     print('Error while Generating Native Excel file:', e)
        # if self.api.is_connected:  # Disconnecting VPN if it's still connected
        #     self.api.disconnect()
        #     print('VPN Connected!' if self.api.is_connected else 'VPN Disconnected!')
        #
        # end = time.time()
        # print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute(f'scrapy crawl {TwsaOrgTaiwanSpider.name}'.split())
