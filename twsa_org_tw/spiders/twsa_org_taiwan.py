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


def get_xpath_text(text_div) -> str:
    text = text_div.xpath('.//text()').get(default='N/A').replace('：', '').strip()  # Extract text or return an empty string
    return text


def parse_text_to_dict(text: str, data_dict: dict) -> dict:\
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
    for key, value in matches:
        value = value.strip()
        if ", " in value and "," not in value:  # Handle comma-separated values like emails
            value = ' | '.join(v.strip() for v in value.split(","))
        elif "," in value and ", " not in value:  # Handle comma-separated values like emails
            value = ' | '.join(v.strip() for v in value.split(","))
        elif "\n" in value:  # Handle multi-line values like social media links
            value = ' | '.join(v.strip() for v in value.splitlines())
        data_dict[key] = value

    # Handle specific case for "Social Media Details" being part of another value
    if "網址" in data_dict and "Social Media Details" in data_dict["網址"]:
        value_text = data_dict.get("網址", 'N/A').split('Social Media Details')
        # Use regex to clean unwanted characters
        data_dict["網址"] = re.sub(pattern=r"^[|:]+|[|:]+$", repl="", string=value_text[0].strip())
        data_dict["Social Media Details"] = re.sub(pattern=r"^[|:]+|[|:]+$", repl="", string=value_text[1].strip())
    return data_dict


class TwsaOrgTaiwanSpider(scrapy.Spider):
    name = "twsa_org_taiwan"
    custom_settings = {
        'CONCURRENT_REQUESTS': 32,  # Increase concurrency
        'DOWNLOAD_DELAY': 0.1,  # Reduce delay if server can handle it
        'AUTOTHROTTLE_ENABLED': False,  # Disable throttling for speed
    }

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

        self.form_data = json.loads(open(file=r'../Form_Data_Files/form_data.json', mode='r', encoding='utf-8').read())
        self.form_data_detail_page = json.loads(open(file=r'../Form_Data_Files/form_data_detail.json', mode='r', encoding='utf-8').read())

    def start_requests(self) -> Iterable[Request]:
        cookies_form_data = {
            '__utmz': '177061027.1732859996.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
            'ASP.NET_SessionId': 'nyvyymgagbo2npxqugdy4tj5',
            '__utmc': '177061027',
            '__utma': '177061027.528475930.1732859996.1733813366.1733819802.14',
            '__utmb': '177061027.2.10.1733819802',
        }
        headers_form_data = {
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

        yield scrapy.Request(url="http://web.twsa.org.tw/alert/", method='POST', cookies=cookies_form_data, headers=headers_form_data,
                             callback=self.parse, dont_filter=True, errback=self.handle_error,
                             body=urlencode(self.form_data))  # Convert the form data dictionary to a URL-encoded string and Set it in the body

    def parse(self, response):
        # parsed_tree = fromstring(response.text)

        event_validation = response.xpath('//input[@id="__EVENTVALIDATION"]/@value').get()

        alerts_table = response.xpath('//table//a[contains(@id, "MainContent_ucAlertList_rptAlert_lbtnMore_")]/@href').getall()

        # # Parse event targets
        # event_targets = [re.search(pattern=r"'(.*)',", string=event_target_string).group(1)
        #                  for event_target_string in alerts_table if re.search(pattern=r"'(.*)',", string=event_target_string)]
        # print(len(event_targets))
        #
        # # cookies_detail_page = {
        # #     '__utmz': '177061027.1732859996.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
        # #     'ASP.NET_SessionId': 'qg30t25xqq30b4oghrpzithf',
        # #     '__utma': '177061027.528475930.1732859996.1733899070.1734324495.21',
        # #     '__utmc': '177061027',
        # #     '__utmt': '1',
        # #     '__utmb': '177061027.3.10.1734324495',
        # # }
        # # headers_detail_page = {
        # #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        # #     'Accept-Language': 'en-US,en;q=0.9',
        # #     'Cache-Control': 'max-age=0',
        # #     'Connection': 'keep-alive',
        # #     'Content-Type': 'application/x-www-form-urlencoded',
        # #     'Origin': 'http://web.twsa.org.tw',
        # #     'Referer': 'http://web.twsa.org.tw/alert/',
        # #     'Upgrade-Insecure-Requests': '1',
        # #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        # # }
        # #
        # # Split event targets into chunks of 10
        # # for event_target_batches in self.chunkify(event_targets, size=10):
        # #     for event_target in event_target_batches:
        # #         # Create a copy of form data and modify __EVENTTARGET
        # #         new_form_data = self.form_data_detail_page.copy()
        # #         new_form_data["__EVENTTARGET"] = event_target
        # #         new_form_data["__EVENTVALIDATION"] = event_validation
        # #         print(event_target)
        # #
        # #         # Send the request for each target in the batch
        # #         yield scrapy.Request(url="http://web.twsa.org.tw/alert/", method='POST', cookies=cookies_detail_page, headers=headers_detail_page,
        # #                              body=urlencode(new_form_data), callback=self.parse_detail_page, errback=self.handle_error, dont_filter=True)
        for event_target_string in alerts_table[:50]:
            match = re.search(pattern=r"'(.*)',", string=event_target_string)  # Find matches
            if match:  # Extract and print values if a match is found
                event_target = match.group(1)  # First value

                # Send Request on detail page
                new_form_data = self.form_data_detail_page.copy()
                # Changing event_target and view_state value in form-data to get desired response
                print('event_target:', event_target)
                new_form_data["__EVENTTARGET"] = event_target
                new_form_data["__EVENTVALIDATION"] = event_validation

                cookies_detail_page = {
                    '__utmz': '177061027.1732859996.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
                    'ASP.NET_SessionId': 'qg30t25xqq30b4oghrpzithf',
                    '__utma': '177061027.528475930.1732859996.1733899070.1734324495.21',
                    '__utmc': '177061027',
                    '__utmt': '1',
                    '__utmb': '177061027.3.10.1734324495',
                }
                headers_detail_page = {
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

                yield scrapy.Request(url="http://web.twsa.org.tw/alert/", method='POST', cookies=cookies_detail_page, headers=headers_detail_page,
                                     callback=self.parse_detail_page, dont_filter=True, errback=self.handle_error,
                                     body=urlencode(new_form_data))  # Set the URL-encoded form data in the body
            else:
                print("No match found.")

    def chunkify(self, iterable, size):
        """Split iterable into chunks of a given size."""
        for index in range(0, len(iterable), size):
            yield iterable[index:index + size]

    def parse_detail_page(self, response):
        fieldset_div = response.xpath('//div[@class="generalForm"]/fieldset[@class="register"]')[0]

        data_dict = dict()
        data_dict['url'] = 'http://web.twsa.org.tw/alert/'  # Keeping Static url because payload form data is changing instead of url for detail page
        # Extract the outer Div data
        outer_labels_list = [get_xpath_text(text_div) for text_div in fieldset_div.xpath('./p/label')]
        outer_values_list = [get_xpath_text(text_div) for text_div in fieldset_div.xpath('./p/label/following-sibling::font')]
        for outer_label, outer_value in zip(outer_labels_list, outer_values_list):
            data_dict[outer_label] = outer_value

        # Extract the inner Div data
        inner_labels = [get_xpath_text(text_div) for text_div in fieldset_div.xpath('./fieldset[@class="register"]/p/label[not(contains(@id,"Information"))]')]
        inner_values = [get_xpath_text(text_div) for text_div in fieldset_div.xpath('./fieldset[@class="register"]/p/label[not(contains(@id,"Information"))]/following-sibling::font')]
        for inner_label, inner_value in zip(inner_labels, inner_values):
            data_dict[inner_label] = inner_value

        # Extract the inner Div data from the `font` elements which are together in single string
        font_text_divs = fieldset_div.xpath('./fieldset[@class="register"]/p/label[contains(@id,"Information")]/following-sibling::font//text()').getall()

        # Join all lines into one string to handle multi-line values
        # font_texts_combined = ' '.join([text.strip() for text in font_text_divs if text.strip()])
        # Use regex to extract label-value pairs
        # Pattern Explanation:
        # - `([\w\s]+):` matches the label (text before the colon).
        # - `(.+?)(?= [\w\s]+:|$)` matches the value (text after the colon, stopping at the next label or end of string).
        # print('font_texts_combined', font_texts_combined)

        # # matches = re.findall(pattern=r'([\w\s]+):\s*(.+?)(?= [\w\s]+:|$)', string=font_texts_combined)
        # matches = re.findall(pattern=r'([\w\s()]+):\s*(.+?)(?= [\S]+:|$)', string=font_texts_combined)
        #
        # for label, values in matches:
        #     if '@' in values:
        #         value = ' | '.join([value.strip() for value in values.split(',')]).strip()
        #     elif ';' in values and '@' in values:
        #         value = ' | '.join([value.strip() for value in values.split(';')]).strip()
        #     else:
        #         value = values.strip()
        #     data_dict[label.strip()] = value  # Populate the dictionary with extracted data

        # data_dict = parse_text_to_dict(text=font_texts_combined, data_dict=data_dict)

        notice_information_combined = ' '.join([text.strip() for text in font_text_divs if text.strip()])
        data_dict['相關資訊(Notice information)'] = notice_information_combined

        print(data_dict)
        self.final_data_list.append(data_dict)

    def handle_error(self, failure):
        print('Handle error...', failure)
        self.logger.error(f"Request failed: {failure}")

    def close(self, reason):
        print('closing spider...')
        print("Converting List of Dictionaries into DataFrame, then into Excel file...")
        try:
            print("Creating Native sheet...")
            data_df = pd.DataFrame(self.final_data_list)
            # data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning
            data_df.insert(loc=0, column='id', value=range(1, len(data_df) + 1))  # Add 'id' column at position 1
            # data_df.set_index(keys='id', inplace=True)  # Set 'id' as index for the Excel output
            with pd.ExcelWriter(path=self.filename, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
                data_df.to_excel(excel_writer=writer, index=False)

            print("Native Excel file Successfully created.")
        except Exception as e:
            print('Error while Generating Native Excel file:', e)
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()
            print('VPN Connected!' if self.api.is_connected else 'VPN Disconnected!')

        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute(f'scrapy crawl {TwsaOrgTaiwanSpider.name}'.split())
