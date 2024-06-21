import scrapy
from scrapy.http import Headers, Request, Response
from scrapy_impersonate.parser import RequestParser
import re
import json
import os
import csv
from lxml import etree
from lxml import html
import pandas as pd
from urllib.parse import urlencode


class ScrapeSpider(scrapy.Spider):
    name = "scrape"

    start_urls = ["https://suchen.mobile.de/fahrzeuge/search.html?dam=false&fe=NONSMOKER_VEHICLE&isSearchRequest=true&pageNumber=1&q=autohero&ref=srpNextPage&refId=e6900f52-7104-9d58-6064-18d4a6be2364&s=Car&sb=rel&vc=Car&lang=en"]
    current_page_number = 0
    custom_headers = {
        "x-oxylabs-user-agent-type": "desktop_chrome",
        "x-oxylabs-geo-location": "Germany",
        "X-Oxylabs-Render": "html",
    }

    def __init__(self):
        self.current_page_number = 1

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, headers=self.custom_headers,meta={'proxy': 'http://tahira_75Vwy_75Vwy:34mo1TJfY8onP_@unblock.oxylabs.io:60000'})


    def parse(self, response):

        main_cards = response.xpath('//div[starts-with (@class, "mN_WC")]')
        for main_card in main_cards:
            brand = main_card.xpath('.//h2[@class="QeGRL"]/text()').get()
            seller = main_card.xpath('.//div[@data-testid="seller-info"]/div/div/span[1]/text()').get()
            price  = main_card.xpath('.//span[@data-testid="price-label"]/text()').get()
            price_fairness  = main_card.xpath('.//div[@class="_u77E bzOeV"]/text()').get()
            # fr = main_card.xpath('//div[contains(text(),"FR")]/text()').get()
            review = main_card.xpath('.//span[@class="W9v_K"]/text()').getall()
            if review:
                joined_review = ''.join(review)
                numeric_value_str = re.search(r'\d+', joined_review).group()
                numeric_value = int(numeric_value_str)
            else:
                numeric_value = ''

            km_list = main_card.xpath('.//div[@data-testid="listing-details-attributes"]/div[1]/text()').getall()
            split_list = [item.strip() for item in ''.join(km_list).split(' • ') if item.strip()]
            cleaned_list = [item.replace('\xa0', ' ') for item in split_list]
            if len(cleaned_list) == 3:
                fr_date = cleaned_list[0]
                km_value = cleaned_list[1]

            url = main_card.xpath('.//a[starts-with (@data-testid, "result-listing")]/@href').get()
            abolute_url = response.urljoin(url)
            data = {
                'Brand':brand,
                'Seller': seller,
                'Price': price,
                'Price Fairness Score': price_fairness,
                'First Resgistration': fr_date,
                'KM': km_value,
                'Dealler Review Score': numeric_value,
                'Listing URL': abolute_url
            }
            self.write_to_xlsx(data)
            yield data


        self.current_page_number += 1
        next_page_url = f"https://suchen.mobile.de/fahrzeuge/search.html?dam=false&fe=NONSMOKER_VEHICLE&isSearchRequest=true&pageNumber={self.current_page_number}&q=autohero&ref=srpNextPage&refId=e6900f52-7104-9d58-6064-18d4a6be2364&s=Car&sb=rel&vc=Car&lang=en"
        print("Next Pagination:", next_page_url)
        if not main_cards:
            self.log("No more links found, stopping pagination.")
            return

        yield scrapy.Request(
            next_page_url,
            callback=self.parse,
            headers=self.custom_headers,
            meta={'proxy': 'http://tahira_75Vwy_75Vwy:34mo1TJfY8onP_@unblock.oxylabs.io:60000'},
        )


    def write_to_xlsx(self, data):
        file_path = 'suchen_mobile.xlsx'
        file_exists = os.path.isfile(file_path)
        df = pd.DataFrame([data])
        if file_exists:
            existing_df = pd.read_excel(file_path)
            new_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            new_df = df
        new_df.to_excel(file_path, index=False)

    # def write_to_csv(self, data):
    #     file_path = 'suchen_mobile.csv'
    #     file_exists = os.path.isfile(file_path)
    #
    #     # Column names for the CSV file
    #     fieldnames = ['Brand', 'Seller', 'Price', 'Price Fairness Score', 'First Registration', 'KM',
    #                   'Dealer Review Score']
    #
    #     if file_exists:
    #         # If file exists, append the data
    #         with open(file_path, mode='a', newline='', encoding='utf-8-sig') as file:
    #             writer = csv.DictWriter(file, fieldnames=fieldnames)
    #             writer.writerow(data)
    #     else:
    #         # If file does not exist, write header and data
    #         with open(file_path, mode='w', newline='', encoding='utf-8-sig') as file:
    #             writer = csv.DictWriter(file, fieldnames=fieldnames)
    #             writer.writeheader()
    #             writer.writerow(data)
    #
    #         # Optionally, if you want to use pandas to read back the data
    #     df = pd.read_csv(file_path)



        # all_links = response.xpath('//a[starts-with (@data-testid, "result-listing")]/@href').getall()
        # for link in all_links[:1]:
        #     absolute_url = response.urljoin(link)
        #     yield scrapy.Request(
        #         url=absolute_url,
        #         callback=self.main_links,
        #         meta={'url': absolute_url, 'proxy': 'http://tahira_75Vwy_75Vwy:34mo1TJfY8onP_@unblock.oxylabs.io:60000'},
        #         dont_filter=True
        #     )

        # self.current_page_number += 1
        # next_page_url = f"https://suchen.mobile.de/fahrzeuge/search.html?dam=false&fe=NONSMOKER_VEHICLE&isSearchRequest=true&pageNumber={self.current_page_number}&q=autohero&ref=srpNextPage&refId=e6900f52-7104-9d58-6064-18d4a6be2364&s=Car&sb=rel&vc=Car&lang=en"
        #
        # # Check if next_page_url is None (or if there are no more pages)
        # if not all_links:
        #     self.log("No more links found, stopping pagination.")
        #     return
        # yield scrapy.Request(
        #     next_page_url,
        #     callback=self.parse,
        #     meta={'proxy': 'http://tahira_75Vwy_75Vwy:34mo1TJfY8onP_@unblock.oxylabs.io:60000'},
        #     dont_filter=True
        # )


    # def main_links(self, response):
    #     main_url = response.meta['url']
    #
    #     auto = response.xpath('//h4[@class="h3 seller-title__inner"]/a/text()').get()
    #     brand = response.xpath('//h1[@id="ad-title"]/text()').get()
    #     price = response.xpath('//div[@class="mde-price-rating__badge__label"]/text()').get()
    #     registered = response.xpath('//*[contains(text(),"First Registration")]/following-sibling::*/text()').get()
    #     milage = response.xpath('//*[contains(text(),"Mileage")]/following-sibling::*/text()').get()
    #
    #     prices = response.xpath('//div[@class="main-price-and-rating-row"]/span[@class="h3"]/text()').get()
    #
    #     yield {
    #         'Dealer': auto,
    #         'Brand':brand,
    #         'Price fairness score':price,
    #         'Year first registered':registered,
    #         'kilometers':milage,
    #         'Price':prices,
    #         'main_url':main_url,
    #     }
