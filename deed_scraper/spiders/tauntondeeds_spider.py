import scrapy
import scrapy_splash
import json
import datetime

from deed_scraper.spiders.base_spider import BaseSpider


class DeedsSpider(BaseSpider):
    name = "deeds"
    start_urls = ['http://www.tauntondeeds.com/Searches/ImageSearch.aspx']

    def __init__(self, *args, **kwargs):
        self.start_date = '2020-01-01-00-00-00'
        self.end_date = '2020-12-31-00-00-00'
        self.doc_type = 'DEED'

    def parse(self, response):
        yield scrapy.Request(
            'http://www.tauntondeeds.com/Searches/ImageSearch.aspx',
            callback=self.parse_image_search
        )

    def parse_image_search(self, response):
        form_data = self._get_form_data(response)
        form_data['__EVENTTARGET'] = 'ctl00$cphMainContent$btnSearchRL'

        yield scrapy.FormRequest(
            'http://www.tauntondeeds.com/Searches/ImageSearch.aspx',
            formdata=form_data,
            callback=self.parse_deeds
        )

    def parse_deeds(self, response):
        deeds_table_path = "//table[@id='ctl00_cphMainContent_gvSearchResults']"
        deeds = response.xpath(f"{deeds_table_path}/tr[contains(@class, 'Row')]")
        pager = response.xpath(f"{deeds_table_path}/tr[@class='gridPager']")[0]

        for deed in deeds:
            parsed_document = self._parse_deed(deed)
            self._write_to_file(parsed_document)

        if self._is_last_page(pager):
            return

        page_num = int(pager.xpath(".//span/text()").get())
        next_page = page_num + 1

        form_data = self._get_form_data(response)
        form_data['__EVENTTARGET'] = 'ctl00$cphMainContent$gvSearchResults'
        form_data['__EVENTARGUMENT'] = f'Page${next_page}'

        yield scrapy.FormRequest(
            'http://www.tauntondeeds.com/Searches/ImageSearch.aspx',
            formdata=form_data,
            callback=self.parse_deeds
        )

    def _is_last_page(self, pager):
        if pager.xpath(".//span/../following-sibling::td"):
            return False
        return True

    def _get_form_data(self, response):
        result = {}
        form_data = response.xpath("//input[contains(@id, 'ctl00_')]")
        template = "//input[@id='{}']/@value"
        viewstate = response.xpath(template.format('__VIEWSTATE')).get()
        viewstategenereator = response.xpath(template.format('__VIEWSTATEGENERATOR')).get()
        doc_type_id = response.xpath(f"//option[text()='{self.doc_type}']/@value").get()

        result['__VIEWSTATE'] = viewstate
        result['__VIEWSTATEGENERATOR'] = viewstategenereator
        result['ctl00$cphMainContent$ddlRLDocumentType$vddlDropDown'] = doc_type_id
        result['ctl00$cphMainContent$txtRLStartDate$dateInput'] = self.start_date
        result['ctl00$cphMainContent$txtRLEndDate$dateInput'] = self.end_date
        return result

    def _parse_street(self, description):
        description = description.rsplit('$', 1)[0].strip(' ,')
        rev_description = list(reversed(description.split()))
        for index, word in enumerate(rev_description):
            if any(ch.isdigit() for ch in word):
                break
        else:
            if description.startswith('SEE DEED'):
                return description[8:].strip()
            return
        if not '-' in word:
            index += 1
        address = list(reversed(rev_description[:index]))
        for index, word in enumerate(address):
            if '(' in word:
                del(address[index])
                break
        address = ' '.join(address)
        return address

    def _parse_cost(self, description):
        try:
            info, cost = description.rsplit(',', 1)
        except(ValueError):
            return
        try:
            cost = float(cost.lstrip(' $'))
        except(ValueError):
            return
        return cost

    def _parse_description(self, description):
        cost = self._parse_cost(description)
        street_address = self._parse_street(description)
        return street_address, cost

    def _parse_deed(self, deed):
        all_fields = deed.xpath("./td/text()").extract()
        searched_fields = all_fields[1:-2]
        description = deed.xpath(f".//span/text()").get()
        address, cost = self._parse_description(description)

        parsed_document = {
            'date': searched_fields[0],
            'type': searched_fields[1],
            'book': searched_fields[2],
            'page_num': searched_fields[3],
            'doc_num': searched_fields[4],
            'city': searched_fields[5],
            'desription': description,
            'cost': cost,
            'street_address': address,
            'state': None,
            'zip': None,
        }
        return parsed_document

    def _write_to_file(self, parsed_document):
        with open('results.json', 'a') as f:
                json.dump(parsed_document, f)
                f.write(',\n')
