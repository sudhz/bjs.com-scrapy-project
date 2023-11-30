import scrapy
import re
import json
import socket
import struct
import random
import requests
from w3lib.html import remove_tags

class ProductsSpider(scrapy.spiders.SitemapSpider):
    name = "products"
    sitemap_urls = ["https://www.bjs.com/sitemap_products_1.xml"]
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Referer': 'https://www.bjs.com/',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
    }

    def __init__(self, *args, **kwargs):
        super(ProductsSpider, self).__init__(*args, **kwargs)
        self.valid_image_urls = []

    def parse(self, response):
        product_info = self.extract_product_info(response)
        if product_info:
            api_url = self.build_api_url(product_info['productid'])
            yield scrapy.Request(api_url, callback=self.parse_api, headers=self.headers, meta=product_info)

    def get_attribute_value(self, specs, keyword):
        obj = next((attr for attr in specs.get('productDetailsData', {}).get('descriptiveAttributes', [])
                    if re.search(keyword, attr.get('name'), re.IGNORECASE)), {})
        return obj['attributeValueDataBeans'][0]['value'] if 'attributeValueDataBeans' in obj else None

    def check_image_url(self, url, sku):
        try:
            response = requests.head(url, allow_redirects=True)
            if response.status_code == 200:
                return url
        except requests.RequestException:
            pass
        return None

    def get_valid_image_urls(self, sku):
        base_url_template = "https://bjs.scene7.com/is/image/bjs/{}{}?$bjs-Zoom$"
        valid_urls = []

        main_image = self.check_image_url(
            base_url_template.format(sku, ""), sku)
        if main_image:
            valid_urls.append(main_image)

        for alt_num in range(1, 9):
            alt_url = self.check_image_url(
                base_url_template.format(sku, f"__alt{alt_num}"), sku)
            if alt_url:
                valid_urls.append(alt_url)

        return valid_urls

    def extract_product_info(self, response):
        sku = re.findall(
            '\d+', response.css('[auto-data="product_ItemId"]').get())
        title = response.css('[auto-data="product_name"]::text').get().strip(
            '"') if response.css('[auto-data="product_name"]::text').get() else None
        breadcrumbs = response.css(
            '[auto-data^="product_bread_crumbL"]::text').getall()
        category = breadcrumbs[-1] if breadcrumbs else ''
        url = response.url
        productid = url.split('/')[-1]
        pdpData = json.loads(response.css('#pdp-data script::text').get()[31:])
        upc_object = next((attr for attr in pdpData.get('productDetailsData', {}).get(
            'descriptiveAttributes', []) if attr.get('name') == 'upc'), {})
        upc = upc_object['attributeValueDataBeans'][0]['value'] if 'attributeValueDataBeans' in upc_object else None
        productData = json.loads(response.css(
            'script[data-rh="true"][type="application/ld+json"]::text').extract_first())
        mpn = productData.get("mpn")
        brand = productData.get("brand", {}).get("name")
        description = remove_tags(productData["description"])
        specs = json.loads(response.css(
            'div#pdp-data script::text').get()[31:])
        model_obj = next((attr for attr in specs.get('productDetailsData', {}).get(
            'descriptiveAttributes', []) if re.search(r'model', attr.get('name'), re.IGNORECASE)), {})
        model = model_obj['attributeValueDataBeans'][0]['value'] if 'attributeValueDataBeans' in model_obj else None
        weight_obj = next((attr for attr in specs.get('productDetailsData', {}).get(
            'descriptiveAttributes', []) if re.search(r'weight', attr.get('name'), re.IGNORECASE)), {})
        weight = weight_obj['attributeValueDataBeans'][0]['value'] if 'attributeValueDataBeans' in weight_obj else None
        dimensions_obj = next((attr for attr in specs.get('productDetailsData', {}).get(
            'descriptiveAttributes', []) if re.search(r'dimensions', attr.get('name'), re.IGNORECASE)), {})
        dimensions = dimensions_obj['attributeValueDataBeans'][0][
            'value'] if 'attributeValueDataBeans' in dimensions_obj else None
        size_obj = next((attr for attr in specs.get('productDetailsData', {}).get(
            'descriptiveAttributes', []) if re.search(r'size', attr.get('name'), re.IGNORECASE)), {})
        size = size_obj['attributeValueDataBeans'][0]['value'] if 'attributeValueDataBeans' in size_obj else None
        color_obj = next((attr for attr in specs.get('productDetailsData', {}).get(
            'descriptiveAttributes', []) if re.search(r'colo[u]?r', attr.get('name'), re.IGNORECASE)), {})
        color = color_obj['attributeValueDataBeans'][0]['value'] if 'attributeValueDataBeans' in color_obj else None
        package_dimensions_obj = next((attr for attr in specs.get('productDetailsData', {}).get(
            'descriptiveAttributes', []) if re.search(r'package dimensions', attr.get('name'), re.IGNORECASE)), {})
        package_dimensions = package_dimensions_obj['attributeValueDataBeans'][0][
            'value'] if 'attributeValueDataBeans' in package_dimensions_obj else dimensions
        shipping_weight_obj = next((attr for attr in specs.get('productDetailsData', {}).get(
            'descriptiveAttributes', []) if re.search(r'shipping weight', attr.get('name'), re.IGNORECASE)), {})
        shipping_weight = shipping_weight_obj['attributeValueDataBeans'][0][
            'value'] if 'attributeValueDataBeans' in shipping_weight_obj else weight

        # Get valid image URLs and store them in self.valid_image_urls
        self.valid_image_urls = self.get_valid_image_urls(sku[0])

        return {
            'SKU': sku,
            'Title': title,
            'Category': category,
            'Brand': brand,
            'Color': color,
            'Model Number': model,
            'Size': size,
            'UPC': upc,
            'Link': url,
            'Description': description,
            'MPN': mpn,
            'Product Dimensions': dimensions,
            'Item Weight': weight,
            'Package Dimensions': package_dimensions,
            'Shipping Weight': shipping_weight,
            'Product Images': self.valid_image_urls,
            'productid': productid,
        }

    def build_api_url(self, productid):
        return f'https://api.bjs.com/digital/live/api/v1.0/product/price/10201?productId={productid}&pageName=PDP'

    def parse_api(self, response):
        data = json.loads(response.body)
        unique_prices = set()
        self.extract_prices(data, unique_prices)
        inventory_data = self.get_inventory(response.meta['SKU'])
        inventory_status = inventory_data['Body']['ShowInventoryAvailability'][
            'DataArea']['InventoryAvailability']['InventoryStatus']
        available_quantity = inventory_data['Body']['ShowInventoryAvailability'][
            'DataArea']['InventoryAvailability']['AvailableQuantity']
        yield {**response.meta, 'Price': list(unique_prices), 'In Stock': inventory_status, 'Available Quantity': available_quantity, 'Condition': '', 'Bullet Points': ''}

    def extract_prices(self, json_obj, unique_prices):
        if isinstance(json_obj, dict):
            for key, value in json_obj.items():
                if key.lower() in ["amount", "price"] and isinstance(value, (int, float)):
                    unique_prices.add(value)
                else:
                    self.extract_prices(value, unique_prices)
        elif isinstance(json_obj, list):
            for item in json_obj:
                self.extract_prices(item, unique_prices)

    def get_inventory(self, sku):
        url = f"https://api.bjs.com/digital/live/api/v1.2/inventory/club"
        request_body = {
            "InventoryMicroServiceEnableSwitch": "ON",
            "Body": {
                "GetInventoryAvailability": {
                    "ApplicationArea": {
                        "BusinessContext": {
                            "ContextData": []
                        }
                    },
                    "PartNumber": sku[0],
                    "uom": "C62"
                }
            }
        }
        headrs = {
            'x-forwarded-host': socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))}
        try:
            response = requests.post(url, headers=headrs, json=request_body)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(f"HTTP Error: {err}")
            print(f"Response Content: {response.content}")

        return json.loads(response.content)
