from flask import Flask, request, render_template
from elasticsearch import Elasticsearch, helpers
import logging
import requests
from bs4 import BeautifulSoup
import os
import time

app = Flask(__name__)

indexname = "indext18"
flagname = "flags/indexing_done_68.flag"

class Product:
    def __init__(self, product_name=None, prices=None, rating_count=None, attributes=None):
        self.product_name = product_name
        self.prices = prices or []
        self.rating_count = rating_count or []
        self.attributes = attributes or {}

def create_elastic_client():
    return Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

def scrape_web():
    url = "https://www.trendyol.com/sr/oyuncu-mouselari-x-c106088?sst=BEST_SELLER"
    response = requests.get(url)
    products = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        product_nodes = soup.select('div.p-card-chldrn-cntnr.card-border')

        for node in product_nodes:
            product_name_node = node.select_one("h3.prdct-desc-cntnr-ttl-w")
            price_node = node.select_one("div.prc-box-dscntd")
            rating_count_node = node.select_one("span.ratingCount")
            product_link_node = node.select_one("a")

            product_name = (
                " ".join([
                    product_name_node.select_one("span.prdct-desc-cntnr-ttl").get_text().strip() if product_name_node.select_one("span.prdct-desc-cntnr-ttl") else "",
                    product_name_node.select_one("span.prdct-desc-cntnr-name").get_text().strip() if product_name_node.select_one("span.prdct-desc-cntnr-name") else "",
                    product_name_node.select_one("div.product-desc-sub-text").get_text().strip() if product_name_node.select_one("div.product-desc-sub-text") else ""
                ])
                if product_name_node else None
            )
            price = price_node.get_text().strip() if price_node else None
            if price:
                price = float(price.replace("TL", "").replace(",", "").replace(".", ""))
            rating_count = rating_count_node.get_text().strip() if rating_count_node else None
            product_link = f"https://www.trendyol.com{product_link_node['href']}" if product_link_node else None

            attributes = scrape_product_details(product_link) if product_link else {}

            product = Product(
                product_name=product_name,
                prices=[price] if price else [],
                rating_count=rating_count,
                attributes=attributes
            )
            products.append(product)

        return products, soup

    return products, None

def scrape_product_details(url):
    response = requests.get(url)
    attributes = {}

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        attribute_mappings = {
            'Mouse Hassasiyeti (Dpi)': 'dpi',
            'RGB Aydınlatma': 'rgb_lighting',
            'Mouse Tipi': 'mouse_type',
            'Buton Sayısı': 'button_count'
        }

        for attr_name, key in attribute_mappings.items():
            attr_node = soup.select_one(f'span[title="{attr_name}"] + span.attribute-value > div.attr-name.attr-name-w')
            attributes[key] = attr_node.get_text().strip() if attr_node else None

    return attributes

def index_products(client, products, logger):
    actions = [
        {
            "_index": indexname,
            "_source": {
                "product_name": product.product_name,
                "prices": product.prices,
                "rating_count": product.rating_count,
                "attributes": product.attributes
            }
        }
        for product in products
    ]

    helpers.bulk(client, actions)

def create_index_if_not_exists(client, logger):
    if not client.indices.exists(index=indexname):
        client.indices.create(index=indexname, body={
            "mappings": {
                "properties": {
                    "product_name": {"type": "text"},
                    "prices": {"type": "float"},
                    "rating_count": {"type": "keyword"},
                    "attributes": {"type": "object", "enabled": False}
                }
            }
        })

def search_products(client, search_text, logger):
    search_response = client.search(
        index=indexname,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": search_text,
                                "fields": ["product_name^3", "rating_count"],
                                "fuzziness": "AUTO"
                            }
                        },
                        {
                            "range": {
                                "prices": {
                                    "gte": 0
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "price_ranges": {
                    "range": {
                        "field": "prices",
                        "ranges": [
                            {"to": 50},
                            {"from": 50, "to": 1000},
                            {"from": 1000}
                        ]
                    }
                }
            }
        }
    )

    results = search_response['hits']['hits']
    search_results = []
    for result in results:
        product = result["_source"]
        search_results.append({
            "product_name": product['product_name'],
            "prices": product.get('prices', []),
            "rating_count": product.get('rating_count', 'N/A'),
            "attributes": product.get('attributes', {})
        })

    return search_results, search_response['aggregations']['price_ranges']['buckets']

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/search', methods=['POST'])
def search():
    search_text = request.form['search']
    logger = logging.getLogger("ProductSearch")
    client = create_elastic_client()
    search_results, price_ranges = search_products(client, search_text, logger)
    return render_template('results.html', search_text=search_text, results=search_results, price_ranges=price_ranges)

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("ProductScraper")

    client = create_elastic_client()
    create_index_if_not_exists(client, logger)

    products, _ = scrape_web()

    if not os.path.exists(flagname):
        index_products(client, products, logger)
        os.makedirs(os.path.dirname(flagname), exist_ok=True)
        with open(flagname, 'w') as flag_file:
            flag_file.write('')

if __name__ == "__main__":
    main()
    app.run(debug=True)
