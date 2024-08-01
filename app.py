from flask import Flask, request, render_template
import logging
import os
import time

from scraper import scrape_web
from elastic import create_elastic_client, create_index_if_not_exists, index_products, search_products
from models import Product

app = Flask(__name__)

indexname = "indext23"
flagname = "flags/indexing_done_73.flag"

@app.route('/home')
def home():
    return render_template('index.html')

@app.route('/products')
def products():
    return render_template('products.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/search', methods=['POST'])
def search():
    search_text = request.form['search']
    logger = logging.getLogger("ProductSearch")
    client = create_elastic_client()
    search_results, price_ranges = search_products(client, search_text, logger)
    return render_template('results.html', search_text=search_text, results=search_results, price_ranges=price_ranges)

def main():
    start_time1 = time.time()
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
    search_duration = time.time() - start_time1

if __name__ == "__main__":
    main()
    app.run(debug=True)
