from elasticsearch import Elasticsearch, helpers

indexname = "indext22"

def create_elastic_client():
    return Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

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
                            {"to": 500},
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
