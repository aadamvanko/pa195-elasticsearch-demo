from elasticsearch import Elasticsearch


def load_quotes(filepath):
    with open(filepath) as file:
        quotes = []
        for line in file:
            movie, year, quote = line.strip().split(";")
            quotes.append({"movie": movie, "year": int(year), "quote": quote})
        return quotes


es = Elasticsearch([{"host": "localhost", "port": 9200}])
print(es)


# prepare and insert
quotes = load_quotes("quotes.txt")
for index, quote in enumerate(quotes):
    print(quote)
    es.index(index="quotes", doc_type="quote", id=index, body=quote)
print()


# search
print("MATCH ALL")
doc = {"size": 10000, "query": {"match_all": {}}}
res = es.search(index="quotes", body=doc)
for hit in res["hits"]["hits"]:
    print(hit["_source"])
print()


print("MOVIES BEFORE 2000")
doc = {"size": 10000, "query": {"bool": {"filter": {"range": {"year": {"lt": 2000}}}}}}
res = es.search(index="quotes", body=doc)
for hit in res["hits"]["hits"]:
    print(hit["_source"])
print()


print("QUOTES CONTAINING and")
doc = {"size": 10000, "query": {"wildcard": {"quote": {"value": "and"}}}}
res = es.search(index="quotes", body=doc)
for hit in res["hits"]["hits"]:
    print(hit["_source"])
