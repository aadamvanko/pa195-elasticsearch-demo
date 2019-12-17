from elasticsearch import Elasticsearch


es = Elasticsearch([{"host": "localhost", "port": 9200}])
print(es)
quote = {"movie": "Heat", "year": 1995, "quote": "Told you I'm never going back..."}
print("quote =", quote)
print()


print("INSERT")
res = es.index(index="quotes", doc_type="quote", id=1234, body=quote)
print("res =", res)
print()


print("GET")
res = es.get(index="quotes", doc_type="quote", id=1234)
print("retrieved_quote = ", res["_source"])
print()


print("MODIFY")
modified_quote = res["_source"]
modified_quote["year"] = 2000
res = es.index(index="quotes", doc_type="quote", id=1234, body=modified_quote)
res = es.get(index="quotes", doc_type="quote", id=1234)
print("modified_quote =", res["_source"])
print()


print("DELETE")
res = es.delete(index="quotes", id=1234)
print("res =", res)
