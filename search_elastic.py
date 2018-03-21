
def search_elastic(ix):
    from datetime import datetime
    from elasticsearch import Elasticsearch
    import json

    es = Elasticsearch(['https://elastic:taiko7Ei@elasticsearch.blueteam.devwerx.org'])
    result = es.search(index=ix, size=30000)
    # print(json.dumps(result, indent=4))
    return(result)
