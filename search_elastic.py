import json

def search_elastic(ix):
    from elasticsearch import Elasticsearch
    all_results = []

    es = Elasticsearch(['https://elastic:taiko7Ei@elasticsearch.blueteam.devwerx.org'])
    count_es = es.count(index=ix)

    # print("count: ", json.dumps(count_es, indent=4))

    n = count_es['count']

    if (n <= 10000):
        result = es.search(index=ix, size=n)
        return(result)

    scan_es = es.search(index=ix, size=10000, scroll="3m")

    # print("search: ", json.dumps(scan_es, indent=4))

    sid = scan_es['_scroll_id']
    all_results.append(scan_es)

    # print("all_results: ", json.dumps(all_results))

    while (n > 0):
        n = n - 10000
        result = es.scroll(scroll_id=sid, scroll="3m")

        # print("result: ", json.dumps(result))

        all_results.append(result)

    return(all_results)


# data = search_elastic('auditbeat-6.2.2-2018.02.27')
#
# print("data: ", json.dumps(data, indent=4))