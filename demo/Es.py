import elasticsearch


class Es:
    index_name = ''
    es_obj = ''

    def __init__(self, index_name=''):
        if not index_name:
            return False
        self.index_name = index_name
        self.es_obj = self.connect_host()

    def connect_host(self):
        # hosts = [{'host': '47.52.255.132', 'port': 9201}]
        hosts = [{'host': '172.31.2.127', 'port': 9201}]
        es = elasticsearch.Elasticsearch(
            hosts
        )
        return es

    def query(self, scroll=None, search_type=None, page=0, size=10000):
        query_body = {'query': {'match_all': {}}, 'from':
            page, 'size': size}  # 查找所有文档
        result = self.es_obj.search(index=self.index_name, body=query_body, scroll=scroll, search_type=search_type)
        return result

    def scroll_query(self, scroll, scroll_id):
        result = self.es_obj.scroll(scroll_id=scroll_id, scroll=scroll)
        return result

    def count(self):
        count_data = self.es_obj.count(index=self.index_name)
        if not count_data:
            return False
        return count_data['count']
