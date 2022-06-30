import elasticsearch
import datetime


class Elastic:
    def __init__(self, uri):
        self.uri = uri

        self.today = datetime.date.today() - datetime.timedelta(days = 1)


    def get_data(self, query, index):
        es = elasticsearch.Elasticsearch(self.uri)

        if query is "*":
            pass
        else:
            query = f'"{query}"'

        data = es.search(index=index, body={
            'size': 10000,
            'sort': [
                {
                    'datetime': {
                        'order': 'desc'
                    }
                }
            ],
            'query': {
                'bool': {
                    'must': [
                        {
                            'query_string': {
                                'query': f'subject: {query}'
                            }

                        },
                        {
                            'range': {
                                'datetime': {
                                    'gte': f'{self.today}T00:00:01',
                                    'lt': f'{self.today}T23:59:59'
                                }
                            }
                        }
                    ]
                }

            },
            '_source': [
                'subject',
                'date',
                'attachments',
                'from',
                'to',
                'body',
                'snippet'
            ]
        })
        return [log for log in data['hits']['hits']]
