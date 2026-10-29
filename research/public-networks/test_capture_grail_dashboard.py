import json
import unittest
from capture_grail_dashboard import permitted_query


class GrailQueryTests(unittest.TestCase):
    def setUp(self):
        self.url = 'https://grail-grafana.tplr.ai/api/ds/query?ds_type=loki'
        self.body = {'from': '1000', 'to': '21601000', 'queries': [
            {'datasource': {'type': 'loki', 'uid': 'loki'}, 'expr': '{job="grail"}', 'queryType': 'range'}]}

    def test_only_bounded_official_loki_read_endpoint_allowed(self):
        self.assertTrue(permitted_query(self.url, json.dumps(self.body)))
        self.assertFalse(permitted_query(self.url.replace('/api/ds/query', '/api/dashboards/db'), json.dumps(self.body)))
        self.assertFalse(permitted_query(self.url.replace('grail-grafana.tplr.ai', 'example.org'), json.dumps(self.body)))
        self.body['to'] = '999999999999'
        self.assertFalse(permitted_query(self.url, json.dumps(self.body)))

    def test_rejects_other_datasource_and_malformed_body(self):
        self.body['queries'][0]['datasource']['type'] = 'postgres'
        self.assertFalse(permitted_query(self.url, json.dumps(self.body)))
        self.assertFalse(permitted_query(self.url, '{}'))
        self.assertFalse(permitted_query(self.url, 'bad json'))


if __name__ == '__main__':
    unittest.main()
