#!/bin/env python3

import requests
import html

GOOGLE_API_KEY = "AIzaSyButZDalB-o4K7kq6ufjQw_jgUtHL97EZo"


class GoogleKnowledgeGraph:
    cache = {}

    def __init__(self, useCache=True):
        self.useCache = useCache

    def search_by_id(self, id, lang='ja'):
        cacheKey = f'{id}:{lang}'

        if self.useCache and cacheKey in self.cache:
            return self.cache[cacheKey]

        d = requests.get('https://kgsearch.googleapis.com/v1/entities:search',
                         params={
                             'ids': id,
                             'languages': lang,
                             'limit': 1,
                             'key': GOOGLE_API_KEY
                         }).json()

        node = d['itemListElement'][0]['result'] if (
            'itemListElement' in d and len(d['itemListElement']) > 0) else None

        if node is not None and 'name' in node:
            node['name'] = html.unescape(node['name'])

        if self.useCache:
            self.cache[cacheKey] = node

        return node
