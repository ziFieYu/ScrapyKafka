# -*- coding: utf-8 -*-
from scrapy import Request
from scrapy_kafka.spiders import ListeningKafkaSpider


class ExampleSpider(ListeningKafkaSpider):
    name = "example"

    def make_requests_from_url(self, url):
        if '://' in url:
            return Request(url, callback=self.parse, dont_filter=True)
        else:
            self.logger.error("Unexpected URL from '%s': %r", self.server.topic.name, url)

    def parse(self, response):
        self.logger.debug(response.body)
