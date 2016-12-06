# -*- coding: utf-8 -*-
from pykafka import KafkaClient
from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider


class KafkaSpiderMixin(object):
    """
    Mixin class to implement reading urls from a kafka queue.
    """

    server = None

    def start_requests(self):
        """Returns a batch of start requests from redis."""
        return self.next_requests()

    def setup_kafka(self, crawler=None):
        """Setup redis connection and idle signal.

        This should be called after the spider has set its crawler object.
        """
        if self.server is not None:
            return
        if crawler is None:
            # We allow optional crawler argument to keep backwards compatibility.
            # XXX: Raise a deprecation warning.
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        zookeeper_connect = crawler.settings.get('SCRAPY_ZOOKEEPER_CONNECT', 'localhost:2181')
        hosts = crawler.settings.get('SCRAPY_KAFKA_HOSTS', 'localhost:9092')
        consumer_group = crawler.settings.get('SCRAPY_KAFKA_SPIDER_CONSUMER_GROUP', 'scrapy-kafka')
        kafka_topic = crawler.settings.get('SCRAPY_KAFKA_TOPIC', '%s-starturls' % self.name)

        _kafka = KafkaClient(hosts=hosts)
        topic = _kafka.topics[kafka_topic]
        self.server = topic.get_balanced_consumer(
            consumer_group=consumer_group,
            auto_commit_enable=True,
            zookeeper_connect=zookeeper_connect
        )
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def next_requests(self):
        """
        Returns a request to be scheduled.
        :rtype: str or None
        """
        data = self.server.consume().value
        # if '://' in data:
        #
        # else:
        #     self.logger.debug("Unexpected URL from data: %r", data)
        req = self.make_requests_from_url(data)
        if req:
            yield req
        else:
            self.logger.debug("Request not made from data: %r", data)

    def schedule_next_request(self):
        """Schedules a request if available"""
        for req in self.next_requests():
            self.crawler.engine.crawl(request=req, spider=self)

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        self.schedule_next_request()
        raise DontCloseSpider


class ListeningKafkaSpider(KafkaSpiderMixin, Spider):
    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(ListeningKafkaSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_kafka(crawler)
        return obj
