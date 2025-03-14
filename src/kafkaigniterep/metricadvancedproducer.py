#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import configparser
import os
import logging
from logging.handlers import TimedRotatingFileHandler
import random
import time
from typing import Dict, Tuple

from faker import Faker
from faker.providers import BaseProvider


def configure_logging(log_file: str, retention_days: int, debug: bool) -> None:
    log_level = logging.DEBUG if debug else logging.INFO

    handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=retention_days, encoding="utf-8"
    )
    handler.suffix = "%Y-%m-%d"

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[handler, logging.StreamHandler()]
    )


logger = logging.getLogger(__name__)


class MetricAdvancedProvider(BaseProvider):
    def __init__(self, host: int, cpu: int):
        super().__init__(Faker())
        self.host = host
        self.cpu = cpu

    def hostname(self) -> str:
        return f"hostname{random.randint(0, self.host)}"

    def cpu_id(self) -> str:
        return f"cpu{random.randint(0, self.cpu)}"

    @staticmethod
    def usage() -> float:
        return round(random.uniform(70, 100), 2)

    def produce_msg(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        try:
            hostname = self.hostname()
            ts = int(time.time() * 1000)
            message = {
                "hostname": hostname,
                "cpu": self.cpu_id(),
                "usage": self.usage(),
                "occurred_at": ts,
            }
            key = {"hostname": hostname}
            logger.info(f"Generated Message: {message}")
            return message, key
        except Exception as e:
            logger.error(f"Error generating message: {e}")
            return {}, {}


def read_server_config(config_file: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    try:
        cfg.read(config_file)
        defaults = {
            'user': 'KAFKA_USER',
            'password': 'KAFKA_PASSWORD',
            'cafile': 'KAFKA_CA_CRT',
            'bootstrap_servers': 'localhost:9092',
            'ssl_check_hostname': 'true',
            'topic': 'debug-tool-01',
            'topic_replication_factor': '1',
            'topic_retention_ms': '15552000000',
            'flush_timeout_s': '60.0',
        }

        if 'kafka' not in cfg:
            cfg['kafka'] = {}

        for key, env_var in defaults.items():
            cfg['kafka'].setdefault(key, str(os.environ.get(env_var, defaults[key])))
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        raise
    return cfg


def main() -> None:
    args = parse_command_line()
    configure_logging(args.log_file, args.log_retention_days, args.debug)
    cfg = read_server_config(args.config_file)
    metric_advanced_provider = MetricAdvancedProvider(args.host, args.cpu)

    while True:
        try:
            message, key = metric_advanced_provider.produce_msg()
            if message:
                logger.info(f"Successfully generated: {message}")
            time.sleep(random.uniform(1, 5))
        except Exception as e:
            logger.critical(f"Critical failure in message generation loop: {e}")
            time.sleep(5)


def parse_command_line() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='metric_advanced_provider is a Python library that generates '
                                                 'synthetic host and CPU metrics with configurable logging and '
                                                 'message production for testing and debugging.')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--kafka-debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--log-file', default=r'D:\kafkaignite\logs\metric_advanced_provider.log', help='Log file')
    parser.add_argument('--log-retention-days', default=7, type=int, help='Log Retention Days')
    parser.add_argument('--config-file', default='metric_advanced_provider.conf', help='Configuration file')
    parser.add_argument('--host', default=100_000, type=int, help='Number of hosts')
    parser.add_argument('--cpu', default=30, type=int, help='Number of CPUs')
    return parser.parse_args()


if __name__ == "__main__":
    main()
