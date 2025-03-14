#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import platform
import sys

logger = logging.getLogger(__name__)


def set_memory_limit(limit_in_mib):
    current_platform = platform.system()
    if current_platform in ["Linux", "Darwin"]:  # Unix-like systems
        import resource
        limit_in_bytes = limit_in_mib * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (limit_in_bytes, limit_in_bytes))
        logger.info(f"Memory limit set to {limit_in_mib} MiB on {current_platform}.")
    elif current_platform == "Windows":
        try:
            import psutil
            process = psutil.Process(os.getpid())
            mem_usage_mib = process.memory_info().rss / (1024 * 1024)
            if mem_usage_mib > limit_in_mib:
                logger.info(f"Memory usage exceeded {limit_in_mib} MiB. Taking action.")
                sys.exit(1)
        except ImportError:
            logger.info("psutil module not installed. Cannot monitor memory usage on Windows.")
    else:
        logger.info(f"Memory limit setting is not supported on {current_platform}.")
