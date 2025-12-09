#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=W0102,E0712,C0103,R0903

"""World Radio Prefixes - RCLDX companion software"""

__updated__ = "2025-12-09 01:42:57"

import os
from dotenv import load_dotenv, find_dotenv


# Load .env if present (ideal for local development).
# In containers, if .env is missing, environment variables are used as-is.
load_dotenv(find_dotenv())


def get_config() -> dict:
    """
    Return the 12-factor configuration as a simple dict.
    Valid for both API and worker modes.
    """

    # --- Service environment ---
    # local, dev, test, staging, prod, ...
    env = os.getenv("SERVICE_ENV", "local")

    # Default log level per environment
    if env in ("local", "dev"):
        default_log_level = "DEBUG"
    else:
        default_log_level = "INFO"

    return {
        "REDIS_HOST": os.getenv("REDIS_HOST", "localhost"),
        "REDIS_PORT": os.getenv("REDIS_PORT", "6379"),
        "REDIS_DB": os.getenv("REDIS_DB", "0"),
        "REDIS_PASSWORD": os.getenv("REDIS_PASSWORD", "none"),
        "REDIS_MAX_CONN": os.getenv("REDIS_MAX_CONN", "25"),
    }
