#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=W0102,E0712,C0103,R0903

"""World Radio Prefixes - RCLDX companion software"""

__updated__ = "2025-12-08 13:53:33"

import os
from dotenv import load_dotenv, find_dotenv


# Load .env if present (ideal for local development).
# In containers, if .env is missing, environment variables are used as-is.
load_dotenv(find_dotenv())


def str_to_bool(value: str | None, default: bool = True) -> bool:
    """
    Convert strings like "true"/"false"/"1"/"0" to boolean values.
    If value is None, return the provided default.
    """
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "y", "t")


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
        # --- Execution mode ---
        # - api: run the Flask API
        # - worker: run the worker
        "APP_TYPE": os.getenv("APP_TYPE", "none"),
    }
