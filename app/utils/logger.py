# app/utils/logger.py

import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(log_file='data/logs/app.log'):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger('crypto_index_logger')
    logger.setLevel(logging.INFO)

    if not logger.handlers:  # Prevent adding handlers multiple times
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

         # ==== Rotating File handler ====
        file_handler = RotatingFileHandler(
            log_file, maxBytes=5*1024*1024, backupCount=3
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # ==== Console handler (optional, but nice during dev) ====
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

#Example usage:
"""
from app.utils.logger import setup_logger
logger = setup_logger()
logger.info("This is an info message")
logger.error("This is an error message")
"""
#critical error 
"""
# Attempting to connect to the database
if not db_connected:
    logger.critical("Database connection failed — app cannot continue")
    # No point in continuing if critical error happens
    sys.exit(1)  # Shutdown the app
"""
#warning error
"""
# Fetching data from an API
if missing_data:
    logger.warning("Missing data for 3 coins, continuing with others")
else:
    logger.info("Data fetched successfully")
"""

#cheat sheet
"""
| Situation                      | Use                    |
| ------------------------------ | ---------------------- |
| Just tracking progress?        | `info`                 |
| Something odd happened but OK? | `warning`              |
| Something failed?              | `error` or `exception` |
| Major failure, crash risk?     | `critical`             |
| Developer-only detailed info?  | `debug`                |
"""