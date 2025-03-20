import logging
import os
from settings import *

def setup_logger(name):
    # Ensure logs directory exists
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    # Set up the logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create handlers
    file_handler = logging.FileHandler(LOG_FILE)
    stream_handler = logging.StreamHandler()

    # Create formatter and add it to handlers
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger