import logging
import sys

def get_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Initializes and returns a logger.

    Args:
        name (str): The name for the logger, typically __name__.
        level: The logging level, e.g., logging.INFO.

    Returns:
        logging.Logger: A configured logger instance.
    """
    logger = logging.getLogger(name)

    # Prevents adding handlers multiple times in certain environments like Databricks notebooks
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger
