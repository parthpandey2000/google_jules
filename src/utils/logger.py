import logging
import sys

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configures and returns a logger.

    Args:
        name (str): Name of the logger, typically __name__ of the calling module.
        level (int): Logging level, e.g., logging.INFO, logging.DEBUG.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding multiple handlers if logger already has them
    if not logger.handlers:
        # Create a handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)

        # Create a formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(handler)

    return logger

if __name__ == "__main__":
    # Example usage:
    logger = get_logger(__name__)
    logger.info("This is an info message from the logger module.")
    logger.debug("This is a debug message (will not be shown with default INFO level).")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")

    debug_logger = get_logger("my_debug_logger", level=logging.DEBUG)
    debug_logger.debug("This is a debug message from a different logger instance.")
    debug_logger.info("Info from debug_logger.")
