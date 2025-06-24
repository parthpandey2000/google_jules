import unittest
import logging
from io import StringIO
import sys

# Add src to sys.path to allow direct import of modules
# This might be needed if running tests directly and Python can't find src
# For a proper project structure with setup.py or pytest config, this might not be necessary
sys.path.insert(0, '.') # Add project root to path

from src.utils.logger import get_logger

class TestLogger(unittest.TestCase):

    def setUp(self):
        # Redirect stdout to capture log messages for stream handler tests
        self.captured_output = StringIO()
        sys.stdout = self.captured_output
        # Reset logging handlers to ensure clean state for each test
        # This is important because getLogger caches logger instances
        logging.shutdown()
        # Ensure no handlers are present on the root logger from previous tests
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)


    def tearDown(self):
        # Restore stdout
        sys.stdout = sys.__stdout__
        # Clean up logging handlers again
        logging.shutdown()
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        # Clear any cached loggers by removing references
        # Get all current loggers
        loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
        for logger in loggers:
            logger.handlers = []
            logger.propagate = True # Reset propagate attribute
            # logger.setLevel(logging.NOTSET) # Reset level if needed


    def test_get_logger_creates_logger(self):
        logger_name = "test_logger_1"
        logger = get_logger(logger_name)
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, logger_name)

    def test_get_logger_default_level_info(self):
        logger = get_logger("test_logger_info")
        self.assertEqual(logger.level, logging.INFO)
        # Check handler's level as well, if a stream handler is added by default
        if logger.handlers:
            self.assertEqual(logger.handlers[0].level, logging.INFO)

    def test_get_logger_custom_level_debug(self):
        logger = get_logger("test_logger_debug", level=logging.DEBUG)
        self.assertEqual(logger.level, logging.DEBUG)
        if logger.handlers:
            self.assertEqual(logger.handlers[0].level, logging.DEBUG)

    def test_logger_output_format_info(self):
        logger = get_logger("output_test_logger_info", level=logging.INFO)
        message = "Info test message"
        logger.info(message)
        log_output = self.captured_output.getvalue()
        self.assertIn(message, log_output)
        self.assertIn("INFO", log_output)
        self.assertIn("output_test_logger_info", log_output)
        # Example: 2023-04-01 12:34:56 - output_test_logger_info - INFO - Info test message
        self.assertRegex(log_output, r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} - output_test_logger_info - INFO - Info test message")

    def test_logger_debug_message_not_shown_at_info_level(self):
        logger = get_logger("debug_filter_logger", level=logging.INFO)
        logger.debug("This is a debug message.")
        log_output = self.captured_output.getvalue()
        self.assertEqual(log_output, "") # Should be empty as debug < info

    def test_logger_debug_message_shown_at_debug_level(self):
        logger = get_logger("debug_show_logger", level=logging.DEBUG)
        message = "This debug message should be shown."
        logger.debug(message)
        log_output = self.captured_output.getvalue()
        self.assertIn(message, log_output)
        self.assertIn("DEBUG", log_output)

    def test_get_logger_returns_same_instance_for_same_name(self):
        logger1 = get_logger("shared_logger")
        logger2 = get_logger("shared_logger")
        self.assertIs(logger1, logger2)

    def test_get_logger_adds_handler_only_once(self):
        logger_name = "handler_test_logger"
        logger1 = get_logger(logger_name)
        self.assertEqual(len(logger1.handlers), 1) # Should have one handler

        # Call get_logger again for the same logger name
        logger2 = get_logger(logger_name)
        self.assertEqual(len(logger2.handlers), 1) # Should still have only one handler
        self.assertIs(logger1.handlers[0], logger2.handlers[0]) # And it's the same handler instance

if __name__ == "__main__":
    # To run these tests from the command line from the root directory:
    # python -m unittest tests.test_logger
    unittest.main()
