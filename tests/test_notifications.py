import unittest
from unittest.mock import patch, MagicMock
import sys

sys.path.insert(0, '.') # Add project root to path

# Import the functions to be tested
from src.utils.notifications import send_success_notification, send_failure_notification
# We need to mock the logger instance used within notifications.py
# The logger is obtained by get_logger(__name__), which means its name is 'src.utils.notifications'

class TestNotifications(unittest.TestCase):

    # Patch 'get_logger' within the 'src.utils.notifications' module's scope
    # This means when notifications.py calls get_logger, it gets our mock.
    @patch('src.utils.notifications.get_logger')
    def test_send_success_notification_logs_info(self, mock_get_logger):
        # Create a mock logger instance that get_logger will return
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        pipeline_name = "TestPipeline"
        message = "Process completed."
        details = {"items_processed": 100}

        send_success_notification(pipeline_name, message, details)

        # Verify get_logger was called correctly within notifications.py
        mock_get_logger.assert_called_once_with('src.utils.notifications')

        # Verify that the info method of the mock logger was called
        mock_logger.info.assert_called_once()
        # Check the content of the log message
        args, _ = mock_logger.info.call_args
        log_message = args[0]
        self.assertIn(f"SUCCESS: Pipeline '{pipeline_name}' - {message}", log_message)
        self.assertIn(f"Details: {details}", log_message)

    @patch('src.utils.notifications.get_logger')
    def test_send_success_notification_no_details(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        pipeline_name = "SimplePipeline"
        message = "Simple success."

        send_success_notification(pipeline_name, message)
        mock_logger.info.assert_called_once()
        args, _ = mock_logger.info.call_args
        log_message = args[0]
        self.assertIn(f"SUCCESS: Pipeline '{pipeline_name}' - {message}", log_message)
        self.assertNotIn("Details:", log_message)


    @patch('src.utils.notifications.get_logger')
    def test_send_failure_notification_logs_error(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        pipeline_name = "CriticalPipeline"
        message = "A critical error occurred."
        error_details = "Traceback: ..."
        details = {"step": "validation"}

        send_failure_notification(pipeline_name, message, error_details, details)

        mock_get_logger.assert_called_once_with('src.utils.notifications')

        # Check that logger.error was called for the main message
        # It will be called twice if error_details and details are both present in current implementation
        # First call for the message with error_details
        # Second call for additional details
        self.assertTrue(mock_logger.error.called)

        call_args_list = mock_logger.error.call_args_list

        # First call: main error message
        args1, kwargs1 = call_args_list[0]
        log_message1 = args1[0]
        self.assertIn(f"FAILURE: Pipeline '{pipeline_name}' - {message}. Error: {error_details}", log_message1)
        self.assertEqual(kwargs1.get('exc_info'), False) # As per current implementation

        # Second call (if details are present and non-empty)
        if details:
            self.assertEqual(len(call_args_list), 2)
            args2, _ = call_args_list[1]
            log_message2 = args2[0]
            self.assertIn(f"Additional Failure Details: {details}", log_message2)
        else:
            self.assertEqual(len(call_args_list), 1)


    @patch('src.utils.notifications.get_logger')
    def test_send_failure_notification_no_error_details_or_additional_details(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        pipeline_name = "SimpleFailPipeline"
        message = "Just failed."

        send_failure_notification(pipeline_name, message)
        mock_logger.error.assert_called_once()
        args, _ = mock_logger.error.call_args
        log_message = args[0]
        self.assertIn(f"FAILURE: Pipeline '{pipeline_name}' - {message}", log_message)
        self.assertNotIn("Error:", log_message)
        self.assertNotIn("Additional Failure Details:", log_message)


    @patch('src.utils.notifications.get_logger')
    def test_send_failure_notification_with_error_details_only(self, mock_get_logger):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        pipeline_name = "ErrorOnlyPipeline"
        message = "Failure with error string."
        error_details_str = "Exception: Something went wrong."

        send_failure_notification(pipeline_name, message, error_details=error_details_str)

        mock_logger.error.assert_called_once()
        args, kwargs = mock_logger.error.call_args
        log_message = args[0]

        self.assertIn(f"FAILURE: Pipeline '{pipeline_name}' - {message}. Error: {error_details_str}", log_message)
        self.assertEqual(kwargs.get('exc_info'), False)


if __name__ == "__main__":
    # To run these tests from the command line from the root directory:
    # python -m unittest tests.test_notifications
    unittest.main()
