import json
import logging # Added missing import
# import requests # Placeholder for actual webhook integration
from .logger import get_logger

# Initialize logger for this module
LOGGER = get_logger(__name__)

class NotificationManager:
    """
    Manages sending notifications based on pipeline status and configuration.
    """
    def __init__(self, notification_config: list, pipeline_name: str, environment: str):
        """
        Initializes the NotificationManager.

        Args:
            notification_config (list): A list of notification configurations
                                        (e.g., from the main config.yaml).
            pipeline_name (str): Name of the pipeline, for context in messages.
            environment (str): Environment (dev, prod), for context.
        """
        self.notification_config = notification_config if notification_config else []
        self.pipeline_name = pipeline_name
        self.environment = environment

    def send_notification(self, subject: str, message: str, status: str = "INFO"):
        """
        Sends notifications based on the configured types.

        Args:
            subject (str): The subject of the notification.
            message (str): The main content of the notification.
            status (str): "INFO" or "ERROR", can be used to tailor message appearance.
        """
        if not self.notification_config:
            LOGGER.info("No notification channels configured.")
            # Still log the core message for traceability
            LOGGER.info(f"Notification Subject: {subject}\nMessage: {message}")
            return

        for config in self.notification_config:
            notification_type = config.get("type", "").lower()

            # Replace placeholders in subject and message
            full_subject = subject.replace("{{pipeline_name}}", self.pipeline_name).replace("{{environment}}", self.environment)
            full_message = message.replace("{{pipeline_name}}", self.pipeline_name).replace("{{environment}}", self.environment)

            if notification_type == "log":
                self._send_log_notification(full_subject, full_message, status)
            elif notification_type == "email":
                recipients = config.get("recipients")
                # Use configured subject if available, else use the passed subject
                email_subject = config.get("subject", full_subject).replace("{{pipeline_name}}", self.pipeline_name).replace("{{environment}}", self.environment)
                self._send_email_notification(recipients, email_subject, full_message, status)
            elif notification_type == "teams_webhook":
                webhook_url = config.get("webhook_url")
                self._send_teams_webhook_notification(webhook_url, full_subject, full_message, status)
            else:
                LOGGER.warning(f"Unsupported notification type: {notification_type}")

    def _send_log_notification(self, subject: str, message: str, status: str):
        """Sends a notification to the console log."""
        log_level = logging.ERROR if status.upper() == "ERROR" else logging.INFO
        LOGGER.log(log_level, f"--- NOTIFICATION ---")
        LOGGER.log(log_level, f"Subject: {subject}")
        LOGGER.log(log_level, f"Message: {message}")
        LOGGER.log(log_level, f"--- END NOTIFICATION ---")

    def _send_email_notification(self, recipients: list, subject: str, message: str, status: str):
        """
        Placeholder for sending email notifications.
        Requires an email sending library (e.g., smtplib) and configuration.
        """
        if not recipients:
            LOGGER.error("Email notification configured but no recipients provided.")
            return
        LOGGER.info(f"Emailing '{subject}' to {recipients}: {message[:100]}...")
        # Placeholder: Implement actual email sending logic here
        # Example:
        # import smtplib
        # from email.mime.text import MIMEText
        # msg = MIMEText(message)
        # msg['Subject'] = subject
        # msg['From'] = 'etl-noreply@example.com'
        # msg['To'] = ", ".join(recipients)
        # with smtplib.SMTP('smtp.example.com') as server:
        #     server.sendmail(msg['From'], recipients, msg.as_string())
        LOGGER.warning("Email notification is a placeholder. Implement actual sending logic.")


    def _send_teams_webhook_notification(self, webhook_url: str, subject: str, message: str, status: str):
        """
        Sends a notification to a Microsoft Teams channel via an incoming webhook.
        """
        if not webhook_url:
            LOGGER.error("Teams webhook notification configured but no webhook_url provided.")
            return

        LOGGER.info(f"Sending Teams notification: {subject}")

        # Basic adaptive card format for Teams
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "text": subject,
                                "weight": "Bolder",
                                "size": "Medium",
                                "color": "Attention" if status.upper() == "ERROR" else "Default"
                            },
                            {
                                "type": "TextBlock",
                                "text": f"Pipeline: {self.pipeline_name} ({self.environment})",
                                "isSubtle": True,
                                "spacing": "None"
                            },
                            {
                                "type": "TextBlock",
                                "text": message,
                                "wrap": True
                            }
                        ]
                    }
                }
            ]
        }

        try:
            # response = requests.post(webhook_url, data=json.dumps(payload), headers={'Content-Type': 'application/json'})
            # response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
            # LOGGER.info(f"Teams notification sent successfully. Status: {response.status_code}")
            LOGGER.warning(f"Teams webhook notification is a placeholder. Uncomment and test 'requests.post'. Payload: {json.dumps(payload)}")
        except Exception as e: # Catching requests.exceptions.RequestException for network issues
            LOGGER.error(f"Failed to send Teams webhook notification: {e}")


if __name__ == '__main__':
    # Example Usage (for testing this module independently)
    print("Running NotificationManager tests...")

    # Sample configurations
    sample_pipeline_name = "TestPipeline"
    sample_environment = "DEV"

    # Test 1: Log notification only
    log_only_config = [{"type": "log"}]
    manager_log = NotificationManager(log_only_config, sample_pipeline_name, sample_environment)
    LOGGER.info("\n--- Test 1: Log Notification (Success) ---")
    manager_log.send_notification(
        subject=f"Pipeline {{pipeline_name}} Succeeded on {{environment}}",
        message="All steps completed successfully.",
        status="INFO"
    )
    LOGGER.info("\n--- Test 1: Log Notification (Failure) ---")
    manager_log.send_notification(
        subject=f"Pipeline {{pipeline_name}} Failed on {{environment}}",
        message="Error encountered in transformation step.",
        status="ERROR"
    )

    # Test 2: Email (placeholder) and Log notification
    email_log_config = [
        {"type": "log"},
        {
            "type": "email",
            "recipients": ["test@example.com", "dev-alerts@example.com"],
            "subject": "ALERT: ETL Job {{pipeline_name}} - {{environment}}" # Custom subject
        }
    ]
    manager_email_log = NotificationManager(email_log_config, sample_pipeline_name, sample_environment)
    LOGGER.info("\n--- Test 2: Email (Placeholder) and Log Notification ---")
    manager_email_log.send_notification(
        subject="Default Subject: Pipeline {{pipeline_name}} Update", # This will be overridden for email by its config
        message="This is a test message for email and log.",
        status="INFO"
    )

    # Test 3: Teams Webhook (placeholder)
    teams_config = [
        {"type": "log"},
        {
            "type": "teams_webhook",
            "webhook_url": "https://your-teams-webhook-url.com/...." # Replace with a dummy or real URL for testing
        }
    ]
    manager_teams = NotificationManager(teams_config, "DataProcessingPipeline", "PROD")
    LOGGER.info("\n--- Test 3: Teams Webhook (Placeholder) Notification ---")
    manager_teams.send_notification(
        subject="Critical Alert: {{pipeline_name}} Failure",
        message="A critical error occurred in the production data pipeline. Details: Connection timeout to source.",
        status="ERROR"
    )

    # Test 4: No notifications configured
    manager_none = NotificationManager(None, sample_pipeline_name, sample_environment)
    LOGGER.info("\n--- Test 4: No Notifications Configured ---")
    manager_none.send_notification(
        subject="This should only log",
        message="No channels are set up."
    )

    # Test 5: Invalid notification type
    invalid_config = [{"type": "sms"}]
    manager_invalid = NotificationManager(invalid_config, sample_pipeline_name, sample_environment)
    LOGGER.info("\n--- Test 5: Invalid Notification Type ---")
    manager_invalid.send_notification(
        subject="Test Invalid Type",
        message="This notification type is not supported."
    )

    LOGGER.info("\nNotificationManager tests complete. Review log output.")
