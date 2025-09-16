"""Unit tests for WebAppHealthAlert class."""

from unittest.mock import Mock, patch

import requests

from monitoring.alerts.config import Cases
from monitoring.alerts.webapp_health_alert import WebAppHealthAlert


class TestWebAppHealthAlert:
    """Test suite for WebAppHealthAlert class."""

    def test_name_should_return_webapp_health_case(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = WebAppHealthAlert()

        # when
        result = alert.name

        # then
        assert result == Cases.WEBAPP_HEALTH

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    def test_get_issues_should_return_empty_list_when_webapp_url_not_configured(
        self, mock_env_get: Mock
    ) -> None:
        """Test that get_issues returns empty list when WEBAPP_URL is not configured."""
        # given
        mock_env_get.return_value = ""  # Empty WEBAPP_URL
        alert = WebAppHealthAlert()

        # when
        result = alert._get_issues([])

        # then
        assert result == []

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    @patch("monitoring.alerts.webapp_health_alert.requests.get")
    def test_get_issues_should_return_empty_list_when_webapp_responds_200(
        self, mock_requests_get: Mock, mock_env_get: Mock
    ) -> None:
        """Test that get_issues returns empty list when webapp responds with 200."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests_get.return_value = mock_response
        alert = WebAppHealthAlert()

        # when
        result = alert._get_issues([])

        # then
        assert result == []
        mock_requests_get.assert_called_once_with("http://localhost:8501", timeout=10)

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    @patch("monitoring.alerts.webapp_health_alert.requests.get")
    def test_get_issues_should_return_issue_when_webapp_responds_non_200(
        self, mock_requests_get: Mock, mock_env_get: Mock
    ) -> None:
        """Test that get_issues returns issue when webapp responds with non-200 status."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.reason = "Service Unavailable"
        mock_requests_get.return_value = mock_response
        alert = WebAppHealthAlert()

        # when
        result = alert._get_issues([])

        # then
        assert result == [("webapp", "HTTP 503: Service Unavailable")]

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    @patch("monitoring.alerts.webapp_health_alert.requests.get")
    def test_get_issues_should_return_issue_when_request_times_out(
        self, mock_requests_get: Mock, mock_env_get: Mock
    ) -> None:
        """Test that get_issues returns issue when request times out."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        mock_requests_get.side_effect = requests.exceptions.Timeout()
        alert = WebAppHealthAlert(timeout=5)

        # when
        result = alert._get_issues([])

        # then
        assert result == [("webapp", "Request timeout after 5 seconds")]
        mock_requests_get.assert_called_once_with("http://localhost:8501", timeout=5)

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    @patch("monitoring.alerts.webapp_health_alert.requests.get")
    def test_get_issues_should_return_issue_when_connection_fails(
        self, mock_requests_get: Mock, mock_env_get: Mock
    ) -> None:
        """Test that get_issues returns issue when connection fails."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        mock_requests_get.side_effect = requests.exceptions.ConnectionError()
        alert = WebAppHealthAlert()

        # when
        result = alert._get_issues([])

        # then
        assert result == [("webapp", "Connection failed - webapp may be down")]

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    @patch("monitoring.alerts.webapp_health_alert.requests.get")
    def test_get_issues_should_return_issue_when_request_exception_occurs(
        self, mock_requests_get: Mock, mock_env_get: Mock
    ) -> None:
        """Test that get_issues returns issue when a general request exception occurs."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        mock_requests_get.side_effect = requests.exceptions.RequestException(
            "SSL error"
        )
        alert = WebAppHealthAlert()

        # when
        result = alert._get_issues([])

        # then
        assert result == [("webapp", "Request failed: SSL error")]

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    @patch("monitoring.alerts.webapp_health_alert.requests.get")
    def test_get_issues_should_return_issue_when_unexpected_exception_occurs(
        self, mock_requests_get: Mock, mock_env_get: Mock
    ) -> None:
        """Test that get_issues returns issue when an unexpected exception occurs."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        mock_requests_get.side_effect = ValueError("Unexpected error")
        alert = WebAppHealthAlert()

        # when
        result = alert._get_issues([])

        # then
        assert result == [("webapp", "Unexpected error: Unexpected error")]

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    def test_format_message_should_format_single_issue_correctly(
        self, mock_env_get: Mock
    ) -> None:
        """Test that format_message formats single issue correctly."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        alert = WebAppHealthAlert()
        issues = [("webapp", "HTTP 503: Service Unavailable")]

        # when
        result = alert.format_message(issues)

        # then
        expected = "Webapp health check failed: HTTP 503: Service Unavailable (URL: http://localhost:8501)"
        assert result == expected

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    def test_format_message_should_handle_empty_issues_list(
        self, mock_env_get: Mock
    ) -> None:
        """Test that format_message handles empty issues list gracefully."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        alert = WebAppHealthAlert()
        issues = []

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "Webapp health check failed: Unknown error (URL: http://localhost:8501)"
        )
        assert result == expected

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    @patch("monitoring.alerts.webapp_health_alert.requests.get")
    def test_custom_timeout_should_be_used_in_request(
        self, mock_requests_get: Mock, mock_env_get: Mock
    ) -> None:
        """Test that custom timeout is passed to the request."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests_get.return_value = mock_response
        alert = WebAppHealthAlert(timeout=30)

        # when
        alert._get_issues([])

        # then
        mock_requests_get.assert_called_once_with("http://localhost:8501", timeout=30)

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    @patch("monitoring.alerts.webapp_health_alert.requests.get")
    def test_different_http_status_codes(
        self, mock_requests_get: Mock, mock_env_get: Mock
    ) -> None:
        """Test various HTTP status codes are handled correctly."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        alert = WebAppHealthAlert()

        test_cases = [
            (404, "Not Found"),
            (500, "Internal Server Error"),
            (502, "Bad Gateway"),
            (403, "Forbidden"),
        ]

        for status_code, reason in test_cases:
            # given
            mock_response = Mock()
            mock_response.status_code = status_code
            mock_response.reason = reason
            mock_requests_get.return_value = mock_response

            # when
            result = alert._get_issues([])

            # then
            assert result == [("webapp", f"HTTP {status_code}: {reason}")]

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    @patch("monitoring.alerts.webapp_health_alert.requests.get")
    def test_webapp_url_initialization_and_usage(
        self, mock_requests_get: Mock, mock_env_get: Mock
    ) -> None:
        """Test that webapp_url is properly initialized from environment and used."""
        # given
        test_url = "https://example.com:9000/health"
        mock_env_get.return_value = test_url
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests_get.return_value = mock_response

        # when
        alert = WebAppHealthAlert()
        result = alert._get_issues([])

        # then
        assert alert.webapp_url == test_url
        assert result == []
        mock_requests_get.assert_called_once_with(test_url, timeout=10)

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    def test_webapp_url_not_set_warning_logged(self, mock_env_get: Mock) -> None:
        """Test that warning is logged when WEBAPP_URL is not set."""
        # given
        mock_env_get.return_value = ""  # Empty webapp URL

        with patch(
            "monitoring.alerts.webapp_health_alert.logging.warning"
        ) as mock_warning:
            alert = WebAppHealthAlert()

            # when
            result = alert._get_issues([])

            # then
            assert result == []
            mock_warning.assert_called_once_with(
                "WEBAPP_URL environment variable not set, skipping webapp health check"
            )

    @patch("monitoring.alerts.webapp_health_alert.os.environ.get")
    @patch("monitoring.alerts.webapp_health_alert.requests.get")
    @patch("monitoring.alerts.webapp_health_alert.logging.exception")
    def test_unexpected_exception_logging(
        self, mock_logging_exception: Mock, mock_requests_get: Mock, mock_env_get: Mock
    ) -> None:
        """Test that unexpected exceptions are logged properly."""
        # given
        mock_env_get.return_value = "http://localhost:8501"
        mock_requests_get.side_effect = ValueError("Unexpected error")
        alert = WebAppHealthAlert()

        # when
        result = alert._get_issues([])

        # then
        assert result == [("webapp", "Unexpected error: Unexpected error")]
        mock_logging_exception.assert_called_once_with(
            "Unexpected error during webapp health check"
        )
