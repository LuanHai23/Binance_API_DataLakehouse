import unittest
from unittest.mock import Mock, patch

from kafka_producer.runtime_control import (
    MAX_RUN_DURATION_SECONDS,
    RuntimeController,
    parse_run_duration_seconds,
)


class TestParseRunDurationSeconds(unittest.TestCase):

    def test_missing_duration_returns_none(self):
        config = {}

        result = parse_run_duration_seconds(config)

        self.assertIsNone(result)

    def test_valid_duration_returns_integer(self):
        config = {
            "RUN_DURATION_SECONDS": "300",
        }

        result = parse_run_duration_seconds(config)

        self.assertEqual(result, 300)
        self.assertIsInstance(result, int)

    def test_duration_is_trimmed(self):
        config = {
            "RUN_DURATION_SECONDS": " 300 ",
        }

        result = parse_run_duration_seconds(config)

        self.assertEqual(result, 300)

    def test_blank_duration_raises_value_error(self):
        invalid_values = [
            "",
            "   ",
            "\t",
            "\n",
        ]

        for value in invalid_values:
            with self.subTest(value=repr(value)):
                config = {
                    "RUN_DURATION_SECONDS": value,
                }

                with self.assertRaises(ValueError):
                    parse_run_duration_seconds(config)

    def test_non_integer_duration_raises_value_error(self):
        invalid_values = [
            "1.5",
            "abc",
            "300.0",
            "1e2",
        ]

        for value in invalid_values:
            with self.subTest(value=value):
                config = {
                    "RUN_DURATION_SECONDS": value,
                }

                with self.assertRaises(ValueError):
                    parse_run_duration_seconds(config)

    def test_non_positive_duration_raises_value_error(self):
        invalid_values = [
            "0",
            "-1",
            "-300",
        ]

        for value in invalid_values:
            with self.subTest(value=value):
                config = {
                    "RUN_DURATION_SECONDS": value,
                }

                with self.assertRaises(ValueError):
                    parse_run_duration_seconds(config)

    def test_duration_above_maximum_raises_value_error(self):
        config = {
            "RUN_DURATION_SECONDS": str(
                MAX_RUN_DURATION_SECONDS + 1
            ),
        }

        with self.assertRaises(ValueError):
            parse_run_duration_seconds(config)

    def test_non_string_duration_raises_value_error(self):
        invalid_values = [
            300,
            300.0,
            True,
            None,
            [],
            {},
        ]

        for value in invalid_values:
            with self.subTest(value=repr(value)):
                config = {
                    "RUN_DURATION_SECONDS": value,
                }

                with self.assertRaises(ValueError):
                    parse_run_duration_seconds(config)

    def test_duration_boundaries_are_valid(self):
        valid_values = [
            ("1", 1),
            (
                str(MAX_RUN_DURATION_SECONDS),
                MAX_RUN_DURATION_SECONDS,
            ),
        ]

        for value, expected in valid_values:
            with self.subTest(value=value):
                result = parse_run_duration_seconds(
                    {"RUN_DURATION_SECONDS": value}
                )

                self.assertEqual(result, expected)


class TestRuntimeController(unittest.TestCase):

    @patch("kafka_producer.runtime_control.threading.Timer")
    def test_start_without_duration_does_not_create_timer(
        self,
        mock_timer,
    ):
        controller = RuntimeController(None)

        controller.start()

        mock_timer.assert_not_called()

    @patch("kafka_producer.runtime_control.threading.Timer")
    def test_start_with_duration_arms_daemon_timer(
        self,
        mock_timer,
    ):
        timer = Mock()
        mock_timer.return_value = timer

        controller = RuntimeController(300)

        controller.start()

        mock_timer.assert_called_once_with(
            300,
            controller.request_stop,
        )

        self.assertTrue(timer.daemon)

        timer.start.assert_called_once_with()

    @patch("kafka_producer.runtime_control.threading.Timer")
    def test_timer_callback_requests_stop_and_closes_websocket(
        self,
        mock_timer,
    ):
        timer = Mock()
        mock_timer.return_value = timer

        websocket_app = Mock()

        controller = RuntimeController(300)
        controller.start()
        controller.attach_websocket(websocket_app)

        callback = mock_timer.call_args.args[1]

        callback()

        self.assertTrue(controller.stop_requested)
        websocket_app.close.assert_called_once_with()

    def test_request_stop_is_idempotent(self):
        websocket_app = Mock()

        controller = RuntimeController(None)

        controller.attach_websocket(websocket_app)

        controller.request_stop()
        controller.request_stop()
        controller.request_stop()

        self.assertTrue(controller.stop_requested)

        websocket_app.close.assert_called_once_with()

    def test_attach_after_stop_closes_websocket_immediately(self):
        websocket_app = Mock()

        controller = RuntimeController(None)

        controller.request_stop()
        controller.attach_websocket(websocket_app)

        websocket_app.close.assert_called_once_with()

    def test_detach_prevents_websocket_from_being_closed(self):
        websocket_app = Mock()

        controller = RuntimeController(None)

        controller.attach_websocket(websocket_app)
        controller.detach_websocket(websocket_app)
        controller.request_stop()

        websocket_app.close.assert_not_called()

    def test_detaching_other_websocket_preserves_active_websocket(
        self,
    ):
        active_websocket = Mock()
        other_websocket = Mock()

        controller = RuntimeController(None)

        controller.attach_websocket(active_websocket)
        controller.detach_websocket(other_websocket)
        controller.request_stop()

        active_websocket.close.assert_called_once_with()
        other_websocket.close.assert_not_called()

    def test_wait_returns_event_wait_result(self):
        controller = RuntimeController(None)

        with patch.object(
            controller._stop_event,
            "wait",
            return_value=True,
        ) as mock_wait:
            result = controller.wait(5)

        self.assertTrue(result)
        mock_wait.assert_called_once_with(5)

    @patch("kafka_producer.runtime_control.threading.Timer")
    def test_close_cancels_timer(self, mock_timer):
        timer = Mock()
        mock_timer.return_value = timer

        controller = RuntimeController(300)
        controller.start()
        controller.close()

        timer.cancel.assert_called_once_with()

    @patch("kafka_producer.runtime_control.threading.Timer")
    def test_close_is_idempotent(self, mock_timer):
        timer = Mock()
        mock_timer.return_value = timer

        websocket_app = Mock()

        controller = RuntimeController(300)
        controller.start()
        controller.attach_websocket(websocket_app)

        controller.close()
        controller.close()

        timer.cancel.assert_called_once_with()
        websocket_app.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()