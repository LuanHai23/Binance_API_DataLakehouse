import json
import unittest
from unittest.mock import Mock, patch

from kafka_producer import Binance_kafka_producer as binance_producer

class FakeRuntime:
    def __init__(
        self,
        stop_states,
        wait_result=True,
    ):
        self._stop_states = iter(stop_states)
        self.wait_result = wait_result
        self.attached = []
        self.detached = []
        self.wait_calls = []

    @property
    def stop_requested(self):
        return next(self._stop_states)

    def attach_websocket(self, websocket_app):
        self.attached.append(websocket_app)

    def detach_websocket(self, websocket_app):
        self.detached.append(websocket_app)

    def wait(self, timeout_seconds):
        self.wait_calls.append(timeout_seconds)
        return self.wait_result

class TestBinanceProducer(unittest.TestCase):

    def setUp(self):
        binance_producer.msg_count = 0

    def tearDown(self):
        binance_producer.msg_count = 0

    def test_valid_message_publishes_once(self):
        publisher = Mock()

        raw_message = {
            "data": {
                "e": "aggTrade",
                "s": "btcusdt",
                "a": 123,
                "p": "50000.12000000",
                "q": "0.00100000",
                "f": 120,
                "l": 123,
                "T": 1720000000123,
                "m": False,
            }
        }

        binance_producer.on_message(
            Mock(),
            json.dumps(raw_message),
            publisher=publisher,
        )

        publisher.publish.assert_called_once()

    def test_published_event_has_expected_contract_fields(self):
        publisher = Mock()

        raw_message = {
            "data": {
                "e": "aggTrade",
                "s": "btcusdt",
                "a": 123,
                "p": "50000.12000000",
                "q": "0.00100000",
                "f": 120,
                "l": 123,
                "T": 1720000000123,
                "m": False,
            }
        }

        binance_producer.on_message(
            Mock(),
            json.dumps(raw_message),
            publisher=publisher,
        )

        event = publisher.publish.call_args.args[0]

        self.assertEqual(
            event["event_id"],
            "binance:aggTrade:BTCUSDT:123",
        )
        self.assertEqual(
            event["schema_version"],
            1,
        )
        self.assertEqual(
            event["symbol"],
            "BTCUSDT",
        )

    def test_message_without_data_is_not_published(self):
        publisher = Mock()

        raw_message = {
            "stream": "btcusdt@aggTrade",
        }

        binance_producer.on_message(
            Mock(),
            json.dumps(raw_message),
            publisher=publisher,
        )

        publisher.publish.assert_not_called()

    def test_invalid_json_is_not_published(self):
        publisher = Mock()

        binance_producer.on_message(
            Mock(),
            "{invalid-json",
            publisher=publisher,
        )

        publisher.publish.assert_not_called()

    def test_publisher_error_propagates(self):
        publisher = Mock()

        publisher.publish.side_effect = RuntimeError(
            "publisher failed"
        )

        raw_message = {
            "data": {
                "e": "aggTrade",
                "s": "btcusdt",
                "a": 123,
                "p": "50000.12000000",
                "q": "0.00100000",
                "f": 120,
                "l": 123,
                "T": 1720000000123,
                "m": False,
            }
        }

        with self.assertRaisesRegex(
            RuntimeError,
            "publisher failed",
        ):
            binance_producer.on_message(
                Mock(),
                json.dumps(raw_message),
                publisher=publisher,
            )

    @patch.object(binance_producer, "run")
    @patch.object(binance_producer, "create_publisher")
    @patch.object(binance_producer, "RuntimeController")
    def test_main_normal_lifecycle(
        self,
        mock_runtime_class,
        mock_create_publisher,
        mock_run,
    ):
        runtime = Mock()
        publisher = Mock()

        mock_runtime_class.return_value = runtime
        mock_create_publisher.return_value = publisher

        binance_producer.main()

        mock_runtime_class.assert_called_once_with(
            None
        )
        mock_create_publisher.assert_called_once_with(
            binance_producer.os.environ
        )

        runtime.start.assert_called_once_with()
        publisher.start.assert_called_once_with()
        mock_run.assert_called_once_with(
            publisher,
            runtime,
        )

        runtime.close.assert_called_once_with()
        publisher.close.assert_called_once_with()

    @patch.object(binance_producer, "run")
    @patch.object(binance_producer, "create_publisher")
    @patch.object(binance_producer, "RuntimeController")
    @patch.object(
        binance_producer,
        "parse_run_duration_seconds",
    )
    def test_main_parses_duration_and_creates_runtime(
        self,
        mock_parse_duration,
        mock_runtime_class,
        mock_create_publisher,
        mock_run,
    ):
        mock_parse_duration.return_value = 300

        runtime = Mock()
        publisher = Mock()

        mock_runtime_class.return_value = runtime
        mock_create_publisher.return_value = publisher

        binance_producer.main()

        mock_parse_duration.assert_called_once_with(
            binance_producer.os.environ
        )
        mock_runtime_class.assert_called_once_with(300)

    @patch.object(binance_producer, "run")
    @patch.object(binance_producer, "create_publisher")
    @patch.object(binance_producer, "RuntimeController")
    def test_main_starts_runtime_before_publisher(
        self,
        mock_runtime_class,
        mock_create_publisher,
        mock_run,
    ):
        runtime = Mock()
        publisher = Mock()

        mock_runtime_class.return_value = runtime
        mock_create_publisher.return_value = publisher

        calls = []

        runtime.start.side_effect = lambda: calls.append(
            "runtime.start"
        )

        publisher.start.side_effect = lambda: calls.append(
            "publisher.start"
        )

        binance_producer.main()

        self.assertEqual(
            calls,
            [
                "runtime.start",
                "publisher.start",
            ],
        )

    @patch.object(binance_producer, "run")
    @patch.object(binance_producer, "create_publisher")
    @patch.object(binance_producer, "RuntimeController")
    def test_main_start_failure_closes_runtime_and_publisher(
        self,
        mock_runtime_class,
        mock_create_publisher,
        mock_run,
    ):
        runtime = Mock()
        publisher = Mock()

        runtime.start.side_effect = RuntimeError(
            "runtime start failed"
        )

        mock_runtime_class.return_value = runtime
        mock_create_publisher.return_value = publisher

        with self.assertRaisesRegex(
            RuntimeError,
            "runtime start failed",
        ):
            binance_producer.main()

        mock_run.assert_not_called()
        runtime.close.assert_called_once_with()
        publisher.close.assert_called_once_with()

    @patch.object(binance_producer, "run")
    @patch.object(binance_producer, "create_publisher")
    @patch.object(binance_producer, "RuntimeController")
    def test_main_runtime_failure_closes_runtime_and_publisher(
        self,
        mock_runtime_class,
        mock_create_publisher,
        mock_run,
    ):
        runtime = Mock()
        publisher = Mock()

        mock_runtime_class.return_value = runtime
        mock_create_publisher.return_value = publisher

        mock_run.side_effect = RuntimeError(
            "runtime failed"
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "runtime failed",
        ):
            binance_producer.main()

        runtime.start.assert_called_once_with()
        publisher.start.assert_called_once_with()
        runtime.close.assert_called_once_with()
        publisher.close.assert_called_once_with()

    @patch.object(
        binance_producer.signal,
        "signal",
    )
    @patch.object(
        binance_producer.signal,
        "getsignal",
    )
    def test_shutdown_signal_requests_runtime_stop(
        self,
        mock_getsignal,
        mock_signal,
    ):
        runtime = Mock()

        binance_producer.on_shutdown_signal(
            binance_producer.signal.SIGTERM,
            None,
            runtime=runtime,
        )

        runtime.request_stop.assert_called_once_with()

    @patch.object(binance_producer, "run")
    @patch.object(binance_producer, "create_publisher")
    @patch.object(binance_producer, "RuntimeController")
    def test_main_restores_sigterm_handler(
        self,
        mock_runtime_class,
        mock_create_publisher,
        mock_run,
    ):
        runtime = Mock()
        publisher = Mock()

        mock_runtime_class.return_value = runtime
        mock_create_publisher.return_value = publisher

        previous_handler = Mock()

        with patch.object(
            binance_producer.signal,
            "getsignal",
            return_value=previous_handler,
        ) as mock_getsignal, patch.object(
            binance_producer.signal,
            "signal",
        ) as mock_signal:
            binance_producer.main()

        mock_getsignal.assert_called_once_with(
            binance_producer.signal.SIGTERM
        )

        mock_signal.assert_any_call(
            binance_producer.signal.SIGTERM,
            previous_handler,
        )

    @patch.object(binance_producer.websocket, "WebSocketApp")
    def test_run_attaches_and_detaches_websocket(self, mock_ws_app):
        runtime = Mock()
        publisher = Mock()

        runtime.stop_requested = False

        ws_app = Mock()
        mock_ws_app.return_value = ws_app

        def run_forever_side_effect(**kwargs):
            runtime.stop_requested = True

        ws_app.run_forever.side_effect = (
            run_forever_side_effect
        )

        binance_producer.run(
            publisher,
            runtime,
        )

        runtime.attach_websocket.assert_called_once_with(
            ws_app
        )
        runtime.detach_websocket.assert_called_once_with(
            ws_app
        )

        ws_app.run_forever.assert_called_once_with(
            ping_interval=20,
            ping_timeout=10,
        )

    @patch.object(binance_producer.websocket,"WebSocketApp",)
    def test_run_does_not_start_socket_when_stop_arrives_after_attach(
        self,
        mock_ws_app,
    ):
        publisher = Mock()
        ws_app = Mock()
        mock_ws_app.return_value = ws_app

        runtime = FakeRuntime(
            stop_states=[
                False,  # enter while
                True,   # stop immediately after attach
            ],
        )

        binance_producer.run(
            publisher,
            runtime,
        )

        self.assertEqual(
            runtime.attached,
            [ws_app],
        )
        self.assertEqual(
            runtime.detached,
            [ws_app],
        )
        ws_app.run_forever.assert_not_called()

    @patch.object(binance_producer.websocket, "WebSocketApp")
    def test_run_does_not_create_websocket_when_already_stopped(
        self,
        mock_ws_app,
    ):
        runtime = Mock()
        publisher = Mock()

        runtime.stop_requested = True

        binance_producer.run(
            publisher,
            runtime,
        )

        mock_ws_app.assert_not_called()

    @patch.object(binance_producer.websocket,"WebSocketApp",)
    def test_run_waits_for_reconnect_using_runtime(
        self,
        mock_ws_app,
    ):
        publisher = Mock()

        runtime = FakeRuntime(
            stop_states=[
                False,  # while condition
                False,  # after attach
                False,   # after run_forever
            ],
            wait_result=True,
        )

        ws_app = Mock()
        mock_ws_app.return_value = ws_app

        binance_producer.run(
            publisher,
            runtime,
        )

        self.assertEqual(
            runtime.wait_calls,
            [5],
        )

        self.assertEqual(
            len(runtime.attached),
            1,
        )

        self.assertEqual(
            len(runtime.detached),
            1,
        )

        mock_ws_app.assert_called_once_with(
            binance_producer.BINANCE_SOCKET,
            on_open=unittest.mock.ANY,
            on_message=unittest.mock.ANY,
            on_error=binance_producer.on_error,
            on_close=binance_producer.on_close,
        )

        ws_app.run_forever.assert_called_once_with(
            ping_interval=20,
            ping_timeout=10,
        )

    @patch.object(binance_producer.websocket,"WebSocketApp",)
    def test_run_does_not_reconnect_when_wait_requests_stop(
        self,
        mock_ws_app,
    ):
        publisher = Mock()

        runtime = FakeRuntime(
            stop_states=[
                False,
                False,
                False,
            ],
            wait_result=True,
        )

        ws_app = Mock()
        mock_ws_app.return_value = ws_app

        binance_producer.run(
            publisher,
            runtime,
        )

        self.assertEqual(
            runtime.wait_calls,
            [5],
        )

        mock_ws_app.assert_called_once()

    def test_on_open_closes_websocket_after_stop(self):
        runtime = Mock()
        runtime.stop_requested = True

        ws = Mock()

        binance_producer.on_open(
            ws,
            runtime=runtime,
        )

        ws.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()