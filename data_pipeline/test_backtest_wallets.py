import copy
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

os.environ.setdefault("HELIUS_API_KEY", "offline-test")

import data_pipeline.backtest_wallets as backtester
from data_pipeline.backtest_wallets import (
    PUMP_AMM_PROGRAM,
    PilotFatalError,
    configure_stdout_utf8,
    is_pump_fun,
    is_pump_swap,
    parse_trade,
    run_pilot,
    score_wallet,
)


WALLET = "wallet"
MINT = "token-mint"


def trade_fixture(native_balance_change=800_328_221):
    return {
        "accountData": [
            {
                "account": WALLET,
                "nativeBalanceChange": native_balance_change,
            }
        ],
        "tokenTransfers": [
            {
                "fromUserAccount": WALLET,
                "toUserAccount": "counterparty",
                "mint": MINT,
                "tokenAmount": 18_969_867.095602,
            }
        ],
    }


def pump_swap_fixture(native_balance_change=800_328_221, source="PUMP_AMM"):
    tx = trade_fixture(native_balance_change)
    tx["source"] = source
    return tx


class ParseTradeTests(unittest.TestCase):
    def test_existing_pump_fun_parsing_is_unchanged(self):
        tx = trade_fixture()
        tx["source"] = "PUMP_FUN"

        self.assertTrue(is_pump_fun(tx))
        with redirect_stdout(io.StringIO()):
            result = score_wallet(WALLET, [tx])

        self.assertEqual(result["total_trades"], 1)
        self.assertEqual(result["net_sol"], 0.8003)

        tx["source"] = "JUPITER"
        tx["instructions"] = [
            {
                "programId": "router",
                "innerInstructions": [
                    {"programId": backtester.PUMP_FUN_PROGRAM}
                ],
            }
        ]
        self.assertFalse(is_pump_fun(tx))

    def test_direct_pump_amm_buy_parses_correctly(self):
        tx = pump_swap_fixture(-500_000_000)
        tx["tokenTransfers"][0]["fromUserAccount"] = "counterparty"
        tx["tokenTransfers"][0]["toUserAccount"] = WALLET

        self.assertTrue(is_pump_swap(tx))
        trade = parse_trade(tx, WALLET)

        self.assertEqual(trade["direction"], "buy")
        self.assertEqual(trade["sol_amount"], 0.5)

    def test_direct_pump_amm_sell_parses_correctly(self):
        tx = pump_swap_fixture()

        self.assertTrue(is_pump_swap(tx))
        trade = parse_trade(tx, WALLET)

        self.assertEqual(trade["direction"], "sell")
        self.assertEqual(trade["sol_amount"], 0.800328221)

    def test_routed_source_requires_pump_swap_program_id(self):
        tx = pump_swap_fixture(152_351_294, source="JUPITER")
        tx["tokenTransfers"][0]["tokenAmount"] = 2_471_609.859401
        tx["instructions"] = [
            {
                "programId": "router",
                "innerInstructions": [{"programId": PUMP_AMM_PROGRAM}],
            }
        ]

        self.assertTrue(is_pump_swap(tx))
        with redirect_stdout(io.StringIO()):
            result = score_wallet(WALLET, [tx])
        self.assertEqual(result["total_trades"], 1)

        del tx["instructions"]
        self.assertFalse(is_pump_swap(tx))

    def test_sell_uses_wallet_native_balance_change(self):
        trade = parse_trade(trade_fixture(), WALLET)

        self.assertEqual(trade["direction"], "sell")
        self.assertEqual(trade["sol_amount"], 0.800328221)
        self.assertEqual(trade["token_amount"], 18_969_867.095602)

    def test_buy_requires_tokens_received_and_sol_spent(self):
        tx = trade_fixture(-500_000_000)
        tx["tokenTransfers"][0]["fromUserAccount"] = "counterparty"
        tx["tokenTransfers"][0]["toUserAccount"] = WALLET

        trade = parse_trade(tx, WALLET)

        self.assertEqual(trade["direction"], "buy")
        self.assertEqual(trade["sol_amount"], 0.5)

    def test_malformed_or_missing_account_data_fails_closed(self):
        cases = {
            "missing": {},
            "null": {"accountData": None},
            "not_list": {"accountData": {}},
            "malformed_record": {"accountData": ["invalid"]},
            "wallet_missing": {
                "accountData": [
                    {"account": "other", "nativeBalanceChange": 800_328_221}
                ]
            },
            "duplicate_wallet": {
                "accountData": [
                    {"account": WALLET, "nativeBalanceChange": 800_328_221},
                    {"account": WALLET, "nativeBalanceChange": 800_328_221},
                ]
            },
        }

        for name, account_case in cases.items():
            with self.subTest(name=name):
                tx = trade_fixture()
                if "accountData" in account_case:
                    tx["accountData"] = account_case["accountData"]
                else:
                    del tx["accountData"]
                self.assertIsNone(parse_trade(tx, WALLET))

    def test_invalid_native_balance_change_fails_closed(self):
        cases = {
            "missing": object(),
            "null": None,
            "non_numeric": "800328221",
            "boolean": True,
            "zero": 0,
        }

        for name, value in cases.items():
            with self.subTest(name=name):
                tx = trade_fixture()
                if name == "missing":
                    del tx["accountData"][0]["nativeBalanceChange"]
                else:
                    tx["accountData"][0]["nativeBalanceChange"] = value
                self.assertIsNone(parse_trade(tx, WALLET))

    def test_token_and_sol_direction_disagreement_fails_closed(self):
        tx = pump_swap_fixture(-800_328_221)

        self.assertIsNone(parse_trade(tx, WALLET))

    def test_transaction_fee_alone_is_not_a_buy(self):
        tx = trade_fixture(-2_000_000)
        tx["fee"] = 2_000_000
        tx["feePayer"] = WALLET
        tx["tokenTransfers"][0]["fromUserAccount"] = "counterparty"
        tx["tokenTransfers"][0]["toUserAccount"] = WALLET

        self.assertIsNone(parse_trade(tx, WALLET))

    def test_ambiguous_pump_swap_multi_leg_fails_closed(self):
        tx = pump_swap_fixture(-19_276_022)
        tx["fee"] = 17_201_942
        tx["feePayer"] = WALLET
        tx["tokenTransfers"][0]["tokenAmount"] = 0.000084
        received = copy.deepcopy(tx["tokenTransfers"][0])
        received["fromUserAccount"] = "counterparty"
        received["toUserAccount"] = WALLET
        tx["tokenTransfers"].append(received)

        self.assertIsNone(parse_trade(tx, WALLET))


class StdoutConfigurationTests(unittest.TestCase):
    def test_reconfigures_text_wrapper_to_utf8(self):
        buffer = io.BytesIO()
        stream = io.TextIOWrapper(buffer, encoding="cp1252")
        try:
            configure_stdout_utf8(stream)
            stream.write("🚀")
            stream.flush()

            self.assertEqual(stream.encoding, "utf-8")
            self.assertEqual(buffer.getvalue(), "🚀".encode("utf-8"))
        finally:
            stream.close()

    def test_string_io_without_reconfigure_is_unchanged(self):
        stream = io.StringIO()

        configure_stdout_utf8(stream)
        stream.write("🚀")

        self.assertEqual(stream.getvalue(), "🚀")

    def test_replacement_stream_reconfigure_failure_is_ignored(self):
        class ReplacementStream:
            encoding = "cp1252"

            def reconfigure(self, **_kwargs):
                raise TypeError("unsupported")

        configure_stdout_utf8(ReplacementStream())


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self.payload = [] if payload is None else payload
        self.headers = headers or {}

    def json(self):
        return copy.deepcopy(self.payload)


class FakeHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, url, *, params, timeout):
        self.calls.append((url, dict(params), timeout))
        if not self.responses:
            raise AssertionError("unexpected HTTP request")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def full_page(prefix):
    return [{"signature": f"{prefix}-{index}"} for index in range(100)]


class PilotBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.candidates = self.root / "candidates.txt"
        self.output = self.root / "output"

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_candidates(self, wallets):
        self.candidates.write_text("\n".join(wallets) + "\n", encoding="utf-8")

    def run_pilot_case(self, client, sleeper=lambda _seconds: None):
        return run_pilot(
            self.candidates,
            self.output,
            request_get=client,
            sleeper=sleeper,
        )

    def test_more_than_ten_unique_wallets_rejected_before_request(self):
        self.write_candidates([f"wallet-{index}" for index in range(11)])
        client = FakeHttp([])

        with self.assertRaisesRegex(PilotFatalError, "exceeds 10"):
            self.run_pilot_case(client)

        self.assertEqual(client.calls, [])

    def test_ten_full_pages_are_incomplete_and_cannot_qualify(self):
        wallet = "wallet-one"
        self.write_candidates([wallet])
        client = FakeHttp(
            [FakeResponse(payload=full_page(f"page-{index}")) for index in range(10)]
        )

        results, manifest = self.run_pilot_case(client)

        self.assertEqual(len(client.calls), 10)
        self.assertEqual(results, [])
        self.assertEqual(manifest["wallets"][wallet]["status"], "INCOMPLETE")
        self.assertEqual(manifest["wallets"][wallet]["pages_cached"], 10)

    def test_120_attempt_and_12000_credit_ceiling(self):
        wallet = "wallet-one"
        self.write_candidates([wallet])
        self.output.mkdir()
        (self.output / "manifest.json").write_text(
            json.dumps(
                {
                    "total_attempts": 120,
                    "estimated_credits": 12_000,
                    "wallets": {
                        wallet: {
                            "status": "NOT_STARTED",
                            "pages_cached": 0,
                            "last_cursor": None,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        client = FakeHttp([])

        with self.assertRaisesRegex(PilotFatalError, "budget exhausted"):
            self.run_pilot_case(client)

        manifest = json.loads(
            (self.output / "manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual(client.calls, [])
        self.assertEqual(manifest["total_attempts"], 120)
        self.assertEqual(manifest["estimated_credits"], 12_000)

    def test_successful_page_is_cached_and_reused_after_restart(self):
        wallet = "wallet-one"
        page = [{"signature": "only-page"}]
        self.write_candidates([wallet])
        first_client = FakeHttp([FakeResponse(payload=page)])

        self.run_pilot_case(first_client)

        cache_path = self.output / "cache" / wallet / "page_1.json"
        self.assertEqual(
            json.loads(cache_path.read_text(encoding="utf-8")),
            page,
        )
        second_client = FakeHttp([])
        _, manifest = self.run_pilot_case(second_client)
        self.assertEqual(second_client.calls, [])
        self.assertEqual(manifest["wallets"][wallet]["status"], "COMPLETE")

    def test_401_402_and_403_abort_entire_run(self):
        for status in (401, 402, 403):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                candidates = root / "candidates.txt"
                candidates.write_text("wallet-one\nwallet-two\n", encoding="utf-8")
                client = FakeHttp([FakeResponse(status_code=status)])

                with self.assertRaisesRegex(PilotFatalError, f"HTTP {status}"):
                    run_pilot(
                        candidates,
                        root / "output",
                        request_get=client,
                        sleeper=lambda _seconds: None,
                    )

                self.assertEqual(len(client.calls), 1)

    def test_second_429_is_fatal(self):
        self.write_candidates(["wallet-one", "wallet-two"])
        client = FakeHttp(
            [
                FakeResponse(status_code=429),
                FakeResponse(status_code=429),
            ]
        )

        with self.assertRaisesRegex(PilotFatalError, "Second HTTP 429"):
            self.run_pilot_case(client)

        self.assertEqual(len(client.calls), 2)

    def test_repeated_5xx_is_fatal(self):
        self.write_candidates(["wallet-one", "wallet-two"])
        client = FakeHttp(
            [
                FakeResponse(status_code=500),
                FakeResponse(status_code=503),
            ]
        )

        with self.assertRaisesRegex(PilotFatalError, "Repeated HTTP 5xx"):
            self.run_pilot_case(client)

        self.assertEqual(len(client.calls), 2)

    def test_api_key_is_absent_from_output_and_files(self):
        self.write_candidates(["wallet-one"])
        secret = backtester.HELIUS_API_KEY
        client = FakeHttp([RuntimeError(f"url contained api-key={secret}")])
        stdout = io.StringIO()
        stderr = io.StringIO()

        with redirect_stdout(stdout), redirect_stderr(stderr):
            with self.assertRaises(PilotFatalError) as caught:
                self.run_pilot_case(client)

        visible = stdout.getvalue() + stderr.getvalue() + str(caught.exception)
        file_text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in self.root.rglob("*")
            if path.is_file()
        )
        self.assertNotIn(secret, visible)
        self.assertNotIn(secret, file_text)


if __name__ == "__main__":
    unittest.main()
