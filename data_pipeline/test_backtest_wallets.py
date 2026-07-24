import copy
import io
import os
import unittest

os.environ.setdefault("HELIUS_API_KEY", "offline-test")

from data_pipeline.backtest_wallets import configure_stdout_utf8, parse_trade


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


class ParseTradeTests(unittest.TestCase):
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
        tx = trade_fixture(-800_328_221)

        self.assertIsNone(parse_trade(tx, WALLET))

    def test_transaction_fee_alone_is_not_a_buy(self):
        tx = trade_fixture(-2_000_000)
        tx["fee"] = 2_000_000
        tx["feePayer"] = WALLET
        tx["tokenTransfers"][0]["fromUserAccount"] = "counterparty"
        tx["tokenTransfers"][0]["toUserAccount"] = WALLET

        self.assertIsNone(parse_trade(tx, WALLET))

    def test_ambiguous_token_direction_fails_closed(self):
        tx = trade_fixture()
        tx["tokenTransfers"].append(copy.deepcopy(tx["tokenTransfers"][0]))

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


if __name__ == "__main__":
    unittest.main()
