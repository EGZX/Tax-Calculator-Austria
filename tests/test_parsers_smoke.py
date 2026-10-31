"""Parser tests: synthetic CSVs mirroring the real export formats, plus smoke
tests against local exports (skipped when ``exports/`` is absent)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from conftest import EXPORTS

from tax_calc_at.engine.rules import BrokerConfig
from tax_calc_at.model import (
    FLAG_CA_UNCLASSIFIED,
    FLAG_PAYMENT_IN_LIEU,
    FLAG_ROC_LABEL,
    FLAG_WHT_COUNTRY_UNCERTAIN,
    FLAG_WHT_NOT_DISCLOSED,
    AssetClass,
    CutoffViolationError,
    ParserError,
    Severity,
    TxType,
)
from tax_calc_at.parsers import ibkr_flex, parse_file, scalable, trade_republic, trading212

D = Decimal


def _codes(tx):
    return {f.code for f in tx.flags}


# ===================================================================== IBKR
TRADE_HDR = (
    "ClientAccountID,CurrencyPrimary,AssetClass,SubCategory,Symbol,Description,ISIN,"
    "IssuerCountryCode,Multiplier,DateTime,TradeDate,TransactionType,Quantity,TradePrice,"
    "Proceeds,Taxes,IBCommission,IBCommissionCurrency,Notes/Codes,Buy/Sell,TransactionID,"
    "LevelOfDetail\n"
)
CASH_HDR = (
    "ClientAccountID,CurrencyPrimary,AssetClass,SubCategory,Symbol,Description,ISIN,"
    "IssuerCountryCode,Date/Time,SettleDate,Amount,Type,TransactionID,ActionID,LevelOfDetail\n"
)


def _ibkr(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "flex.csv"
    p.write_text(body, encoding="utf-8")
    return p


def test_ibkr_dividends_are_gross_and_withholding_signed(tmp_path):
    p = _ibkr(tmp_path, CASH_HDR
              + "U1,DKK,STK,COMMON,NOVOB,NOVO B(DK0062498333) CASH DIVIDEND DKK 5.00 PER SHARE"
                " (Ordinary Dividend),DK0062498333,DK,20230322;202000,20230322,100,Dividends,"
                "11,A1,DETAIL\n"
              + "U1,DKK,STK,COMMON,NOVOB,NOVO B(DK0062498333) CASH DIVIDEND DKK 5.00 PER SHARE"
                " - DK TAX,DK0062498333,DK,20230322;202000,20230322,-27,Withholding Tax,"
                "12,A1,DETAIL\n")
    (div,) = ibkr_flex.parse(p)[0]
    assert div.tx_type is TxType.DIVIDEND_CASH
    assert div.gross_native == D("100") and div.dividend_is_net is False
    assert div.tax_withheld_native == D("27") and div.withholding_country == "DK"


def test_ibkr_withholding_reversal_nets_out(tmp_path):
    """Corrections arrive as +reversal / −rebooking: summing abs() was wrong."""
    rows = [
        ("8.64", "Dividends", "CASH DIVIDEND USD 0.24 (Ordinary Dividend)"),
        ("-0.86", "Withholding Tax", "CASH DIVIDEND USD 0.24 - US TAX"),
        ("0.86", "Withholding Tax", "CASH DIVIDEND USD 0.24 - US TAX"),
        ("-0.08", "Withholding Tax", "CASH DIVIDEND USD 0.24 - US TAX"),
    ]
    body = CASH_HDR + "".join(
        f"U1,USD,STK,COMMON,AGG,AGG(US4642872265) {d},US4642872265,US,20210205;202000,"
        f"20210205,{a},{t},{i},A9,DETAIL\n"
        for i, (a, t, d) in enumerate(rows)
    )
    (div,) = ibkr_flex.parse(_ibkr(tmp_path, body))[0]
    assert div.tax_withheld_native == D("0.08")


def test_ibkr_wht_country_from_description_and_adr(tmp_path):
    body = CASH_HDR + (
        "U1,USD,STK,ADR,TSM,TSM(US8740391003) CASH DIVIDEND USD 0.8 (Ordinary Dividend),"
        "US8740391003,TW,20260410;202000,20260410,8,Dividends,1,A2,DETAIL\n"
        "U1,USD,STK,ADR,TSM,TSM(US8740391003) CASH DIVIDEND USD 0.8 - TW TAX,"
        "US8740391003,TW,20260410;202000,20260410,-1.68,Withholding Tax,2,A2,DETAIL\n"
        "U1,USD,STK,ADR,BBD,BBD(US0594603039) CASH DIVIDEND USD 0.1 (Ordinary Dividend),"
        "US0594603039,US,20260410;202000,20260410,5,Dividends,3,A3,DETAIL\n"
        "U1,USD,STK,ADR,BBD,BBD(US0594603039) CASH DIVIDEND USD 0.1 - XX,"
        "US0594603039,US,20260410;202000,20260410,-0.75,Withholding Tax,4,A3,DETAIL\n"
    )
    tsm, bbd = sorted(ibkr_flex.parse(_ibkr(tmp_path, body))[0], key=lambda t: t.symbol)[::-1]
    assert tsm.withholding_country == "TW" and FLAG_WHT_COUNTRY_UNCERTAIN not in _codes(tsm)
    assert FLAG_WHT_COUNTRY_UNCERTAIN in _codes(bbd)


def test_ibkr_interest_withholding_attached_and_pil_flagged(tmp_path):
    body = CASH_HDR + (
        "U1,EUR,,,,EUR CREDIT INT FOR DEC-2025,,,20260106,20260106,0.11,"
        "Broker Interest Received,1,,DETAIL\n"
        "U1,EUR,,,,WITHHOLDING @ 20% ON CREDIT INT FOR DEC-2025,,,20260106,20260106,"
        "-0.02,Withholding Tax,2,,DETAIL\n"
        "U1,USD,STK,COMMON,X,X(US0378331005) PAYMENT IN LIEU OF DIVIDEND,US0378331005,US,"
        "20260106;202000,20260106,3,Payment In Lieu Of Dividends,3,A5,DETAIL\n"
    )
    txns = ibkr_flex.parse(_ibkr(tmp_path, body))[0]
    interest = next(t for t in txns if t.tx_type is TxType.INTEREST)
    assert interest.tax_withheld_native == D("0.02") and interest.dividend_is_net is False
    pil = next(t for t in txns if t.tx_type is TxType.DIVIDEND_CASH)
    assert FLAG_PAYMENT_IN_LIEU in _codes(pil)


def test_ibkr_roc_label_is_still_a_dividend(tmp_path):
    body = CASH_HDR + (
        "U1,USD,STK,COMMON,X,X(US0378331005) CASH DIVIDEND 1 USD PER SHARE (Return of Capital),"
        "US0378331005,US,20260106;202000,20260106,10,Dividends,1,A6,DETAIL\n"
    )
    (tx,) = ibkr_flex.parse(_ibkr(tmp_path, body))[0]
    assert tx.tx_type is TxType.DIVIDEND_CASH and FLAG_ROC_LABEL in _codes(tx)


def test_ibkr_trades_fx_and_commission(tmp_path):
    body = TRADE_HDR + (
        "U1,USD,STK,ETF,VOO,VANGUARD S&P500,US9229083632,US,1,20230315;103359,20230315,"
        "ExchTrade,5,500,-2500,0,-1,USD,,BUY,1,EXECUTION\n"
        "U1,USD,CASH,,EUR.USD,EUR.USD,,,1,20230315;103400,20230315,ExchTrade,-2150,1.163,"
        "2500.45,0,0,EUR,AFx,SELL,2,EXECUTION\n"
        "U1,USD,STK,COMMON,AMZN,AMAZON,US0231351067,US,1,20230315;103500,20230315,"
        "ExchTrade,1,200,-200,0,-1.5,EUR,,BUY,3,EXECUTION\n"
    )
    txns = ibkr_flex.parse(_ibkr(tmp_path, body))[0]
    etf = next(t for t in txns if t.symbol == "VOO")
    assert etf.asset_class is AssetClass.ETF and etf.fee_native == D("1")
    fx = next(t for t in txns if t.tx_type is TxType.FX_CONVERSION)
    assert (fx.currency_native, fx.gross_native, fx.counter_currency, fx.counter_amount) == (
        "USD", D("2500.45"), "EUR", D("-2150"))
    fee = next(t for t in txns if t.tx_type is TxType.FEE)
    assert fee.currency_native == "EUR" and fee.gross_native == D("-1.5")


@pytest.mark.parametrize("cls", ["OPT", "FUT", "CFD", "FOP"])
def test_ibkr_rejects_non_securitised_derivatives(tmp_path, cls):
    body = TRADE_HDR + (
        f"U1,USD,{cls},,X,X,,,100,20230315;110000,20230315,ExchTrade,1,5,-500,0,-1,USD,,"
        "BUY,1,EXECUTION\n"
    )
    with pytest.raises(ParserError, match="progressive tariff"):
        ibkr_flex.parse(_ibkr(tmp_path, body))


def test_ibkr_rejects_multiplier_and_cancellations(tmp_path):
    body = TRADE_HDR + (
        "U1,USD,STK,COMMON,X,X,US0378331005,US,1,20251209;1,20251209,ExchTrade,1,5,-5,0,0,"
        "USD,,BUY (Ca.),1,EXECUTION\n"
    )
    with pytest.raises(ParserError, match="Buy/Sell"):
        ibkr_flex.parse(_ibkr(tmp_path, body))


def test_ibkr_corporate_actions_split_and_issue_change(tmp_path):
    hdr = ("ClientAccountID,CurrencyPrimary,AssetClass,SubCategory,Symbol,Description,ISIN,"
           "Date/Time,Quantity,Amount,Proceeds,Value,Type,TransactionID,ActionID\n")
    body = hdr + (
        "U1,USD,STK,COMMON,GME,GME SPLIT 4 FOR 1,US36467W1099,20220722;202500,3,0,0,0,FS,1,C1\n"
        "U1,USD,STK,COMMON,OLD,OLD ISIN CHANGE,US0404131064,20241204;202500,-4,0,0,0,IC,2,C2\n"
        "U1,USD,STK,COMMON,NEW,NEW ISIN CHANGE,US0404132054,20241204;202500,4,0,0,0,IC,3,C2\n"
    )
    txns = ibkr_flex.parse(_ibkr(tmp_path, body))[0]
    split = next(t for t in txns if t.isin == "US36467W1099")
    assert split.tx_type is TxType.SPLIT and split.quantity == 3
    out = next(t for t in txns if t.isin == "US0404131064")
    inn = next(t for t in txns if t.isin == "US0404132054")
    assert (out.tx_type, inn.tx_type) == (TxType.EXCHANGE_OUT, TxType.EXCHANGE_IN)
    assert out.link_id == inn.link_id and out.quantity == 4


def test_ibkr_stock_dividend_needs_declaration(tmp_path):
    """SD may be bonus shares out of profits (income) or a capital increase
    from company funds (neutral); the export does not say which."""
    hdr = ("ClientAccountID,CurrencyPrimary,AssetClass,SubCategory,Symbol,Description,ISIN,"
           "Date/Time,Quantity,Amount,Proceeds,Value,Type,TransactionID,ActionID\n")
    body = hdr + ("U1,HKD,STK,COMMON,1211,1211 STOCK DIVIDEND,CNE100000296,20250610;202500,"
                  "8,0,0,0,SD,1,C1\n")
    (tx,) = ibkr_flex.parse(_ibkr(tmp_path, body))[0]
    assert tx.tx_type is TxType.SPLIT and tx.quantity == 8
    flag = next(f for f in tx.flags if f.code == FLAG_CA_UNCLASSIFIED)
    assert flag.severity is Severity.ERROR


def test_ibkr_full_fixture_parses():
    p = EXPORTS / "IBKR" / "FlexQ_last365days.csv"
    if not p.exists():
        pytest.skip("fixture missing")
    txns, report = ibkr_flex.parse(p)
    types = {t.tx_type for t in txns}
    assert {TxType.BUY, TxType.SELL, TxType.DIVIDEND_CASH, TxType.FX_CONVERSION} <= types
    divs = [t for t in txns if t.tx_type is TxType.DIVIDEND_CASH]
    assert all(t.dividend_is_net is False for t in divs)


# ================================================================= Trading 212
T212_HDR = (
    "Action,Time,ISIN,Ticker,Name,Notes,ID,No. of shares,Price / share,"
    "Currency (Price / share),Exchange rate,Result,Currency (Result),Total,Currency (Total),"
    "Withholding tax,Currency (Withholding tax),Currency conversion fee,"
    "Currency (Currency conversion fee)\n"
)


def _t212(tmp_path, body, header=T212_HDR):
    p = tmp_path / "t212.csv"
    p.write_text(header + body, encoding="utf-8")
    return trading212.parse(p)[0]


def test_t212_dividend_gross_from_net_per_share_plus_wht(tmp_path):
    txns = _t212(tmp_path,
                 "Dividend (Dividend),2025-03-03 10:00:00,US0378331005,AAPL,Apple,,D1,10,0.2125,"
                 "USD,1.05,,,2.02,EUR,0.375,USD,,\n")
    div = next(t for t in txns if t.tx_type is TxType.DIVIDEND_CASH)
    assert div.currency_native == "USD" and div.gross_native == D("2.5")
    assert div.tax_withheld_native == D("0.375") and div.dividend_is_net is False
    fx = next(t for t in txns if t.tx_type is TxType.FX_CONVERSION)
    assert fx.gross_native == D("-2.125")


def test_t212_missing_wht_column_flags(tmp_path):
    hdr = ("Action,Time,ISIN,Ticker,Name,Notes,ID,No. of shares,Price / share,"
           "Currency (Price / share),Exchange rate,Total,Currency (Total)\n")
    txns = _t212(tmp_path, "Dividend (Ordinary),2024-06-01 12:00:00,US0378331005,AAPL,Apple,,"
                           "D1,10,0.2,USD,1.08,1.85,EUR\n", header=hdr)
    div = next(t for t in txns if t.tx_type is TxType.DIVIDEND_CASH)
    assert FLAG_WHT_NOT_DISCLOSED in _codes(div)


def test_t212_usd_trade_books_conversion_at_t212_rate(tmp_path):
    txns = _t212(tmp_path, "Market buy,2023-03-15 15:00:00,US7427181091,PG,Procter & Gamble,,B1,"
                           "0.5,150.00,USD,1.06,,EUR,70.75,EUR,,,0.02,EUR\n")
    buy = next(t for t in txns if t.tx_type is TxType.BUY)
    assert buy.currency_native == "USD" and buy.gross_native == -D("0.5") * D("150.00")
    fx = next(t for t in txns if t.tx_type is TxType.FX_CONVERSION)
    assert fx.counter_currency == "EUR"
    assert fx.counter_amount == -(D("0.5") * D("150.00") / D("1.06"))
    assert any(t.tx_type is TxType.FEE and t.gross_native == D("-0.02") for t in txns)


def test_t212_free_share_promo_is_income(tmp_path):
    txns = _t212(tmp_path, "Deposit,2023-03-15 09:00:00,,,,Free Shares Promotion,P1,,,,,,,"
                           "10.00,EUR,,,,\n")
    assert txns[0].tx_type is TxType.PROMO_BONUS


def test_t212_split_close_open_pair(tmp_path):
    txns = _t212(tmp_path,
                 "Stock split close,2025-12-09 06:00:00,GB00B10RZP78,ULVR,Unilever,,S1,0.44143405,"
                 "45.0,GBP,,,,19.87,GBP,,,,\n"
                 "Stock split open,2025-12-09 06:00:00,GB00B10RZP78,ULVR,Unilever,,S2,0.39238582,"
                 "50.6,GBP,,,,19.87,GBP,,,,\n")
    split = next(t for t in txns if t.tx_type is TxType.SPLIT)
    assert split.quantity == D("0.39238582") - D("0.44143405")


def test_t212_ids_are_not_unique_keys(tmp_path):
    txns = _t212(tmp_path, "Deposit,2025-01-01 10:00:00,,,,x,SAME,,,,,,,10,EUR,,,,\n"
                           "Withdrawal,2025-01-02 10:00:00,,,,x,SAME,,,,,,,-5,EUR,,,,\n")
    assert txns[0].dedup_key() != txns[1].dedup_key()


def test_t212_unknown_action_raises(tmp_path):
    with pytest.raises(ParserError, match="Unknown or unsupported"):
        _t212(tmp_path, "Dividend adjustment,2025-01-01 10:00:00,US0378331005,,,,X,,,,,,,1,EUR,,,,\n")


def test_t212_fixture_parses():
    p = EXPORTS / "Trading 212" / "from_2024-12-08_to_2025-12-08_MTc2NTIwNjI4MTkyNA.csv"
    if not p.exists():
        pytest.skip("fixture missing")
    txns, _ = trading212.parse(p)
    assert any(t.currency_native == "USD" and t.tx_type is TxType.BUY for t in txns)
    assert any(t.tx_type is TxType.FX_CONVERSION for t in txns)


# ================================================================= Scalable
SC_HDR = "date;time;status;reference;description;assetType;type;isin;shares;price;amount;fee;tax;currency\n"


def _sc(tmp_path, body):
    p = tmp_path / "sc.csv"
    p.write_text(SC_HDR + body, encoding="utf-8")
    return scalable.parse(p)[0]


def test_scalable_isin_change_becomes_exchange(tmp_path):
    """Regression: −old/+new legs became SPLITs, stranding the basis."""
    txns = _sc(tmp_path,
               "2024-12-04;01:00:00;Executed;WWUM 1;NEW;Security;Corporate action;US0404132054;12;"
               "62,50;750,00;;;EUR\n"
               "2024-12-04;01:00:00;Executed;WWUM 2;OLD;Security;Corporate action;US0404131064;-3;"
               "250,00;-750,00;;;EUR\n")
    by = {t.isin: t for t in txns}
    assert by["US0404131064"].tx_type is TxType.EXCHANGE_OUT and by["US0404131064"].quantity == 3
    assert by["US0404132054"].tx_type is TxType.EXCHANGE_IN
    assert by["US0404131064"].link_id == by["US0404132054"].link_id


def test_scalable_same_isin_legs_net_to_one_split(tmp_path):
    txns = _sc(tmp_path,
               "2024-06-10;02:00:00;Executed;WWUM 1;NVIDIA;Security;Corporate action;US67066G1040;-2;"
               "1.200,00;-2.400,00;;;EUR\n"
               "2024-06-10;02:00:00;Executed;WWUM 2;NVIDIA;Security;Corporate action;US67066G1040;20;"
               "120,00;2.400,00;;;EUR\n"
               "2025-09-10;02:00:00;Executed;CANCEL-WWUM 3;BYD;Security;Corporate action;CNE100000296;"
               "-8;0,00;0,00;;;EUR\n"
               "2025-09-10;02:00:00;Executed;WWUM 4;BYD;Security;Corporate action;CNE100000296;8;"
               "0,00;0,00;;;EUR\n")
    splits = [t for t in txns if t.tx_type is TxType.SPLIT]
    assert [(t.isin, t.quantity) for t in splits] == [("US67066G1040", D("18"))]


def test_scalable_split_vs_added_shares(tmp_path):
    """Old shares out + new in is a split; shares only added (BYD 2025 bonus
    issue, US splits effected as stock dividends) must be declared."""
    txns = _sc(tmp_path,
               "2024-06-10;02:00:00;Executed;WWUM 1;NVIDIA;Security;Corporate action;US67066G1040;-2;"
               "1.200,00;-2.400,00;;;EUR\n"
               "2024-06-10;02:00:00;Executed;WWUM 2;NVIDIA;Security;Corporate action;US67066G1040;20;"
               "120,00;2.400,00;;;EUR\n"
               "2025-06-10;02:00:00;Executed;WWUM 3;BYD;Security;Corporate action;CNE100000296;12;"
               "0,00;0,00;;;EUR\n"
               "2025-06-10;02:00:00;Executed;WWUM 4;BYD;Security;Corporate action;CNE100000296;8;"
               "0,00;0,00;;;EUR\n")
    by = {t.isin: t for t in txns if t.tx_type is TxType.SPLIT}
    assert FLAG_CA_UNCLASSIFIED not in _codes(by["US67066G1040"])
    assert "corporate_action.split" in _codes(by["US67066G1040"])
    assert by["CNE100000296"].quantity == D("20")
    assert FLAG_CA_UNCLASSIFIED in _codes(by["CNE100000296"])


def test_scalable_distribution_and_trade_tax(tmp_path):
    txns = _sc(tmp_path,
               '2023-03-20;01:00:00;Executed;"WWEK 1";"Xylem";Cash;Distribution;US98419M1009;;;'
               "8,50;0,00;1,50;EUR\n"
               '2023-03-20;01:00:00;Executed;"WWEK 2";"Taiwan Semiconductor Manufact. ADR";Cash;'
               "Distribution;US8740391003;;;4,00;0,00;1,00;EUR\n"
               '2023-03-15;10:00:00;Executed;"S1";"Aena";Security;Buy;ES0105046009;3;150,00;-450,00;'
               "0,00;0,90;EUR\n"
               '2023-06-30;01:00:00;Executed;"I1";"KKT-Abschluss";Cash;Interest;;;;2,00;0,00;'
               "100,00;EUR\n"
               '2023-08-14;10:00:00;Executed;"S2";"iShares Core MSCI EM IMI UCITS ETF";Security;Buy;'
               "IE00BKM4GZ66;1;30,00;-30,00;0,99;0,00;EUR\n")
    xyl, tsm, aena, interest, etf = txns
    assert xyl.dividend_is_net is True and xyl.tax_withheld_native == D("1.50")
    assert FLAG_WHT_COUNTRY_UNCERTAIN in _codes(tsm)
    assert aena.fee_native == D("0.90") and aena.tax_withheld_native == 0
    assert interest.tax_withheld_native == 0 and "scalable.interest_tax_ignored" in _codes(interest)
    assert etf.asset_class is AssetClass.ETF


def test_scalable_derivatives_recognised_by_name(tmp_path):
    """Regression: Scalable warrants were filed as shares (KZ 994/892)."""
    txns = _sc(tmp_path,
               '2024-03-01;10:00:00;Executed;"S1";"Adobe Call 800,00 $ Optionsschein HSBC";Security;'
               "Buy;DE000HS3X9G9;100;5,00;-500,00;0,99;0,00;EUR\n"
               '2024-03-01;10:00:00;Executed;"S2";"WisdomTree NASDAQ 100 3x Daily Leveraged";Security;'
               "Buy;IE00BLRPRL42;1;50,00;-50,00;0,00;0,00;EUR\n"
               '2024-03-01;10:00:00;Executed;"S3";"Amundi Nasdaq-100 Daily (2x) Leveraged UCITS ETF Acc";'
               "Security;Buy;FR0010342592;1;50,00;-50,00;0,00;0,00;EUR\n"
               '2024-03-01;10:00:00;Executed;"S4";"Procter & Gamble";Security;'
               "Buy;US7427181091;1;150,00;-150,00;0,00;0,00;EUR\n")
    assert [t.asset_class for t in txns] == [
        AssetClass.DERIVATIVE, AssetClass.DERIVATIVE, AssetClass.ETF, AssetClass.STOCK]


def test_scalable_security_transfer_legs(tmp_path):
    txns = _sc(tmp_path,
               "2023-05-02;01:00:00;Executed;WWUM-1;Microsoft;Security;Security transfer;US5949181045;"
               "-3;300,00;-900,00;;;EUR\n"
               "2023-05-03;01:00:00;Executed;SWITCH-1;Microsoft;Security;Security transfer;"
               "US5949181045;3;302,00;906,00;;;EUR\n")
    assert [t.tx_type for t in txns] == [TxType.MIGRATION_OUT, TxType.MIGRATION_IN]
    assert txns[1].price_native == D("302.00")


def test_scalable_zero_share_transfer_raises(tmp_path):
    with pytest.raises(ParserError, match="zero shares"):
        _sc(tmp_path, "2023-05-02;01:00:00;Executed;B;M;Security;Security transfer;US5949181045;"
                      "0;0;0;;;EUR\n")


def test_scalable_fixture_parses():
    p = EXPORTS / "Scalable Capital" / "2024 Scalable Transaktionen.csv"
    if not p.exists():
        pytest.skip("fixture missing")
    txns, report = scalable.parse(p)
    assert {TxType.BUY, TxType.SELL, TxType.DIVIDEND_CASH} <= {t.tx_type for t in txns}


# ============================================================ Trade Republic
TR_HDR = (
    '"datetime","date","account_type","category","type","asset_class","name","symbol","shares",'
    '"price","amount","fee","tax","currency","original_amount","original_currency","fx_rate",'
    '"description","transaction_id","counterparty_name","counterparty_iban","payment_reference",'
    '"mcc_code"\n'
)


def _tr(tmp_path, body):
    p = tmp_path / "tr.csv"
    p.write_text(TR_HDR + body, encoding="utf-8")
    return p


def test_tr_dividend_is_gross_and_valued_in_payment_currency(tmp_path):
    p = _tr(tmp_path,
            '"2025-07-17T10:00:00Z","2025-07-17","DEFAULT","CASH","DIVIDEND","STOCK","GNL",'
            '"US3793782018","","","29.58","","-4.44","EUR","34.51","USD","0.857265","","d1","","","",""\n')
    div, fx = trade_republic.parse(p)[0]
    assert div.currency_native == "USD" and div.gross_native == D("34.51")
    assert div.dividend_is_net is False
    assert div.tax_withheld_native == D("4.44") / D("0.857265")
    assert fx.tx_type is TxType.FX_CONVERSION and fx.counter_amount == D("25.14")


def test_tr_migration_direction_from_shares(tmp_path):
    """Regression: direction was read from the empty `amount`."""
    p = _tr(tmp_path,
            '"2023-03-15T00:05:00Z","2023-03-15","DEFAULT","DELIVERY","MIGRATION","FUND","Gold",'
            '"IE00B4ND3602","-10.5","40.00","","","","EUR","","","","","m1","","","",""\n'
            '"2023-03-15T00:05:01Z","2023-03-15","DEFAULT","DELIVERY","MIGRATION","FUND","Gold",'
            '"IE00B4ND3602","10.5","40.00","","","","EUR","","","","","m2","","","",""\n')
    txns = trade_republic.parse(p)[0]
    assert [t.tx_type for t in txns] == [TxType.MIGRATION_OUT, TxType.MIGRATION_IN]


def test_tr_stockperk_and_saveback_are_cashback(tmp_path):
    p = _tr(tmp_path,
            '"2023-03-15T10:00:00Z","2023-03-15","DEFAULT","CASH","STOCKPERK","STOCK","Tesla",'
            '"US88160R1014","","","10.00","","","EUR","","","","Stockperk","s1","","","",""\n'
            '"2023-03-15T10:05:00Z","2023-03-15","DEFAULT","TRADING","BUY","STOCK","Tesla",'
            '"US88160R1014","0.05","200.00","-10.00","","","EUR","","","","","b1","","","",""\n'
            '"2023-04-03T09:00:00Z","2023-04-03","DEFAULT","CASH","BENEFITS_SAVEBACK","FUND","N100",'
            '"IE00BLRPRL42","","","3.00","","","EUR","","","","Saveback","s2","","","",""\n'
            '"2023-04-03T09:30:00Z","2023-04-03","DEFAULT","TRADING","BUY","DERIVATIVE","Faktor",'
            '"DE000SW19FA0","500","0.50","-250.00","-1.00","","EUR","","","","","b2","","","",""\n')
    perk, buy, saveback, deriv = trade_republic.parse(p)[0]
    assert perk.tx_type is TxType.CASHBACK and saveback.tx_type is TxType.CASHBACK
    assert buy.tx_type is TxType.BUY and buy.gross_native == D("-10.00")  # full cost basis
    assert deriv.asset_class is AssetClass.DERIVATIVE and deriv.fee_native == D("1.00")


def test_steuereinfach_cutoff_applies_to_any_broker(tmp_path):
    p = _tr(tmp_path,
            '"2025-05-01T10:00:00Z","2025-05-01","DEFAULT","TRADING","BUY","STOCK","Foo",'
            '"US88160R1014","1","100.0","-100.00","","","EUR","","","","","x","","","",""\n')
    cfg = BrokerConfig(parser="trade_republic", display_name="TR", credit_institution=True,
                       steuereinfach_from=date(2025, 4, 29))
    txns, report = parse_file(cfg, p)
    assert txns[0].tx_type is TxType.IGNORED and report.rows_emitted == 0
    with pytest.raises(CutoffViolationError):
        parse_file(cfg, p, strict_cutoff=True)


def test_tr_fixture_parses():
    p = EXPORTS / "Trade Republic" / "TR Transaction export 2023-2025.csv"
    if not p.exists():
        pytest.skip("fixture missing")
    types = {t.tx_type for t in trade_republic.parse(p)[0]}
    assert {TxType.BUY, TxType.SELL, TxType.INTEREST, TxType.CASHBACK} <= types
