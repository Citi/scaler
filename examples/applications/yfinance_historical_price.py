"""This program gets closing price of a given ticker and start dates. This program is revised based on
https://stackoverflow.com/a/77342764"""

import datetime
from typing import Optional

import yfinance as yf

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def get_option_data(stock_symbol: str, expiration_date: Optional[str], option_type: str, strike: float):
    stock = yf.Ticker(stock_symbol)
    option_chain = stock.option_chain(expiration_date)
    options = getattr(option_chain, "calls" if option_type.startswith("call") else "puts")
    option_data = options[options["strike"] == strike]
    return option_data


def get_option_history_data(contract_symbol, days_before_expiration: int = 30):
    option = yf.Ticker(contract_symbol)
    option_info = option.info
    option_expiration_date = datetime.datetime.fromtimestamp(option_info["expireDate"])

    start_date = option_expiration_date - datetime.timedelta(days=days_before_expiration)
    option_history = option.history(start=start_date)
    return option_history


def get_option_close_prices_with_strike(strike):
    stock_symbol = "AAPL"
    expiration_date = None  # User may wish to specify expiration_date
    days_before_expiration = 30
    option_type = "call"

    res = []
    option_data = get_option_data(stock_symbol, expiration_date, option_type, strike)
    for _, od in option_data.iterrows():
        contract_symbol = od["contractSymbol"]
        option_history = get_option_history_data(contract_symbol, days_before_expiration)
        first_option_history = option_history.iloc[0]
        first_option_history_date = option_history.index[0]
        first_option_history_close = first_option_history["Close"]
        res.append((contract_symbol, first_option_history_close, first_option_history_date))
    return res


def main():
    cluster = SchedulerClusterCombo(n_workers=2)
    client = Client(address=cluster.get_address())

    # With scaler, 32.382 s
    results = client.map(get_option_close_prices_with_strike, [(strike,) for strike in range(170, 250)])
    for lists_of_closing_dates in results:
        if lists_of_closing_dates == []:
            continue
        for contract_symbol, first_option_history_close, first_option_history_date in lists_of_closing_dates:
            print(
                f"For {contract_symbol}, the closing price was ${first_option_history_close:.2f} on "
                f"{first_option_history_date}."
            )


if __name__ == "__main__":
    main()
