import datetime

import yfinance as yf

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def get_option_data(stock_symbol: str, expiration_date: str | None, option_type: str, strike: float):
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


def main():
    cluster = SchedulerClusterCombo(n_workers=2)
    client = Client(address=cluster.get_address())

    # Example:
    stock_symbol = "AAPL"
    expiration_date = "2023-10-27"
    expiration_date = None
    option_type = "call"
    # strike = 170.0

    def func(strike):
        option_data = get_option_data(stock_symbol, expiration_date, option_type, strike)
        for _, od in option_data.iterrows():
            contract_symbol = od["contractSymbol"]
            option_history = get_option_history_data(contract_symbol)
            first_option_history = option_history.iloc[0]
            first_option_history_date = option_history.index[0]
            first_option_history_close = first_option_history["Close"]
            return (contract_symbol, first_option_history_close, first_option_history_date)

    # Original version, without scaler. Wall time: 44.487 s
    # for strike in range(170, 250):
    #     res = func(strike)
    #     if res is None:
    #         continue
    #     contract_symbol = res[0]
    #     first_option_history_close = res[1]
    #     first_option_history_date = res[2]
    #     print(f"For {contract_symbol}, the closing price was ${first_option_history_close:.2f} on "
    #           f"{first_option_history_date}.")

    # With scaler, 32.382 s
    results = client.map(func, [(strike,) for strike in range(170, 250)])
    for res in results:
        if res is None:
            continue
        contract_symbol = res[0]
        first_option_history_close = res[1]
        first_option_history_date = res[2]

        print(
            f"For {contract_symbol}, the closing price was ${first_option_history_close:.2f} on "
            f"{first_option_history_date}."
        )


if __name__ == "__main__":
    main()
