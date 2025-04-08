"""This program computes the implied volatility given market price and model price. This program is revised based on
https://stackoverflow.com/questions/61289020/fast-implied-volatility-calculation-in-python"""

import concurrent.futures

import numpy as np
import psutil
from scipy.stats import norm

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def bs_call(S, K, T, r, vol):
    d1 = (np.log(S / K) + (r + 0.5 * vol**2) * T) / (vol * np.sqrt(T))
    d2 = d1 - vol * np.sqrt(T)
    return S * norm.cdf(d1) - np.exp(-r * T) * K * norm.cdf(d2)


def bs_vega(S, K, T, r, sigma):
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    return S * norm.pdf(d1) * np.sqrt(T)


def find_vol(target_value, S, K, T, r):
    MAX_ITERATIONS = 200
    PRECISION = 1.0e-5
    sigma = 0.5
    for _ in range(0, MAX_ITERATIONS):
        price = bs_call(S, K, T, r, sigma)
        vega = bs_vega(S, K, T, r, sigma)
        diff = target_value - price  # our root
        if abs(diff) < PRECISION:
            return sigma
        sigma = sigma + diff / vega  # f(x) / f'(x)
    return sigma  # value wasn't found, return best guess so far


def wrapper(find_vol_function, params):
    return list(map(find_vol_function, *params))


def main():
    n_workers = psutil.cpu_count()
    if n_workers is None:
        n_workers = 3

    cluster = SchedulerClusterCombo(n_workers=n_workers)
    client = Client(address=cluster.get_address())

    # If we were to calculate a single implied volatility, this is how we would write this program
    # S = 100
    # K = 100
    # T = 11
    # r = 0.01
    # vol = 0.25
    #
    # V_market = bs_call(S, K, T, r, vol)
    # implied_vol = find_vol(V_market, S, K, T, r)
    #
    # print ('Implied vol: %.2f%%' % (implied_vol * 100))
    # print ('Market price = %.2f' % V_market)
    # print ('Model price = %.2f' % bs_call(S, K, T, r, implied_vol))

    futs = []
    for _ in range(n_workers):
        size = 10000
        S = np.random.randint(100, 200, size)
        K = S * 1.25
        T = np.ones(size)
        R = np.random.randint(0, 3, size) / 100
        vols = np.random.randint(15, 50, size) / 100
        prices = bs_call(S, K, T, R, vols)
        params = np.vstack((prices, S, K, T, R))
        futs.append(client.submit(wrapper, find_vol, params))

    # User may wish to gather the results and do more interesting stuff. For demonstrating purposes, we just wait.
    concurrent.futures.wait(futs)


if __name__ == "__main__":
    main()
