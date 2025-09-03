"""
This module provides classes for pricing European call and put options using the
Black-Scholes-Merton model and calculating the associated Greeks.

It uses the yfinance library to fetch live market data, including the underlying
asset's price and the option's implied volatility from the options chain. The
risk-free rate is proxied by the 10-Year US Treasury yield (^TNX).

Classes:
    - Option: A base class for financial options that handles data fetching.
    - CallOption: A class for pricing European call options.
    - PutOption: A class for pricing European put options.

Usage:
    See the __main__ block for a detailed usage example demonstrating how to
    price a specific call and put option for a given ticker.
"""
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import numpy as np
import yfinance as yf
from scipy.stats import norm


class Option:
    """
    A base class for an option, handling data fetching and common calculations.
    """

    def __init__(self, ticker_symbol: str):
        """
        Initializes the Option object with the underlying ticker symbol.

        Args:
            ticker_symbol (str): The ticker symbol of the underlying asset (e.g., 'AAPL').
        """
        if not isinstance(ticker_symbol, str) or not ticker_symbol:
            raise ValueError("Ticker symbol must be a non-empty string.")

        self.ticker_symbol: str = ticker_symbol
        self.ticker: yf.Ticker = yf.Ticker(self.ticker_symbol)

        self.S: float = self._fetch_underlying_price()
        self.r: float = self._fetch_risk_free_rate()
        self.last_fetched_iv: Optional[float] = None

    def _fetch_underlying_price(self) -> float:
        """Fetches the last closing price of the underlying asset."""
        hist = self.ticker.history(period="1d")
        if hist.empty:
            raise ValueError(f"Could not fetch price for {self.ticker_symbol}.")
        return hist['Close'].iloc[-1]

    def _get_implied_volatility(self, expiration_date: str, K: float, option_type: str) -> float:
        """Fetches the implied volatility for a specific option contract."""
        try:
            chain = self.ticker.option_chain(expiration_date)

            df = chain.calls if option_type == 'call' else chain.puts
            if df.empty:
                raise ValueError(f"No {option_type} options found for {expiration_date}.")

            contract = df[df['strike'] == K]
            if contract.empty:
                closest_strike_index = (df['strike'] - K).abs().idxmin()
                contract = df.loc[[closest_strike_index]]
                closest_k = contract.iloc[0]['strike']
                print(f"Warning: Strike price {K:.2f} not found. Using closest strike: {closest_k:.2f}")

            iv = contract.iloc[0]['impliedVolatility']
            self.last_fetched_iv = iv
            return iv
        except Exception as e:
            raise ValueError(f"Could not fetch implied volatility. Error: {e}")

    def _fetch_risk_free_rate(self) -> float:
        """Fetches the 10-Year Treasury yield as a proxy for the risk-free rate."""
        tnx = yf.Ticker("^TNX")
        hist = tnx.history(period="1d")
        if hist.empty:
            return 0.04  # Fallback
        return hist['Close'].iloc[-1] / 100.0

    def _calculate_d1_d2(self, K: float, T: float, sigma: float) -> Tuple[float, float]:
        """Calculates the d1 and d2 terms for the Black-Scholes-Merton model."""
        if T <= 0:
            # For expired options, outcome is certain based on intrinsic value
            return np.inf if self.S > K else -np.inf, np.inf if self.S > K else -np.inf

        if sigma <= 0:
            # For zero volatility, outcome is certain. This makes norm.cdf(d1/d2) evaluate to 1 or 0.
            return np.inf if self.S > K else -np.inf, np.inf if self.S > K else -np.inf

        d1 = (np.log(self.S / K) + (self.r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        return d1, d2

    def get_market_data(self) -> Dict[str, Any]:
        """Returns a dictionary of the fetched market data."""
        data = {
            "Underlying Price (S)": self.S,
            "Risk-Free Rate (r)": self.r
        }
        if self.last_fetched_iv is not None:
            data["Last Fetched IV (sigma)"] = self.last_fetched_iv
        return data


class CallOption(Option):
    """
    A class for pricing a European call option and its Greeks.
    """

    def __init__(self, ticker_symbol: str):
        """Initializes the CallOption and its cache."""
        super().__init__(ticker_symbol)
        self._last_K: Optional[float] = None
        self._last_exp: Optional[str] = None
        self._last_params: Optional[Tuple[float, float, float, float]] = None

    def _get_pricing_parameters(self, K: float, expiration_date: str) -> Tuple[float, float, float, float]:
        """Helper to get time to expiration, IV, d1, and d2."""
        # Check cache first to avoid redundant API calls
        if self._last_K == K and self._last_exp == expiration_date and self._last_params:
            return self._last_params

        T = (datetime.strptime(expiration_date, "%Y-%m-%d") - datetime.now()).days / 365.25

        sigma = 0.0
        if T > 0:
            sigma = self._get_implied_volatility(expiration_date, K, 'call')

        d1, d2 = self._calculate_d1_d2(K, T, sigma)

        # Store result in cache
        self._last_K = K
        self._last_exp = expiration_date
        self._last_params = (T, sigma, d1, d2)

        return self._last_params

    def price(self, K: float, expiration_date: str) -> float:
        """Calculates the price of the call option using implied volatility."""
        T, sigma, d1, d2 = self._get_pricing_parameters(K, expiration_date)
        if T <= 0:
            return max(0.0, self.S - K)

        price = (self.S * norm.cdf(d1) - K * np.exp(-self.r * T) * norm.cdf(d2))
        return price

    def get_greeks(self, K: float, expiration_date: str) -> Dict[str, float]:
        """Calculates the Greeks for the call option using implied volatility."""
        T, sigma, d1, d2 = self._get_pricing_parameters(K, expiration_date)

        if T <= 0 or sigma <= 0:
            # When T or sigma is zero, pdf(inf) is 0, making gamma/vega zero.
            # Delta is 1 or 0 based on S vs K.
            delta = 1.0 if self.S > K else 0.0
            gamma = 0.0
            vega = 0.0
            # Theta and Rho can still have value based on interest.
            theta = (-self.r * K * np.exp(-self.r * T) * norm.cdf(d2)) / 365.25
            rho = (K * T * np.exp(-self.r * T) * norm.cdf(d2)) / 100
            return {"Delta": delta, "Gamma": gamma, "Vega": vega, "Theta": theta, "Rho": rho}

        delta = norm.cdf(d1)
        gamma = norm.pdf(d1) / (self.S * sigma * np.sqrt(T))
        vega = self.S * norm.pdf(d1) * np.sqrt(T) / 100
        theta = (- (self.S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - self.r * K * np.exp(-self.r * T) * norm.cdf(
            d2)) / 365.25
        rho = K * T * np.exp(-self.r * T) * norm.cdf(d2) / 100

        return {"Delta": delta, "Gamma": gamma, "Vega": vega, "Theta": theta, "Rho": rho}


class PutOption(Option):
    """
    A class for pricing a European put option and its Greeks.
    """

    def __init__(self, ticker_symbol: str):
        """Initializes the PutOption and its cache."""
        super().__init__(ticker_symbol)
        self._last_K: Optional[float] = None
        self._last_exp: Optional[str] = None
        self._last_params: Optional[Tuple[float, float, float, float]] = None

    def _get_pricing_parameters(self, K: float, expiration_date: str) -> Tuple[float, float, float, float]:
        """Helper to get time to expiration, IV, d1, and d2."""
        # Check cache first to avoid redundant API calls
        if self._last_K == K and self._last_exp == expiration_date and self._last_params:
            return self._last_params

        T = (datetime.strptime(expiration_date, "%Y-%m-%d") - datetime.now()).days / 365.25

        sigma = 0.0
        if T > 0:
            sigma = self._get_implied_volatility(expiration_date, K, 'put')

        d1, d2 = self._calculate_d1_d2(K, T, sigma)

        # Store result in cache
        self._last_K = K
        self._last_exp = expiration_date
        self._last_params = (T, sigma, d1, d2)

        return self._last_params

    def price(self, K: float, expiration_date: str) -> float:
        """Calculates the price of the put option using implied volatility."""
        T, sigma, d1, d2 = self._get_pricing_parameters(K, expiration_date)
        if T <= 0:
            return max(0.0, K - self.S)

        price = (K * np.exp(-self.r * T) * norm.cdf(-d2) - self.S * norm.cdf(-d1))
        return price

    def get_greeks(self, K: float, expiration_date: str) -> Dict[str, float]:
        """Calculates the Greeks for the put option using implied volatility."""
        T, sigma, d1, d2 = self._get_pricing_parameters(K, expiration_date)

        if T <= 0 or sigma <= 0:
            delta = -1.0 if self.S < K else 0.0
            gamma = 0.0
            vega = 0.0
            theta = (self.r * K * np.exp(-self.r * T) * norm.cdf(-d2)) / 365.25
            rho = (-K * T * np.exp(-self.r * T) * norm.cdf(-d2)) / 100
            return {"Delta": delta, "Gamma": gamma, "Vega": vega, "Theta": theta, "Rho": rho}

        delta = norm.cdf(d1) - 1
        gamma = norm.pdf(d1) / (self.S * sigma * np.sqrt(T))
        vega = self.S * norm.pdf(d1) * np.sqrt(T) / 100
        theta = (- (self.S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) + self.r * K * np.exp(-self.r * T) * norm.cdf(
            -d2)) / 365.25
        rho = -K * T * np.exp(-self.r * T) * norm.cdf(-d2) / 100

        return {"Delta": delta, "Gamma": gamma, "Vega": vega, "Theta": theta, "Rho": rho}


if __name__ == '__main__':
    # --- USAGE EXAMPLE ---

    TICKER = 'SPY'
    STRIKE_PRICE = 640.0
    # NOTE: yfinance provides monthly options. Pick an expiration date from the available chain.
    # To find available dates: yf.Ticker('AAPL').options
    EXPIRATION = '2025-10-31'

    # --- Call Option Example ---
    try:
        print(f"--- Calculating Call Option for {TICKER} ---")
        aapl_call = CallOption(TICKER)

        call_price = aapl_call.price(K=STRIKE_PRICE, expiration_date=EXPIRATION)
        market_data = aapl_call.get_market_data()

        print("\nFetched Market Data:")
        for key, value in market_data.items():
            print(f"  {key}: {value:.4f}")

        print(f"\nEstimated Call Price for Strike ${STRIKE_PRICE:.2f} expiring on {EXPIRATION}: ${call_price:.2f}")

        call_greeks = aapl_call.get_greeks(K=STRIKE_PRICE, expiration_date=EXPIRATION)
        print("\nCall Option Greeks:")
        for greek, value in call_greeks.items():
            print(f"  {greek}: {value:.4f}")

    except (ValueError, IndexError) as e:
        print(f"\nError processing Call Option: {e}")

    # --- Put Option Example ---
    try:
        print(f"\n\n--- Calculating Put Option for {TICKER} ---")
        aapl_put = PutOption(TICKER)

        put_price = aapl_put.price(K=STRIKE_PRICE, expiration_date=EXPIRATION)
        market_data = aapl_put.get_market_data()

        print("\nFetched Market Data:")
        for key, value in market_data.items():
            print(f"  {key}: {value:.4f}")

        print(f"\nEstimated Put Price for Strike ${STRIKE_PRICE:.2f} expiring on {EXPIRATION}: ${put_price:.2f}")

        put_greeks = aapl_put.get_greeks(K=STRIKE_PRICE, expiration_date=EXPIRATION)
        print("\nPut Option Greeks:")
        for greek, value in put_greeks.items():
            print(f"  {greek}: {value:.4f}")

    except (ValueError, IndexError) as e:
        print(f"\nError processing Put Option: {e}")
