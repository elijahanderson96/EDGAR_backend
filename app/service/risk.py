"""
This service analyzes the risk of a user-defined options portfolio by combining
options pricing with Monte Carlo simulations of the underlying asset's price.

It defines a portfolio as a collection of option legs and calculates the
potential Profit and Loss (P&L) distribution at the time of the portfolio's
earliest expiration date.

Key metrics calculated:
- P&L at various underlying price percentiles for the earliest expiration.
- Expected P&L, Value at Risk (VaR), and CVaR at the earliest expiration.
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import yfinance as yf

from app.service.montecarlo import MonteCarloStockSimulator

logger = logging.getLogger(__name__)


class Portfolio:
    """Represents a portfolio of options strategies on a single underlying."""

    def __init__(self, ticker: str):
        self.ticker = ticker
        self.legs: List[Dict[str, Any]] = []
        self.total_cost = 0.0
        self._ticker_obj = yf.Ticker(self.ticker)

    def add_leg(self, quantity: int, option_type: str, strike: float, expiration: str):
        """
        Adds an option leg to the portfolio by fetching its current market price.
        Can handle legs with different expiration dates.

        Args:
            quantity (int): Number of contracts. Positive for long, negative for short.
            option_type (str): 'call' or 'put'.
            strike (float): The strike price of the option.
            expiration (str): The expiration date in 'YYYY-MM-DD' format.
        """
        if option_type.lower() not in ['call', 'put']:
            raise ValueError("option_type must be 'call' or 'put'.")

        try:
            # Fetch the live option chain from Yahoo Finance
            opt_chain = self._ticker_obj.option_chain(expiration)
            df = opt_chain.calls if option_type.lower() == 'call' else opt_chain.puts

            # Find the specific contract by its strike price
            contract = df[df['strike'] == strike]
            if contract.empty:
                raise ValueError(f"No contract found for strike ${strike:.2f} on {expiration}.")

            # Use the last traded price for the entry cost calculation
            price = contract.iloc[0]['lastPrice']
            cost = price * quantity * 100  # Standard 100 shares per contract
            self.total_cost += cost

            leg_info = {
                "quantity": quantity,
                "type": option_type,
                "strike": strike,
                "expiration": expiration,
                "entry_cost": cost,
                "entry_price_per_contract": price
            }
            self.legs.append(leg_info)
            logger.info(
                f"Added Leg: {quantity} {self.ticker} {expiration} {strike:.2f}{option_type[0].upper()} @ ${price:.2f} (Market Price). Cost: ${cost:.2f}")

        except Exception as e:
            logger.error(
                f"Could not fetch market price and add leg for {self.ticker} {strike}{option_type[0].upper()}: {e}")
            raise


class RiskAnalyzer:
    """Analyzes a portfolio's risk using Monte Carlo simulation at the earliest expiration."""

    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.pnl_distribution: Optional[np.ndarray] = None
        self.final_prices: Optional[np.ndarray] = None
        self.risk_metrics: Dict[str, float] = {}
        self.pnl_at_stock_percentiles: Dict[str, Dict[int, float]] = {}

    def _calculate_portfolio_value(self, stock_price: float, evaluation_date: datetime) -> float:
        """
        Helper to calculate total portfolio value at a future date and price.
        It values all options that have not yet expired at their intrinsic value.
        """
        total_value = 0
        for leg in self.portfolio.legs:
            leg_exp_date = datetime.strptime(leg['expiration'], "%Y-%m-%d")

            # Only consider legs that have not expired by the evaluation_date
            if leg_exp_date >= evaluation_date:
                intrinsic_value = 0
                if leg['type'] == 'call':
                    intrinsic_value = max(0, stock_price - leg['strike'])
                else:  # put
                    intrinsic_value = max(0, leg['strike'] - stock_price)

                total_value += intrinsic_value * leg['quantity'] * 100
        return total_value

    def run_analysis(self, num_simulations: int = 10000, history_days: int = 365 * 5):
        """
        Runs the risk analysis by simulating outcomes until the earliest
        expiration date in the portfolio.
        """
        if not self.portfolio.legs:
            logger.error("Cannot run analysis on an empty portfolio.")
            return

        # Find the shortest expiration date to set the analysis horizon.
        shortest_expiration_str = min(leg['expiration'] for leg in self.portfolio.legs)
        expiration_date = datetime.strptime(shortest_expiration_str, "%Y-%m-%d")
        time_horizon_days = (expiration_date - datetime.now()).days

        # Approximate trading days (5/7th of calendar days)
        trading_days_to_exp = int(time_horizon_days * (5 / 7))
        if trading_days_to_exp <= 0:
            logger.error(
                f"Cannot run analysis. The earliest expiration date ({shortest_expiration_str}) is in the past or today.")
            return

        logger.info(
            f"\n--- Analyzing for Earliest Expiration: {shortest_expiration_str} ({trading_days_to_exp} trading days) ---")
        logger.info(f"Running {num_simulations} simulations for {self.portfolio.ticker}.")

        simulator = MonteCarloStockSimulator(
            symbol=self.portfolio.ticker,
            data_range_days=history_days,
            num_simulations=num_simulations,
            time_horizon_days=trading_days_to_exp
        )
        simulator.run_simulation()

        if simulator.simulation_results is None:
            logger.error(f"Monte Carlo simulation failed for {shortest_expiration_str}.")
            return

        # --- Calculate P&L at each 10th percentile of underlying price ---
        final_prices_at_percentiles = simulator.percentiles
        pnl_for_this_exp = {}
        for percentile, stock_price in final_prices_at_percentiles.items():
            value = self._calculate_portfolio_value(stock_price, expiration_date)
            pnl = value - self.portfolio.total_cost
            pnl_for_this_exp[percentile] = pnl
        self.pnl_at_stock_percentiles[shortest_expiration_str] = pnl_for_this_exp

        # --- Calculate full P&L distribution for VaR/CVaR ---
        self.final_prices = simulator.simulation_results[-1]
        pnl_dist = []
        for final_price in self.final_prices:
            value = self._calculate_portfolio_value(final_price, expiration_date)
            pnl_dist.append(value - self.portfolio.total_cost)

        self.pnl_distribution = np.array(pnl_dist)
        self._calculate_risk_metrics()

        self.display_results()

    def _calculate_risk_metrics(self):
        """Calculates VaR, CVaR, and other stats from the P&L distribution."""
        if self.pnl_distribution is None:
            return

        var_95 = np.percentile(self.pnl_distribution, 5)
        cvar_95 = self.pnl_distribution[self.pnl_distribution <= var_95].mean()

        self.risk_metrics = {
            "Expected P&L": self.pnl_distribution.mean(),
            "P&L Standard Deviation": self.pnl_distribution.std(),
            "Probability of Profit": (self.pnl_distribution > 0).mean() * 100,
            "Min P&L (Max Loss)": self.pnl_distribution.min(),
            "Max P&L (Max Gain)": self.pnl_distribution.max(),
            "95% VaR": var_95,
            "95% CVaR (Expected Shortfall)": cvar_95
        }

    def display_results(self):
        """Prints a formatted summary of the risk analysis."""
        if not self.risk_metrics and not self.pnl_at_stock_percentiles:
            logger.warning("No risk metrics calculated.")
            return

        print("\n" + "=" * 60)
        print(f"PORTFOLIO RISK ANALYSIS: {self.portfolio.ticker}")
        print(f"Total Initial Cost/Credit: ${self.portfolio.total_cost:.2f}")
        print("=" * 60)

        # Since we only analyze one date, we can get it directly.
        exp_date = list(self.pnl_at_stock_percentiles.keys())[0]

        if self.risk_metrics:
            print(f"\nSTATISTICAL RISK METRICS (at expiration: {exp_date})")
            print("-" * 60)
            for name, value in self.risk_metrics.items():
                if "Probability" in name:
                    print(f"{name:<30}: {value:.2f}%")
                else:
                    print(f"{name:<30}: ${value:,.2f}")
            print("-" * 60)

        if self.pnl_at_stock_percentiles:
            print(f"\nP&L AT UNDERLYING STOCK PRICE PERCENTILES (at expiration: {exp_date})")
            print("-" * 60)
            pnl_data = self.pnl_at_stock_percentiles[exp_date]
            for percentile in sorted(pnl_data.keys()):
                pnl = pnl_data[percentile]
                print(f"  {percentile:>3}th Percentile Stock Price -> P&L: ${pnl:,.2f}")
            print("=" * 60)

    def plot_pnl_distribution(self):
        """Plots the P&L distribution histogram for the earliest expiration."""
        if self.pnl_distribution is None:
            logger.warning("No P&L distribution to plot.")
            return
        try:
            import matplotlib.pyplot as plt
            exp_date = list(self.pnl_at_stock_percentiles.keys())[0]
            plt.figure(figsize=(10, 6))
            plt.hist(self.pnl_distribution, bins=100, edgecolor='k', alpha=0.7)
            plt.title(f'P&L Distribution for {self.portfolio.ticker} at Expiration ({exp_date})')
            plt.xlabel('Profit & Loss ($)')
            plt.ylabel('Frequency')
            plt.axvline(self.risk_metrics.get("Expected P&L", 0), color='r', linestyle='--', label=f'Expected P&L')
            plt.axvline(self.risk_metrics.get("95% VaR", 0), color='orange', linestyle='--', label=f'95% VaR')
            plt.grid(True, alpha=0.3)
            plt.legend()
            plt.show()
        except ImportError:
            logger.error("Matplotlib is not installed. Cannot plot P&L distribution.")


if __name__ == '__main__':
    # --- Example Strategy: SPY Call Calendar Spread ---
    # Goal: Profit from time decay and stable prices.
    # - Sell 1 Call with a near-term expiration.
    # - Buy 1 Call with a longer-term expiration at the same strike.

    TICKER_TO_ANALYZE = 'IWM'
    # Find available expiration dates using: yf.Ticker('SPY').options
    # Using future dates for demonstration
    NEAR_TERM_EXPIRATION = '2025-09-19'
    LONG_TERM_EXPIRATION = '2025-12-31'

    try:
        # 1. Define the Portfolio and its legs
        strategy = Portfolio(TICKER_TO_ANALYZE)

        strategy.add_leg(quantity=1, option_type='call', strike=160, expiration=LONG_TERM_EXPIRATION)

        strategy.add_leg(quantity=1, option_type='put', strike=270, expiration=LONG_TERM_EXPIRATION)

        strategy.add_leg(quantity=-1, option_type='call', strike=242, expiration=NEAR_TERM_EXPIRATION)

        # 2. Create the analyzer and run the analysis for the earliest expiration.
        analyzer = RiskAnalyzer(strategy)
        analyzer.run_analysis(num_simulations=10000)

        # 3. Plot the P&L distribution for the earliest expiration date.
        analyzer.plot_pnl_distribution()

    except Exception as e:
        logger.error(f"An error occurred during the analysis: {e}")
