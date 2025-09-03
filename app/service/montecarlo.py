import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yfinance as yf

# --- Module Level Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class MonteCarloStockSimulator:
    """
    A class to perform Monte Carlo simulations for a given stock symbol.
    """

    def __init__(self,
                 symbol: str,
                 data_range_days: int,
                 num_simulations: int,
                 time_horizon_days: int = 252):
        """
        Initializes the MonteCarloStockSimulator.

        Args:
            symbol (str): The stock ticker symbol (e.g., 'AAPL').
            data_range_days (int): The number of past calendar days to fetch historical data for.
            num_simulations (int): The number of simulation runs to perform.
            time_horizon_days (int): The number of future trading days to project. Defaults to 252.
        """
        self.symbol = symbol
        self.data_range_days = data_range_days
        self.num_simulations = num_simulations
        self.time_horizon_days = time_horizon_days

        self.stock_data: Optional[pd.DataFrame] = None
        self.log_returns: Optional[pd.Series] = None
        self.drift: Optional[float] = None
        self.volatility: Optional[float] = None
        self.simulation_results: Optional[np.ndarray] = None
        self.percentiles: Dict[int, float] = {}

        logger.info(f"Simulator initialized for {self.symbol}.")

    def _fetch_stock_data(self) -> None:
        """Fetches historical stock data from Yahoo Finance."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.data_range_days)
        try:
            logger.info(f"Fetching data for {self.symbol} from {start_date.date()} to {end_date.date()}.")
            self.stock_data = yf.download(self.symbol, start=start_date, end=end_date)['Close'][self.symbol]
            print(self.stock_data)
            if self.stock_data.empty:
                raise ValueError("No data fetched. The symbol may be incorrect or delisted.")
            logger.info("Successfully fetched stock data.")
        except Exception as e:
            logger.error(f"Failed to fetch stock data for {self.symbol}: {e}")
            raise

    def _calculate_historical_params(self) -> None:
        """Calculates historical log returns, drift, and volatility."""
        if self.stock_data is None:
            logger.error("Stock data not available. Cannot calculate parameters.")
            return

        self.log_returns = np.log(1 + self.stock_data.pct_change())
        mean_log_return = self.log_returns.mean()
        variance = self.log_returns.var()
        self.volatility = self.log_returns.std()

        # Calculate drift using the formula: drift = u - (1/2) * var
        self.drift = mean_log_return - (0.5 * variance)
        logger.info(f"Calculated Drift: {self.drift:.6f}, Volatility: {self.volatility:.6f}")

    def run_simulation(self) -> None:
        """Runs the Monte Carlo simulation."""
        self._fetch_stock_data()
        self._calculate_historical_params()

        if self.drift is None or self.volatility is None or self.stock_data is None:
            logger.error("Cannot run simulation due to missing historical parameters.")
            return

        # Generate random values for each day of each simulation
        # Z = standard normal distribution
        z = np.random.standard_normal((self.time_horizon_days, self.num_simulations))
        daily_returns = np.exp(self.drift + self.volatility * z)

        # Initialize price paths array
        price_paths = np.zeros_like(daily_returns)
        last_price = self.stock_data.iloc[-1]
        price_paths[0] = last_price

        # Populate the price paths
        for t in range(1, self.time_horizon_days):
            price_paths[t] = price_paths[t - 1] * daily_returns[t]

        self.simulation_results = price_paths
        logger.info(f"Monte Carlo simulation completed with {self.num_simulations} runs.")
        self._calculate_statistics()

    def _calculate_statistics(self) -> None:
        """Calculates the percentile ranges for the final price."""
        if self.simulation_results is None:
            logger.warning("Simulation results not available to calculate statistics.")
            return

        final_prices = self.simulation_results[-1]
        for i in range(0, 101, 10):
            self.percentiles[i] = np.percentile(final_prices, i)

        logger.info(f"Calculated percentiles for final price.")

    def plot_results(self) -> None:
        """Plots the simulation results using Matplotlib."""
        if self.simulation_results is None:
            logger.warning("No simulation results to plot.")
            return

        plt.figure(figsize=(15, 8))
        plt.plot(self.simulation_results[:, :100])  # Plot first 100 simulations
        plt.title(f'Monte Carlo Simulation for {self.symbol} ({self.num_simulations} runs)')
        plt.xlabel(f'Trading Days from Today (Horizon: {self.time_horizon_days} days)')
        plt.ylabel('Stock Price ($)')
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)

        last_price = self.stock_data.iloc[-1]
        plt.axhline(y=last_price, color='r', linestyle='-', label=f'Starting Price: ${last_price:.2f}')

        colors = cm.rainbow(np.linspace(0, 1, len(self.percentiles)))
        for (percentile, value), color in zip(self.percentiles.items(), colors):
            plt.axhline(y=value, color=color, linestyle='--', label=f'{percentile}th Percentile: ${value:.2f}')

        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        plt.tight_layout(rect=[0, 0, 0.85, 1])
        plt.savefig("monte_carlo_simulation.png")


# --- Example Usage ---
if __name__ == '__main__':
    # --- Parameters ---
    STOCK_SYMBOL = 'IWM'
    HISTORICAL_DAYS = 365 * 25  # Use 25 years of historical data
    SIMULATION_COUNT = 1000
    TIME_HORIZON = 14  # Project one trading year into the future

    # --- Run Simulation ---
    simulator = MonteCarloStockSimulator(
        symbol=STOCK_SYMBOL,
        data_range_days=HISTORICAL_DAYS,
        num_simulations=SIMULATION_COUNT,
        time_horizon_days=TIME_HORIZON
    )
    simulator.run_simulation()

    # --- Access Results ---
    if not simulator.percentiles:
        print('No percentiles were calculated.')
    else:
        print("\n--- Simulation Results ---")
        print(f"Stock Symbol: {simulator.symbol}")
        print(f"Number of Simulations: {simulator.num_simulations}")
        print(f"Time Horizon: {simulator.time_horizon_days} trading days")
        print("\n--- Percentile Ranges ---")
        for percentile, value in simulator.percentiles.items():
            print(f"{percentile}th Percentile: ${value:.2f}")
        print("--------------------------\n")

    # --- Plotting ---
    # Note: The plot will be saved as 'monte_carlo_simulation.png'
    simulator.plot_results()
