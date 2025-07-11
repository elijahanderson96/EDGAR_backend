import yfinance as yf
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import matplotlib.colors as mcolors


class CollarStrategyAnalyzer:
    """
    A class to analyze collar strategies (protective put + covered call) using live options data.
    """

    def __init__(self, ticker_symbol):
        """
        Initializes the analyzer with a specific stock ticker.

        Args:
            ticker_symbol (str): The stock symbol (e.g., 'AAPL', 'TSLA').
        """
        self.ticker_symbol = ticker_symbol
        self.ticker = yf.Ticker(ticker_symbol)
        try:
            hist = self.ticker.history(period="1d")
            if hist.empty:
                raise ValueError(f"No historical data for symbol '{ticker_symbol}'")
            self.underlying_price = hist['Close'].iloc[-1]
        except (IndexError, KeyError) as e:
            raise ValueError(
                f"Could not fetch data for symbol '{ticker_symbol}'. Error: {str(e)}")

        print(f"\nSuccessfully initialized analyzer for {ticker_symbol.upper()}.")
        print(f"Current Underlying Price: ${self.underlying_price:.2f}")

    def get_expirations(self):
        """
        Retrieves the available option expiration dates.

        Returns:
            tuple: A tuple of available expiration date strings.
        """
        return self.ticker.options

    def get_put_options(self, expiration_date):
        """
        Retrieves put options for a specific expiration date.

        Args:
            expiration_date (str): Expiration date in 'YYYY-MM-DD' format

        Returns:
            DataFrame: Put options data
        """
        try:
            puts = self.ticker.option_chain(expiration_date).puts
            return puts
        except Exception as e:
            print(f"Could not fetch put options for {expiration_date}. Error: {e}")
            return pd.DataFrame()

    def get_call_options(self, expiration_date):
        """
        Retrieves call options for a specific expiration date.

        Args:
            expiration_date (str): Expiration date in 'YYYY-MM-DD' format

        Returns:
            DataFrame: Call options data
        """
        try:
            calls = self.ticker.option_chain(expiration_date).calls
            return calls
        except Exception as e:
            print(f"Could not fetch call options for {expiration_date}. Error: {e}")
            return pd.DataFrame()

    def display_options(self, options, option_type):
        """
        Displays available options in a formatted table.

        Args:
            options (DataFrame): Options data
            option_type (str): 'Put' or 'Call'
        """
        if options.empty:
            print(f"No {option_type} options available for this expiration.")
            return None

        # Filter relevant options based on type
        if option_type == 'Put':
            relevant = options[
                (options['strike'] >= self.underlying_price * 0.8) &
                (options['strike'] <= self.underlying_price * 1.0)
                ].sort_values('strike', ascending=False)
        else:  # Call
            relevant = options[
                (options['strike'] >= self.underlying_price * 1.0) &
                (options['strike'] <= self.underlying_price * 1.2)
                ].sort_values('strike')

        if relevant.empty:
            print(f"No relevant {option_type} options near the current price.")
            return None

        # Create formatted table
        print(f"\nAvailable {option_type} Options:")
        print("-" * 120)
        print(
            f"{'Index':<6} | {'Strike':<10} | {'Last Price':<12} | {'Bid':<8} | {'Ask':<8} | {'Volume':<10} | {'Open Interest':<12} | {'In The Money':<12} | {'Implied Volatility':<18}")
        print("-" * 120)

        for i, (_, row) in enumerate(relevant.iterrows()):
            in_the_money = "Yes" if (option_type == 'Put' and row['strike'] > self.underlying_price) or \
                                    (option_type == 'Call' and row['strike'] < self.underlying_price) else "No"
            print(
                f"{i + 1:<6} | ${row['strike']:<9.2f} | ${row['lastPrice']:<11.2f} | ${row['bid']:<7.2f} | ${row['ask']:<7.2f} | "
                f"{row['volume']:<10} | {row['openInterest']:<12} | {in_the_money:<12} | {row['impliedVolatility'] * 100:<18.2f}%")
        print("-" * 120)
        return relevant

    def analyze_collar(self, expiration_date, put_option, call_option):
        """
        Analyzes the collar strategy for selected put and call options.

        Args:
            expiration_date (str): The chosen expiration date
            put_option (Series): The selected put option
            call_option (Series): The selected call option
        """
        put_strike = put_option['strike']
        put_premium = put_option['lastPrice']
        call_strike = call_option['strike']
        call_premium = call_option['lastPrice']

        # Calculate net option cost/premium
        net_option_flow = call_premium - put_premium
        cost_str = "credit" if net_option_flow > 0 else "debit"

        # Calculate days to expiration
        exp_date = datetime.strptime(expiration_date, '%Y-%m-%d')
        days_to_exp = (exp_date - datetime.now()).days

        # Calculate initial investment per share (including options cost)
        initial_investment = self.underlying_price - net_option_flow

        # --- Create a range of future underlying prices to analyze ---
        price_range = np.linspace(self.underlying_price * 0.5, self.underlying_price * 1.5, 101)
        pct_changes = (price_range - self.underlying_price) / self.underlying_price * 100

        unhedged_values = []
        collar_values = []
        collar_returns = []

        # Calculate collar position values and returns
        for price in price_range:
            # Unhedged position
            unhedged_values.append(price)

            # Collar position
            put_value = max(0, put_strike - price)
            call_value = max(0, price - call_strike)
            collar_value = price + put_value - call_value + net_option_flow
            collar_values.append(collar_value)

            # Collar return from initial investment
            collar_return = (collar_value - initial_investment) / initial_investment * 100
            collar_returns.append(collar_return)

        # Calculate key metrics
        max_gain = call_strike - self.underlying_price + net_option_flow
        max_loss = put_strike - self.underlying_price + net_option_flow
        break_even = self.underlying_price - net_option_flow
        max_return = (max_gain / initial_investment) * 100
        max_loss_pct = (max_loss / initial_investment) * 100

        print("\n" + "=" * 120)
        print(f"COLLAR STRATEGY ANALYSIS")
        print("=" * 120)
        print(f"  Stock: {self.ticker_symbol.upper()} at ${self.underlying_price:.2f}")
        print(f"  Expiration: {expiration_date} ({days_to_exp} days)")
        print(f"  Protective Put: ${put_strike:.2f} strike at ${put_premium:.2f}")
        print(f"  Covered Call: ${call_strike:.2f} strike at ${call_premium:.2f}")
        print(f"  Net Option Flow: ${net_option_flow:.2f} ({cost_str})")
        print(f"  Initial Investment per Share: ${initial_investment:.2f}")
        print(f"  Breakeven: ${break_even:.2f}")
        print(f"  Max Gain: ${max_gain:.2f} ({max_return:.1f}%)")
        print(f"  Max Loss: ${max_loss:.2f} ({max_loss_pct:.1f}%)")
        print("=" * 120)

        # --- Create enhanced visualization ---
        plt.figure(figsize=(14, 10))
        plt.style.use('seaborn-v0_8-whitegrid')

        # Create a colormap for the returns
        norm = mcolors.TwoSlopeNorm(vmin=min(collar_returns), vcenter=0, vmax=max(collar_returns))
        cmap = plt.get_cmap('RdYlGn')

        # Plot collar returns with color gradient
        sc = plt.scatter(pct_changes, collar_returns, c=collar_returns, cmap=cmap, norm=norm,
                         s=25, alpha=0.8, edgecolors='none')

        # Add colorbar
        cbar = plt.colorbar(sc)
        cbar.set_label('Return (%)', rotation=270, labelpad=20)

        # Add key price levels
        plt.axvline(x=0, color='black', linestyle='-', alpha=0.5, label='Current Price')
        plt.axvline(x=(call_strike - self.underlying_price) / self.underlying_price * 100,
                    color='darkred', linestyle='--', alpha=0.7, label='Call Strike')
        plt.axvline(x=(put_strike - self.underlying_price) / self.underlying_price * 100,
                    color='darkblue', linestyle='--', alpha=0.7, label='Put Strike')
        plt.axvline(x=(break_even - self.underlying_price) / self.underlying_price * 100,
                    color='purple', linestyle='-.', alpha=0.8, label='Breakeven')

        # Add zero return line
        plt.axhline(y=0, color='gray', linestyle='-', alpha=0.5)

        # Add max gain/loss markers
        plt.scatter((call_strike - self.underlying_price) / self.underlying_price * 100, max_return,
                    s=100, marker='^', color='green', edgecolor='black', label='Max Gain')
        plt.scatter((put_strike - self.underlying_price) / self.underlying_price * 100, max_loss_pct,
                    s=100, marker='v', color='red', edgecolor='black', label='Max Loss')

        # Add annotations
        plt.annotate(f'Max Gain\n${max_gain:.2f}\n{max_return:.1f}%',
                     xy=((call_strike - self.underlying_price) / self.underlying_price * 100, max_return),
                     xytext=(5, max_return + 5), textcoords='data',
                     arrowprops=dict(facecolor='black', shrink=0.05))

        plt.annotate(f'Max Loss\n${max_loss:.2f}\n{max_loss_pct:.1f}%',
                     xy=((put_strike - self.underlying_price) / self.underlying_price * 100, max_loss_pct),
                     xytext=(-60, max_loss_pct - 5), textcoords='data',
                     arrowprops=dict(facecolor='black', shrink=0.05))

        # Add title and labels
        plt.title(
            f'Collar Strategy: {self.ticker_symbol.upper()} | {expiration_date} ({days_to_exp} days)\n'
            f'Put ${put_strike:.2f} (${put_premium:.2f}) | Call ${call_strike:.2f} (${call_premium:.2f}) | '
            f'Net: ${net_option_flow:.2f} ({cost_str})',
            fontsize=14, fontweight='bold')

        plt.xlabel('Stock Price Change (%)')
        plt.ylabel('Return on Investment (%)')
        plt.legend(loc='best')
        plt.grid(True, alpha=0.3)

        # Add metrics table
        metrics = [
            ["Current Price", f"${self.underlying_price:.2f}"],
            ["Put Strike", f"${put_strike:.2f}"],
            ["Call Strike", f"${call_strike:.2f}"],
            ["Put Premium", f"${put_premium:.2f}"],
            ["Call Premium", f"${call_premium:.2f}"],
            ["Net Option Flow", f"${net_option_flow:.2f} ({cost_str})"],
            ["Initial Investment", f"${initial_investment:.2f}"],
            ["Breakeven", f"${break_even:.2f}"],
            ["Max Gain", f"${max_gain:.2f} ({max_return:.1f}%)"],
            ["Max Loss", f"${max_loss:.2f} ({max_loss_pct:.1f}%)"],
            ["Days to Expiration", f"{days_to_exp} days"]
        ]

        plt.table(cellText=metrics,
                  cellLoc='left',
                  loc='bottom',
                  colWidths=[0.3, 0.3],
                  bbox=[0, -0.55, 1, 0.45])

        plt.tight_layout()
        plt.subplots_adjust(bottom=0.4)
        plt.show()


def main():
    """
    Main function to run the collar strategy analyzer.
    """
    print("=" * 70)
    print("Collar Strategy Analyzer: Protected Put + Covered Call")
    print("=" * 70)

    try:
        ticker = input("Enter stock ticker symbol (e.g., AAPL, SCHD): ").strip().upper()
        analyzer = CollarStrategyAnalyzer(ticker)

        expirations = analyzer.get_expirations()
        if not expirations:
            print(f"No options expirations found for {ticker}.")
            return

        print("\nAvailable expiration dates:")
        for i, date in enumerate(expirations):
            days = (datetime.strptime(date, '%Y-%m-%d') - datetime.now()).days
            print(f"  {i + 1}: {date} ({days} days)")

        exp_choice = int(input(f"\nSelect an expiration date (1-{len(expirations)}): "))
        expiration_date = expirations[exp_choice - 1]

        # Get put options for selected expiration
        puts = analyzer.get_put_options(expiration_date)
        if puts.empty:
            print(f"No put options found for expiration: {expiration_date}")
            return

        # Display and select put option
        print("\nSelect a PUT option for downside protection:")
        relevant_puts = analyzer.display_options(puts, 'Put')
        if relevant_puts is None or relevant_puts.empty:
            return
        put_choice = int(input(f"\nSelect a put option (1-{len(relevant_puts)}): "))
        selected_put = relevant_puts.iloc[put_choice - 1]

        # Get call options for selected expiration
        calls = analyzer.get_call_options(expiration_date)
        if calls.empty:
            print(f"No call options found for expiration: {expiration_date}")
            return

        # Display and select call option
        print("\nSelect a CALL option for income generation:")
        relevant_calls = analyzer.display_options(calls, 'Call')
        if relevant_calls is None or relevant_calls.empty:
            return
        call_choice = int(input(f"\nSelect a call option (1-{len(relevant_calls)}): "))
        selected_call = relevant_calls.iloc[call_choice - 1]

        print("\n" + "=" * 70)
        print(f"RUNNING ANALYSIS FOR COLLAR STRATEGY")
        print("=" * 70)
        print(f"  Stock: {ticker} at ${analyzer.underlying_price:.2f}")
        print(f"  Expiration: {expiration_date}")
        print(f"  Protective Put: ${selected_put['strike']} strike at ${selected_put['lastPrice']:.2f}")
        print(f"  Covered Call: ${selected_call['strike']} strike at ${selected_call['lastPrice']:.2f}")
        net_flow = selected_call['lastPrice'] - selected_put['lastPrice']
        print(f"  Net Option Flow: ${net_flow:.2f} ({'credit' if net_flow > 0 else 'debit'})")
        print("=" * 70)

        analyzer.analyze_collar(expiration_date, selected_put, selected_call)

    except ValueError as ve:
        print(f"\nError: {ve}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
