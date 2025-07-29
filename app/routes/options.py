import datetime

import numpy as np
import pandas as pd
import yfinance as yf
from fastapi import APIRouter, Depends, HTTPException, Query
from scipy.stats import norm

from app.models.user import User
from app.routes.auth import get_current_user

router = APIRouter(
    tags=["Options"],
)


@router.get("/get-expirations")
async def get_expirations(symbol: str = Query(...), current_user: User = Depends(get_current_user)):
    try:
        ticker = yf.Ticker(symbol)
        expirations = ticker.options
        return {"expirations": expirations}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/get-options")
async def get_options(symbol: str = Query(...), expiration: str = Query(...),
                      current_user: User = Depends(get_current_user)):
    try:
        ticker = yf.Ticker(symbol)

        # Get current price
        hist = ticker.history(period="1d")

        if hist.empty:
            raise ValueError("No historical data available")

        underlying_price = hist['Close'].iloc[-1]

        # Get options
        chain = ticker.option_chain(expiration)
        puts = chain.puts
        calls = chain.calls

        puts = puts.fillna(0)
        calls = calls.fillna(0)

        rtn = {
            "underlying_price": underlying_price,
            "puts": puts[
                ['strike', 'lastPrice', 'bid', 'ask', 'volume', 'openInterest', 'impliedVolatility']].to_dict(
                orient='records'),
            "calls": calls[
                ['strike', 'lastPrice', 'bid', 'ask', 'volume', 'openInterest', 'impliedVolatility']].to_dict(
                orient='records')
        }

        return rtn
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/calculate-collar-returns")
async def calculate_collar_returns(
        symbol: str = Query(...),
        purchase_price: float = Query(..., gt=0),
        put_strike: float = Query(..., gt=0),
        put_premium: float = Query(..., ge=0),
        call_strike: float = Query(..., gt=0),
        call_premium: float = Query(..., ge=0),
        expiration_date: str = Query(...),
        shares: int = Query(..., gt=0),
        simulations: int = Query(10000, gt=0),
        current_user: User = Depends(get_current_user)
):
    """
    Calculates the potential returns for a collar position and runs a Monte Carlo simulation.
    This version includes a corrected dividend projection logic.
    """
    try:
        # Validate expiration date format
        try:
            expiration = datetime.datetime.strptime(expiration_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Please use YYYY-MM-DD.")

        today = datetime.date.today()
        if expiration <= today:
            raise HTTPException(status_code=400, detail="Expiration date must be in the future.")

        days_to_expiration = (expiration - today).days

        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1y")
        if hist.empty:
            raise HTTPException(status_code=404, detail=f"No historical data found for symbol '{symbol}'.")

        # --- Dividend Calculation (Corrected and Robust Logic) ---
        total_dividends_per_share = 0.0
        divs = ticker.dividends

        # Only proceed if the stock has a dividend history
        if not divs.empty:
            # Get the most recent dividend amount
            last_div_amount = divs.iloc[-1]

            # Estimate the frequency of dividend payments
            if len(divs.index) >= 2:
                # Use the time between the last two payments as the frequency
                frequency = divs.index[-1] - divs.index[-2]
            else:
                # If only one dividend is on record, assume quarterly
                frequency = pd.Timedelta(days=91)

            # Start projecting from the last known ex-dividend date
            projected_date = divs.index[-1]

            # Create a comparable expiration timestamp, handling timezones
            # yfinance dividend dates are typically timezone-aware
            expiration_ts = pd.Timestamp(datetime.datetime.combine(expiration, datetime.time.max))
            if projected_date.tz:
                expiration_ts = expiration_ts.tz_localize(projected_date.tz)

            # Loop to find all projected dividends within the holding period
            while True:
                projected_date += frequency
                if projected_date > expiration_ts:
                    break  # Stop if the next projected date is past our expiration
                if projected_date.date() > today:
                    total_dividends_per_share += last_div_amount

        # --- Net Cost Calculation ---
        net_option_premium = call_premium - put_premium
        initial_investment = purchase_price * shares

        # --- Max Gain, Max Loss, and Profit/Risk Ratio ---
        max_gain_nominal = (call_strike - purchase_price + net_option_premium + total_dividends_per_share) * shares
        max_loss_nominal = (purchase_price - put_strike - net_option_premium - total_dividends_per_share) * shares

        if initial_investment <= 0:
            raise HTTPException(status_code=400, detail="Initial investment must be positive.")

        max_gain_percentage = (max_gain_nominal / initial_investment) * 100
        max_loss_percentage = (max_loss_nominal / initial_investment) * 100

        # Annualized Returns
        annualization_factor = 365.25 / days_to_expiration if days_to_expiration > 0 else 0
        annualized_max_gain_percentage = max_gain_percentage * annualization_factor
        annualized_max_loss_percentage = max_loss_percentage * annualization_factor

        # Profit to Risk Ratio
        if abs(max_loss_nominal) <= 1e-9:  # Avoid division by zero
            profit_to_risk_ratio = float('inf') if max_gain_nominal > 0 else 0
        else:
            profit_to_risk_ratio = max_gain_nominal / abs(max_loss_nominal)

        # --- Monte Carlo Simulation ---
        log_returns = np.log(1 + hist['Close'].pct_change())
        mu = log_returns.mean()
        sigma = log_returns.std()
        daily_drift = mu - (0.5 * sigma ** 2)

        Z = norm.ppf(np.random.rand(days_to_expiration, simulations))
        daily_returns_sim = np.exp(daily_drift + sigma * Z)

        price_paths = np.zeros_like(daily_returns_sim)
        price_paths[0] = hist['Close'].iloc[-1]
        for t in range(1, days_to_expiration):
            price_paths[t] = price_paths[t - 1] * daily_returns_sim[t]

        expiration_prices = price_paths[-1]

        def calculate_pl(stock_price):
            pl_per_share = 0
            if stock_price >= call_strike:
                pl_per_share = call_strike - purchase_price
            elif stock_price <= put_strike:
                pl_per_share = put_strike - purchase_price
            else:  # In between strikes
                pl_per_share = stock_price - purchase_price
            return (pl_per_share + net_option_premium + total_dividends_per_share) * shares

        simulation_returns = [calculate_pl(price) for price in expiration_prices]

        return {
            "max_gain_nominal": max_gain_nominal,
            "max_gain_percentage": max_gain_percentage,
            "annualized_max_gain_percentage": annualized_max_gain_percentage,
            "max_loss_nominal": -max_loss_nominal,  # Return loss as a negative number
            "max_loss_percentage": -max_loss_percentage,
            "annualized_max_loss_percentage": -annualized_max_loss_percentage,
            "profit_to_risk_ratio": profit_to_risk_ratio,
            "total_dividends_received": total_dividends_per_share * shares,
            "monte_carlo_results": {
                "average_return": np.mean(simulation_returns),
                "median_return": np.median(simulation_returns),
                "std_dev_return": np.std(simulation_returns),
                "probability_of_profit": np.mean([1 for r in simulation_returns if r > 0]) * 100,
                "return_distribution": simulation_returns,
            }
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        # Return a more specific error if possible, otherwise a generic server error
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
