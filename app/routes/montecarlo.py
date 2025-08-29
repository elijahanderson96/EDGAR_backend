import yfinance as yf
import numpy as np
from fastapi import APIRouter, HTTPException
from app.models.montecarlo import MonteCarloRequest, MonteCarloResponse, ConfidenceIntervals, Period
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/montecarlo", tags=["Monte Carlo Simulation"])

def fetch_historical_data(symbol: str, period: str):
    """Fetch historical stock data using yfinance"""
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period=period)
        if hist.empty:
            raise ValueError("No data found for symbol and period")
        return hist
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data: {str(e)}")

def monte_carlo_simulation(current_price, returns, num_simulations, forecast_days):
    """Run Monte Carlo simulation based on historical returns"""
    # Calculate daily drift and volatility
    log_returns = np.log(1 + returns)
    u = log_returns.mean()
    var = log_returns.var()
    drift = u - (0.5 * var)
    stdev = log_returns.std()
    
    # Generate random values
    daily_returns = np.exp(drift + stdev * np.random.standard_normal((forecast_days, num_simulations)))
    
    # Create price paths
    price_paths = np.zeros_like(daily_returns)
    price_paths[0] = current_price
    for t in range(1, forecast_days):
        price_paths[t] = price_paths[t-1] * daily_returns[t]
    
    return price_paths

@router.post("/simulate", response_model=MonteCarloResponse)
async def run_monte_carlo_simulation(request: MonteCarloRequest):
    # Fetch historical data
    hist_data = fetch_historical_data(request.symbol, request.period.value)
    
    # Calculate daily returns
    closes = hist_data['Close']
    returns = closes.pct_change().dropna()
    
    # Get current price (last available close)
    current_price = closes.iloc[-1]
    
    # Run Monte Carlo simulation
    simulations = monte_carlo_simulation(
        current_price, 
        returns.values, 
        request.num_simulations, 
        request.forecast_days
    )
    
    # Calculate confidence intervals from the final prices
    final_prices = simulations[-1]
    mean_price = np.mean(final_prices)
    std_price = np.std(final_prices)
    
    # Calculate percentiles for confidence intervals
    one_std_low = np.percentile(final_prices, 15.87)  # Approximately -1 sigma
    one_std_high = np.percentile(final_prices, 84.13)  # Approximately +1 sigma
    two_std_low = np.percentile(final_prices, 2.28)   # Approximately -2 sigma
    two_std_high = np.percentile(final_prices, 97.72)  # Approximately +2 sigma
    three_std_low = np.percentile(final_prices, 0.13)  # Approximately -3 sigma
    three_std_high = np.percentile(final_prices, 99.87)  # Approximately +3 sigma
    
    # Limit number of simulations returned to avoid large response size
    max_simulations_to_return = 100
    simulations_list = simulations[:, :max_simulations_to_return].tolist()
    
    return MonteCarloResponse(
        current_price=current_price,
        simulations=simulations_list,
        confidence_intervals=ConfidenceIntervals(
            one_std=[one_std_low, one_std_high],
            two_std=[two_std_low, two_std_high],
            three_std=[three_std_low, three_std_high]
        ),
        forecast_days=request.forecast_days,
        period_used=request.period.value
    )
