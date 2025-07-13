from datetime import datetime

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Query
import yfinance as yf

from app.models.options import CollarAnalysisRequest, ExpirationRequest, LongOptionAnalysisRequest
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
async def get_options(symbol: str = Query(...), expiration: str = Query(...), current_user: User = Depends(get_current_user)):
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

        # Clean the DataFrames: replace non-finite values with None
        puts = puts.replace([float('inf'), float('-inf')], float('nan')).fillna(None)
        calls = calls.replace([float('inf'), float('-inf')], float('nan')).fillna(None)

        return {
            "underlying_price": underlying_price,
            "puts": puts[
                ['strike', 'lastPrice', 'bid', 'ask', 'volume', 'openInterest', 'impliedVolatility']].to_dict(
                orient='records'),
            "calls": calls[
                ['strike', 'lastPrice', 'bid', 'ask', 'volume', 'openInterest', 'impliedVolatility']].to_dict(
                orient='records')
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/analyze-collar")
async def analyze_collar(request: CollarAnalysisRequest, current_user: User = Depends(get_current_user)):
    try:
        ticker = yf.Ticker(request.symbol)

        # Get current price
        hist = ticker.history(period="1d")
        if hist.empty:
            raise ValueError("No historical data available")
        underlying_price = hist['Close'].iloc[-1]

        # Get entire option chain for the single expiration
        chain = ticker.option_chain(request.expiration)
        
        # Find selected put and call in the same chain
        put = chain.puts[chain.puts['strike'] == request.put_strike]
        if put.empty:
            raise ValueError(f"No put found with strike {request.put_strike} for expiration {request.expiration}")
        put_premium = put['lastPrice'].iloc[0]

        call = chain.calls[chain.calls['strike'] == request.call_strike]
        if call.empty:
            raise ValueError(f"No call found with strike {request.call_strike} for expiration {request.expiration}")
        call_premium = call['lastPrice'].iloc[0]

        # Calculate net option flow
        net_option_flow = call_premium - put_premium
        cost_str = "credit" if net_option_flow > 0 else "debit"

        # Calculate days to expiration
        exp_date = datetime.strptime(request.expiration, '%Y-%m-%d')
        days_to_exp = (exp_date - datetime.now()).days

        # Calculate initial investment
        initial_investment = underlying_price - net_option_flow

        # Create price range
        price_range = np.linspace(underlying_price * 0.5, underlying_price * 1.5, 101)
        pct_changes = (price_range - underlying_price) / underlying_price * 100

        # Calculate values
        collar_values = []
        returns = []

        for price in price_range:
            put_value = max(0, request.put_strike - price)
            call_value = max(0, price - request.call_strike)
            collar_value = price + put_value - call_value + net_option_flow
            collar_values.append(collar_value)
            returns.append((collar_value - initial_investment) / initial_investment * 100)

        # Calculate key metrics
        max_gain = request.call_strike - underlying_price + net_option_flow
        max_loss = request.put_strike - underlying_price + net_option_flow
        break_even = underlying_price - net_option_flow
        max_return = (max_gain / initial_investment) * 100
        max_loss_pct = (max_loss / initial_investment) * 100
        rtn = {
            "underlying_price": underlying_price,
            "put_premium": put_premium,
            "call_premium": call_premium,
            "net_option_flow": net_option_flow,
            "cost_str": cost_str,
            "days_to_exp": days_to_exp,
            "initial_investment": initial_investment,
            "max_gain": max_gain,
            "max_loss": max_loss,
            "break_even": break_even,
            "max_return": max_return,
            "max_loss_pct": max_loss_pct,
            "price_range": price_range.tolist(),
            "pct_changes": pct_changes.tolist(),
            "collar_values": collar_values,
            "returns": returns}

        return rtn
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/analyze-long-put")
async def analyze_long_put(request: LongOptionAnalysisRequest, current_user: User = Depends(get_current_user)):
    try:
        ticker = yf.Ticker(request.symbol)

        # Get current price
        hist = ticker.history(period="1d")
        if hist.empty:
            raise ValueError("No historical data available")
        underlying_price = hist['Close'].iloc[-1]

        # Get options chain
        chain = ticker.option_chain(request.expiration)

        # Find selected put
        put = chain.puts[chain.puts['strike'] == request.strike]
        if put.empty:
            raise ValueError(f"No put found with strike {request.strike}")
        premium = put['lastPrice'].iloc[0]

        # Calculate days to expiration
        exp_date = datetime.strptime(request.expiration, '%Y-%m-%d')
        days_to_exp = (exp_date - datetime.now()).days

        # Create price range
        price_range = np.linspace(underlying_price * 0.5, underlying_price * 1.5, 101)
        pct_changes = (price_range - underlying_price) / underlying_price * 100

        # Calculate profits
        profits = [max(0, request.strike - price) - premium for price in price_range]

        # Calculate key metrics
        break_even = request.strike - premium
        max_profit = request.strike - premium  # When stock goes to 0
        max_loss = -premium

        return {
            "underlying_price": underlying_price,
            "premium": premium,
            "days_to_exp": days_to_exp,
            "break_even": break_even,
            "max_profit": max_profit,
            "max_loss": max_loss,
            "price_range": price_range.tolist(),
            "pct_changes": pct_changes.tolist(),
            "profits": profits
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/analyze-long-call")
async def analyze_long_call(request: LongOptionAnalysisRequest, current_user: User = Depends(get_current_user)):
    try:
        ticker = yf.Ticker(request.symbol)
        hist = ticker.history(period="1d")
        underlying_price = hist['Close'].iloc[-1]

        chain = ticker.option_chain(request.expiration)

        # Find selected call
        call = chain.calls[chain.calls['strike'] == request.strike]
        if call.empty:
            raise ValueError(f"No call found with strike {request.strike}")
        premium = call['lastPrice'].iloc[0]

        exp_date = datetime.strptime(request.expiration, '%Y-%m-%d')
        days_to_exp = (exp_date - datetime.now()).days

        price_range = np.linspace(underlying_price * 0.5, underlying_price * 1.5, 101)
        pct_changes = (price_range - underlying_price) / underlying_price * 100

        # Calculate profits (unlimited upside)
        profits = [max(0, price - request.strike) - premium for price in price_range]

        # Calculate key metrics
        break_even = request.strike + premium
        max_loss = -premium

        return {
            "underlying_price": underlying_price,
            "premium": premium,
            "days_to_exp": days_to_exp,
            "break_even": break_even,
            "max_profit": None,  # Unlimited
            "max_loss": max_loss,
            "price_range": price_range.tolist(),
            "pct_changes": pct_changes.tolist(),
            "profits": profits
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
