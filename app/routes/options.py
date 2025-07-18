from datetime import datetime

import numpy as np
from fastapi import APIRouter, Depends, HTTPException
import yfinance as yf

from app.models.options import CollarAnalysisRequest, ExpirationRequest, TickerRequest
from app.models.user import User
from app.routes.auth import get_current_user

router = APIRouter(
    tags=["Options"],
)


@router.post("/get-expirations")
async def get_expirations(request: TickerRequest, current_user: User = Depends(get_current_user)):
    try:
        ticker = yf.Ticker(request.symbol)
        expirations = ticker.options
        return {"expirations": expirations}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/get-options")
async def get_options(request: ExpirationRequest, current_user: User = Depends(get_current_user)):
    try:
        ticker = yf.Ticker(request.symbol)

        # Get current price
        hist = ticker.history(period="1d")
        if hist.empty:
            raise ValueError("No historical data available")
        underlying_price = hist['Close'].iloc[-1]

        # Get options
        chain = ticker.option_chain(request.expiration)
        puts = chain.puts
        calls = chain.calls

        # Filter relevant options
        relevant_puts = puts[
            (puts['strike'] >= underlying_price * 0.8) &
            (puts['strike'] <= underlying_price * 1.0)
            ].sort_values('strike', ascending=False)

        relevant_calls = calls[
            (calls['strike'] >= underlying_price * 1.0) &
            (calls['strike'] <= underlying_price * 1.2)
            ].sort_values('strike')

        return {
            "underlying_price": underlying_price,
            "puts": relevant_puts[
                ['strike', 'lastPrice', 'bid', 'ask', 'volume', 'openInterest', 'impliedVolatility']].to_dict(
                orient='records'),
            "calls": relevant_calls[
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

        # Get options
        chain = ticker.option_chain(request.expiration)

        # Find selected put
        put = chain.puts[chain.puts['strike'] == request.put_strike]
        if put.empty:
            raise ValueError(f"No put found with strike {request.put_strike}")
        put_premium = put['lastPrice'].iloc[0]

        # Find selected call
        call = chain.calls[chain.calls['strike'] == request.call_strike]
        if call.empty:
            raise ValueError(f"No call found with strike {request.call_strike}")
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

        return {
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
            "returns": returns
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
