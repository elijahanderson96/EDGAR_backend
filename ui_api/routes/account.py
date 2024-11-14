from fastapi import APIRouter, Request, HTTPException
from database.async_database import db_connector
from datetime import datetime
from ui_api.helpers import get_user_id_and_request_type
from ui_api.models.account import AccountInfo

account_router = APIRouter()

@account_router.get("/account", response_model=AccountInfo)
async def get_account_info(request: Request):
    user_id, is_api_key = await get_user_id_and_request_type(request)

    query = """
    SELECT u.username, u.email, u.api_key,
           COALESCE(au.metadata_route_count, 0) AS metadata_route_count,
           COALESCE(au.assets_route_count, 0) AS assets_route_count,
           COALESCE(au.cash_financing_activities_route_count, 0) AS cash_financing_activities_route_count,
           COALESCE(au.cash_investing_activities_route_count, 0) AS cash_investing_activities_route_count,
           COALESCE(au.cash_operating_activities_route_count, 0) AS cash_operating_activities_route_count,
           COALESCE(au.common_stock_route_count, 0) AS common_stock_route_count,
           COALESCE(au.comprehensive_income_route_count, 0) AS comprehensive_income_route_count,
           COALESCE(au.cost_of_revenue_route_count, 0) AS cost_of_revenue_route_count,
           COALESCE(au.current_assets_route_count, 0) AS current_assets_route_count,
           COALESCE(au.current_liabilities_route_count, 0) AS current_liabilities_route_count,
           COALESCE(au.depreciation_and_amortization_route_count, 0) AS depreciation_and_amortization_route_count,
           COALESCE(au.eps_basic_route_count, 0) AS eps_basic_route_count,
           COALESCE(au.eps_diluted_route_count, 0) AS eps_diluted_route_count,
           COALESCE(au.goodwill_route_count, 0) AS goodwill_route_count,
           COALESCE(au.gross_profit_route_count, 0) AS gross_profit_route_count,
           COALESCE(au.historical_data_route_count, 0) AS historical_data_route_count,
           COALESCE(au.intangible_assets_route_count, 0) AS intangible_assets_route_count,
           COALESCE(au.interest_expense_route_count, 0) AS interest_expense_route_count,
           COALESCE(au.inventory_route_count, 0) AS inventory_route_count,
           COALESCE(au.liabilities_route_count, 0) AS liabilities_route_count,
           COALESCE(au.net_income_loss_route_count, 0) AS net_income_loss_route_count,
           COALESCE(au.operating_expenses_route_count, 0) AS operating_expenses_route_count,
           COALESCE(au.operating_income_route_count, 0) AS operating_income_route_count,
           COALESCE(au.operating_income_loss_route_count, 0) AS operating_income_loss_route_count,
           COALESCE(au.preferred_stock_route_count, 0) AS preferred_stock_route_count,
           COALESCE(au.property_plant_and_equipment_route_count, 0) AS property_plant_and_equipment_route_count,
           COALESCE(au.research_and_development_expense_route_count, 0) AS research_and_development_expense_route_count,
           COALESCE(au.retained_earnings_route_count, 0) AS retained_earnings_route_count,
           COALESCE(au.revenue_route_count, 0) AS revenue_route_count,
           COALESCE(au.shares_route_count, 0) AS shares_route_count,
           COALESCE(au.total_stockholders_equity_route_count, 0) AS total_stockholders_equity_route_count
    FROM users.users u
    LEFT JOIN metadata.api_usage au ON u.id = au.user_id AND au.billing_period = $1
    WHERE u.id = $2
    """

    billing_period = datetime.now().strftime("%m-%Y")
    result = await db_connector.run_query(query, (billing_period, int(user_id)))

    if result.empty:
        raise HTTPException(status_code=404, detail="User not found or no API usage for the current period.")

    user_info = result.iloc[0].to_dict()
    return user_info
