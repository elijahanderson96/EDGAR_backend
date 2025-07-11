# Add to your financials router file
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

# Import your token-based authentication dependencies
from app.routes.auth import get_current_user  # Replace with your actual import
from app.models.user import User  # Replace with your actual user model

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/docs",
    tags=["Financial Data"],
)


# REMOVED: router.dependencies.append(Depends(verify_api_key))


@router.get("/financials", include_in_schema=False)
async def get_financial_data_docs(current_user: User = Depends(get_current_user)):
    """
    Secured endpoint for Financial Data API documentation.
    Requires authentication via JWT token.
    """
    try:
        print(current_user.json())
        spec = get_financial_data_openapi_spec()
        return JSONResponse(content=spec)
    except HTTPException as he:
        # Re-raise existing HTTPExceptions
        raise he
    except Exception as e:
        logger.error(f"Failed to generate documentation: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate documentation"
        )


# Cache for OpenAPI spec
FINANCIAL_SPEC_CACHE = None
CACHE_EXPIRATION = None


def get_financial_data_openapi_spec():
    global FINANCIAL_SPEC_CACHE, CACHE_EXPIRATION

    # Return cached version if valid
    if FINANCIAL_SPEC_CACHE and CACHE_EXPIRATION and datetime.now() < CACHE_EXPIRATION:
        return FINANCIAL_SPEC_CACHE

    # Generate the full OpenAPI spec
    from app.main import app
    full_spec = app.openapi()

    # Filter to only include Financial Data endpoints
    filtered_paths = {}
    for path, methods in full_spec.get("paths", {}).items():
        for method, spec in methods.items():
            # Safely check for tags
            endpoint_tags = spec.get("tags", [])
            if "Financial Data" in endpoint_tags:
                if path not in filtered_paths:
                    filtered_paths[path] = {}
                filtered_paths[path][method] = spec

    # Create filtered spec
    filtered_spec = {
        "openapi": full_spec.get("openapi", "3.0.0"),
        "info": {
            "title": "Financial Data API",
            "version": "1.0.0",
            "description": "Secured financial data endpoints for options analysis"
        },
        "paths": filtered_paths,
        "components": {
            "schemas": full_spec.get("components", {}).get("schemas", {}),
            "securitySchemes": full_spec.get("components", {}).get("securitySchemes", {})
        }
    }

    # Add tags if they exist in the root spec
    if "tags" in full_spec:
        filtered_spec["tags"] = [
            tag for tag in full_spec["tags"]
            if tag.get("name") == "Financial Data"
        ]

    # Cache for 5 minutes
    FINANCIAL_SPEC_CACHE = filtered_spec
    CACHE_EXPIRATION = datetime.now() + timedelta(minutes=5)

    return filtered_spec
