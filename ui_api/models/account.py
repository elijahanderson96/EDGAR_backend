from pydantic import BaseModel
from typing import Dict

class AccountInfo(BaseModel):
    username: str
    email: str
    api_key: str
    usage: Dict[str, int]  # Dictionary mapping endpoint names to their respective usage counts
