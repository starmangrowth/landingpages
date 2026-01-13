from pydantic import BaseModel
from typing import Optional

class SiteSettings(BaseModel):
    site_name: str
    description: Optional[str] = None
    site_id: Optional[str] = None 