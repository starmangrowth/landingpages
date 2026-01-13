from pydantic import BaseModel, Field
from typing import Optional

class Page(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    title: str
    body: str
    site_id: Optional[str] = None 