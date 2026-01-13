from pydantic import BaseModel, Field
from typing import Optional

class Post(BaseModel):
    id: Optional[str] = Field(None, alias="_id")
    title: str
    content: str
    site_id: Optional[str] = None 