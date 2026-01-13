from pydantic import BaseSettings
from functools import lru_cache
from typing import Optional

class Settings(BaseSettings):
    # MongoDB URI - must be provided via environment variables
    mongo_uri: Optional[str] = None
    mongodb_url: Optional[str] = None  # Alternative environment variable name
    
    # CORS settings
    allowed_origins: list[str] = ["*"]

    class Config:
        env_file = ".env"
        
    @property
    def effective_mongo_uri(self) -> str:
        """Get the effective MongoDB URI from available sources"""
        uri = self.mongodb_url or self.mongo_uri
        if not uri:
            raise ValueError("MongoDB URI not configured! Please set MONGODB_URL in your environment variables.")
        return uri

@lru_cache
def get_settings():
    return Settings() 