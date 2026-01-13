"""
Client model for multi-tenant support.
Stores client configurations including API keys, webhooks, and display settings.
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class WebinarGeekFieldMappings(BaseModel):
    """Custom field mappings for WebinarGeek"""
    utm_source: str = "extra_field_101"
    utm_medium: str = "extra_field_102"
    utm_campaign: str = "extra_field_103"
    submitted_from_url: str = "extra_field_1527745"


class WebinarGeekConfig(BaseModel):
    """WebinarGeek API configuration"""
    api_key: str
    webinar_id: Optional[str] = None  # Optional default webinar ID
    field_mappings: WebinarGeekFieldMappings = WebinarGeekFieldMappings()


class WebhooksConfig(BaseModel):
    """Webhook URLs configuration"""
    google_sheet_url: Optional[str] = None
    ghl_url: Optional[str] = None
    custom_webhooks: List[Dict[str, Any]] = []


class LandingPage(BaseModel):
    """Landing page configuration"""
    page_id: str
    url: str
    active: bool = True


class DisplaySettings(BaseModel):
    """Display settings for frontend"""
    base_subscriber_count: int = 0  # Base count to add to real count
    timezone: str = "Asia/Kolkata"


class ClientCreate(BaseModel):
    """Schema for creating a new client"""
    client_id: str = Field(..., description="Unique client identifier (slug format, e.g., 'growth-club')")
    client_name: str = Field(..., description="Display name for the client")
    webinar_geek: WebinarGeekConfig
    webhooks: WebhooksConfig = WebhooksConfig()
    landing_pages: List[LandingPage] = []
    display: DisplaySettings = DisplaySettings()


class ClientUpdate(BaseModel):
    """Schema for updating a client"""
    client_name: Optional[str] = None
    status: Optional[str] = None
    webinar_geek: Optional[WebinarGeekConfig] = None
    webhooks: Optional[WebhooksConfig] = None
    landing_pages: Optional[List[LandingPage]] = None
    display: Optional[DisplaySettings] = None


class Client(BaseModel):
    """Full client model with all fields"""
    client_id: str
    client_name: str
    status: str = "active"  # active | inactive | suspended
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    webinar_geek: WebinarGeekConfig
    webhooks: WebhooksConfig = WebhooksConfig()
    landing_pages: List[LandingPage] = []
    display: DisplaySettings = DisplaySettings()

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ClientResponse(BaseModel):
    """Response schema for client data (hides sensitive fields)"""
    client_id: str
    client_name: str
    status: str
    created_at: datetime
    updated_at: datetime
    landing_pages: List[LandingPage] = []
    display: DisplaySettings
    # Note: webinar_geek and webhooks are intentionally excluded to hide API keys
