from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

class WebinarRegistration(BaseModel):
    client_id: str
    email: EmailStr
    firstName: Optional[str] = None
    surname: Optional[str] = None
    name: Optional[str] = None
    companyName: Optional[str] = None
    phone: Optional[str] = None
    countryCode: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    submittedFromUrl: Optional[str] = None
    broadcastId: Optional[str] = None
    webinarId: Optional[str] = None
    id: Optional[str] = None  # Channel ID
    submittedAt: Optional[datetime] = None
    terms: Optional[bool] = False

class WebinarDetails(BaseModel):
    pass  # Expand if needed

class LeadSubmission(BaseModel):
    submittedAt: Optional[datetime] = None
    # Add fields as needed

