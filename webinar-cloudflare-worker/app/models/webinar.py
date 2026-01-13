from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

class WebinarRegistration(BaseModel):
    client_id: str
    email: EmailStr
    firstName: Optional[str] = None
    surname: Optional[str] = None
    name: Optional[str] = None
    # Add all fields from your /register (phone, companyName, countryCode, utm_*, submittedFromUrl, etc.)
    submittedAt: Optional[datetime] = None
    broadcastId: Optional[str] = None
    # etc.

class WebinarDetails(BaseModel):
    pass  # Fill as needed

class LeadSubmission(BaseModel):
    pass  # Fill as needed
