from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

class WebinarRegistration(BaseModel):
    client_id: str = Field(..., description="Client identifier for multi-tenant support")  # NEW: Required for multi-tenant
    name: Optional[str] = None  # Single name field from frontend
    firstName: Optional[str] = None  # For backward compatibility
    lastName: Optional[str] = None   # For backward compatibility
    email: str
    companyName: Optional[str] = None
    phone: Optional[str] = None
    countryCode: Optional[str] = None
    terms: Optional[bool] = False
    webinarId: Optional[str] = "not_available"
    webinarTitle: Optional[str] = "Webinar information not available"
    submittedAt: Optional[datetime] = None
    submittedFromUrl: Optional[str] = None
    # Broadcast timing fields from frontend
    nextBroadcastTimestamp: Optional[int] = None
    nextBroadcastDate: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None
    # WebinarGeek response fields
    webinarGeekResponse: Optional[Dict[str, Any]] = None
    webinarGeekId: Optional[int] = None
    confirmationLink: Optional[str] = None
    watchLink: Optional[str] = None
    emailVerified: Optional[bool] = None
    timeZone: Optional[str] = None
    createdAt: Optional[int] = None
    # Broadcast related fields
    broadcastId: Optional[int] = None
    broadcastDate: Optional[str] = None
    broadcastHasEnded: Optional[bool] = None
    broadcastCancelled: Optional[bool] = None
    replayAvailable: Optional[bool] = None
    # Additional webinar fields
    webinarGeekTitle: Optional[str] = None
    webinarGeekUrl: Optional[str] = None

class WebinarDetails(BaseModel):
    id: str
    title: str
    description: str
    language: str
    image_url: Optional[str] = None
    status: str
    created_date: str
    timezone: str
    duration: int
    registration_url: Optional[str] = None
    replay_url: Optional[str] = None
    view_without_registration_url: Optional[str] = None
    ondemand: bool
    max_subscribers: int
    max_viewers: int
    current_subscribers: int
    total_registrations: int
    total_attendees: int
    total_views: int
    next_broadcast: Optional[Dict[str, Any]] = None
    presenter: Dict[str, Any]

class LeadSubmission(BaseModel):
    firstName: str
    lastName: str
    email: str
    phone: Optional[str] = None
    countryCode: Optional[str] = None
    companyName: Optional[str] = None
    businessStage: Optional[str] = None
    dealSize: Optional[str] = None
    startTime: Optional[str] = None
    selectedDate: Optional[str] = None
    selectedTime: Optional[str] = None
    submittedAt: Optional[datetime] = None 