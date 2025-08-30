#!/usr/bin/env python3
"""FastAPI server for SF Bay Area Events using SQLAlchemy database"""

import json
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, and_, or_, func
from sqlalchemy.orm import Session, sessionmaker, joinedload
import uvicorn

from models import (
    Event, Venue, Genre, Promoter, EventLink, TBAVenueHint,
    EventQueries, create_database, get_session
)

# Initialize FastAPI app
app = FastAPI(
    title="SF Events API (SQLite)",
    description="API for SF Bay Area events using SQLAlchemy and SQLite",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database setup
DATABASE_URL = "sqlite:///events.db"
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Pydantic models for API responses
class EventResponse(BaseModel):
    id: int
    title: str
    url: Optional[str]
    hidden: bool
    dateISO: Optional[str]
    dayLabel: Optional[str]
    timeRange: Optional[str]
    venue: Optional[str]
    city: Optional[str]
    coordinates: Optional[Dict[str, Any]]
    price: Optional[str]
    age: Optional[str]
    genres: List[str]
    promoters: List[str]
    extraLinks: List[Dict[str, str]]

    class Config:
        from_attributes = True

# API Routes
@app.get("/")
async def read_root():
    """Serve the main HTML file"""
    # Try the new version first
    if Path("index_v2.html").exists():
        return FileResponse("index_v2.html")
    return FileResponse("index.html")

@app.get("/api/events", response_model=List[EventResponse])
async def get_events(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    date: Optional[str] = Query(None, description="Specific date (YYYY-MM-DD)"),
    city: Optional[str] = Query(None, description="Filter by city"),
    venue: Optional[str] = Query(None, description="Filter by venue"),
    genre: Optional[str] = Query(None, description="Filter by genre"),
    promoter: Optional[str] = Query(None, description="Filter by promoter"),
    day_of_week: Optional[int] = Query(None, description="Day of week (0=Monday, 6=Sunday)"),
    is_tba: Optional[bool] = Query(None, description="Filter TBA venues"),
    hidden: Optional[bool] = Query(False, description="Include hidden events"),
    limit: Optional[int] = Query(None, description="Limit number of results"),
    db: Session = Depends(get_db)
):
    """Get events with optional filters"""
    
    # Start with base query
    query = db.query(Event).options(
        joinedload(Event.venue),
        joinedload(Event.genres),
        joinedload(Event.promoters),
        joinedload(Event.extra_links)
    )
    
    # Apply filters
    if not hidden:
        query = query.filter(Event.hidden == False)
    
    if date:
        # Specific date takes precedence
        query = query.filter(Event.date == datetime.strptime(date, '%Y-%m-%d').date())
    else:
        if start_date:
            query = query.filter(Event.date >= datetime.strptime(start_date, '%Y-%m-%d').date())
        if end_date:
            query = query.filter(Event.date <= datetime.strptime(end_date, '%Y-%m-%d').date())
    
    if city:
        query = query.join(Venue).filter(Venue.city == city)
    
    if venue:
        query = query.join(Venue).filter(Venue.name == venue)
    
    if genre:
        query = query.join(Event.genres).filter(Genre.name.ilike(f"%{genre}%"))
    
    if promoter:
        query = query.join(Event.promoters).filter(Promoter.name.ilike(f"%{promoter}%"))
    
    if day_of_week is not None:
        # SQLite doesn't have native day of week, so we fetch all and filter in Python
        events = query.all()
        events = [e for e in events if e.date and e.date.weekday() == day_of_week]
    else:
        events = query.all()
    
    if is_tba is not None:
        if is_tba:
            events = [e for e in events if e.venue and e.venue.is_tba]
        else:
            events = [e for e in events if not (e.venue and e.venue.is_tba)]
    
    # Apply limit
    if limit:
        events = events[:limit]
    
    # Convert to response format
    return [event.to_dict() for event in events]

@app.get("/api/events/by-date/{date_str}")
async def get_events_by_date(
    date_str: str,
    db: Session = Depends(get_db)
):
    """Get events for a specific date"""
    try:
        event_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    events = db.query(Event).filter(
        Event.date == event_date,
        Event.hidden == False
    ).options(
        joinedload(Event.venue),
        joinedload(Event.genres),
        joinedload(Event.promoters),
        joinedload(Event.extra_links)
    ).all()
    
    return [event.to_dict() for event in events]

@app.get("/api/events/today")
async def get_todays_events(db: Session = Depends(get_db)):
    """Get today's events"""
    today = date.today()
    
    events = db.query(Event).filter(
        Event.date == today,
        Event.hidden == False
    ).options(
        joinedload(Event.venue),
        joinedload(Event.genres),
        joinedload(Event.promoters),
        joinedload(Event.extra_links)
    ).all()
    
    return [event.to_dict() for event in events]

@app.get("/api/events/weekend")
async def get_weekend_events(db: Session = Depends(get_db)):
    """Get this weekend's events (Friday-Sunday)"""
    today = date.today()
    
    # Find next Friday
    days_until_friday = (4 - today.weekday()) % 7
    if days_until_friday == 0 and today.weekday() > 4:
        days_until_friday = 7
    friday = today + timedelta(days=days_until_friday)
    sunday = friday + timedelta(days=2)
    
    events = db.query(Event).filter(
        Event.date >= friday,
        Event.date <= sunday,
        Event.hidden == False
    ).options(
        joinedload(Event.venue),
        joinedload(Event.genres),
        joinedload(Event.promoters),
        joinedload(Event.extra_links)
    ).all()
    
    return [event.to_dict() for event in events]

@app.get("/api/events/tba")
async def get_tba_events(db: Session = Depends(get_db)):
    """Get all TBA venue events"""
    events = db.query(Event).join(Venue).filter(
        Venue.is_tba == True,
        Event.hidden == False
    ).options(
        joinedload(Event.venue),
        joinedload(Event.genres),
        joinedload(Event.promoters),
        joinedload(Event.extra_links)
    ).all()
    
    # Add TBA hints
    result = []
    for event in events:
        event_dict = event.to_dict()
        
        # Get hints for this event
        hints = db.query(TBAVenueHint).filter(
            TBAVenueHint.event_id == event.id
        ).all()
        
        event_dict['venue_hints'] = [
            {
                'type': hint.hint_type,
                'text': hint.hint_text,
                'confidence': hint.confidence
            }
            for hint in hints
        ]
        
        result.append(event_dict)
    
    return result

@app.get("/api/events/stats")
async def get_stats(db: Session = Depends(get_db)):
    """Get statistics about the events"""
    stats = EventQueries.get_stats(db)
    
    # Add more detailed stats
    cities = db.query(Venue.city, func.count(Event.id)).join(Event).filter(
        Event.hidden == False
    ).group_by(Venue.city).all()
    
    genres = db.query(Genre.name).order_by(Genre.name).all()
    
    stats['cities'] = [city for city, count in cities if city]
    stats['genres'] = [g[0] for g in genres]
    
    return stats

@app.get("/api/venues")
async def get_venues(
    include_tba: bool = Query(False, description="Include TBA venues"),
    db: Session = Depends(get_db)
):
    """Get all unique venues with their locations"""
    query = db.query(Venue)
    
    if not include_tba:
        query = query.filter(Venue.is_tba == False)
    
    venues = query.all()
    
    result = []
    for venue in venues:
        event_count = db.query(Event).filter(Event.venue_id == venue.id).count()
        
        result.append({
            "id": venue.id,
            "name": venue.name,
            "city": venue.city,
            "coordinates": {
                "lat": venue.latitude,
                "lon": venue.longitude,
                "display_name": venue.display_name,
                "approximate": venue.is_approximate
            } if venue.latitude else None,
            "is_tba": venue.is_tba,
            "event_count": event_count
        })
    
    return result

@app.get("/api/genres")
async def get_genres(db: Session = Depends(get_db)):
    """Get all genres with event counts"""
    genres = db.query(
        Genre.name,
        func.count(Event.id).label('event_count')
    ).join(Genre.events).filter(
        Event.hidden == False
    ).group_by(Genre.name).order_by(Genre.name).all()
    
    return [
        {"name": name, "event_count": count}
        for name, count in genres
    ]

@app.get("/api/promoters")
async def get_promoters(db: Session = Depends(get_db)):
    """Get all promoters with event counts"""
    promoters = db.query(
        Promoter.name,
        func.count(Event.id).label('event_count')
    ).join(Promoter.events).filter(
        Event.hidden == False
    ).group_by(Promoter.name).order_by(Promoter.name).all()
    
    return [
        {"name": name, "event_count": count}
        for name, count in promoters
    ]

@app.get("/api/search")
async def search_events(
    q: str = Query(..., description="Search query"),
    field: Optional[str] = Query("all", description="Field to search (title, venue, genre, promoter, all)"),
    db: Session = Depends(get_db)
):
    """Search events by text"""
    search_term = f"%{q}%"
    
    if field == "all":
        events = db.query(Event).filter(
            Event.hidden == False
        ).filter(
            or_(
                Event.title.ilike(search_term),
                Event.venue.has(Venue.name.ilike(search_term)),
                Event.genres.any(Genre.name.ilike(search_term)),
                Event.promoters.any(Promoter.name.ilike(search_term))
            )
        ).options(
            joinedload(Event.venue),
            joinedload(Event.genres),
            joinedload(Event.promoters),
            joinedload(Event.extra_links)
        ).all()
    elif field == "title":
        events = db.query(Event).filter(
            Event.hidden == False,
            Event.title.ilike(search_term)
        ).options(
            joinedload(Event.venue),
            joinedload(Event.genres),
            joinedload(Event.promoters),
            joinedload(Event.extra_links)
        ).all()
    elif field == "venue":
        events = db.query(Event).join(Venue).filter(
            Event.hidden == False,
            Venue.name.ilike(search_term)
        ).options(
            joinedload(Event.venue),
            joinedload(Event.genres),
            joinedload(Event.promoters),
            joinedload(Event.extra_links)
        ).all()
    elif field == "genre":
        events = db.query(Event).join(Event.genres).filter(
            Event.hidden == False,
            Genre.name.ilike(search_term)
        ).options(
            joinedload(Event.venue),
            joinedload(Event.genres),
            joinedload(Event.promoters),
            joinedload(Event.extra_links)
        ).all()
    elif field == "promoter":
        events = db.query(Event).join(Event.promoters).filter(
            Event.hidden == False,
            Promoter.name.ilike(search_term)
        ).options(
            joinedload(Event.venue),
            joinedload(Event.genres),
            joinedload(Event.promoters),
            joinedload(Event.extra_links)
        ).all()
    else:
        events = []
    
    return [event.to_dict() for event in events]

# Serve static files (keep compatibility with JSON endpoints)
@app.get("/events_organized.json")
async def get_organized_json(db: Session = Depends(get_db)):
    """Generate organized JSON from database"""
    events = db.query(Event).filter(
        Event.hidden == False
    ).options(
        joinedload(Event.venue),
        joinedload(Event.genres),
        joinedload(Event.promoters),
        joinedload(Event.extra_links)
    ).order_by(Event.date).all()
    
    # Group by date
    events_by_date = {}
    for event in events:
        date_str = event.date.isoformat() if event.date else 'unknown'
        if date_str not in events_by_date:
            events_by_date[date_str] = []
        events_by_date[date_str].append(event.to_dict())
    
    # Get metadata
    stats = EventQueries.get_stats(db)
    
    return {
        'metadata': {
            'date_range': stats['date_range'],
            'available_filters': {
                'cities': [v.city for v in db.query(Venue.city).distinct().all() if v.city],
                'genres': [g.name for g in db.query(Genre).order_by(Genre.name).all()],
                'venues': [v.name for v in db.query(Venue).filter(Venue.is_tba == False).all()],
                'promoters': [p.name for p in db.query(Promoter).order_by(Promoter.name).all()]
            },
            'statistics': stats
        },
        'events_by_date': events_by_date,
        'generated_at': datetime.now().isoformat()
    }

@app.get("/events_tba.json")
async def get_tba_json(db: Session = Depends(get_db)):
    """Get TBA events as JSON"""
    return await get_tba_events(db)

@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint"""
    # Try a simple query to check DB connection
    try:
        event_count = db.query(Event).count()
        return {
            "status": "healthy",
            "database": "connected",
            "events": event_count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# Run with: poetry run python server_db.py
if __name__ == "__main__":
    print("🚀 Starting SF Events API server (SQLite)...")
    print("📍 API docs: http://localhost:8001/docs")
    print("🗺️  Map view: http://localhost:8001")
    print("🗄️  Database: events.db")
    uvicorn.run("server_db:app", host="0.0.0.0", port=8001, reload=True)