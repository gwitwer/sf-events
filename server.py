#!/usr/bin/env python3
"""FastAPI server for SF Bay Area Events"""

import json
from pathlib import Path
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel
import uvicorn

# Initialize FastAPI app
app = FastAPI(
    title="SF Events API",
    description="API for SF Bay Area events with geocoding",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data models
class EventLocation(BaseModel):
    lat: float
    lon: float
    display_name: str
    approximate: bool = False

class Event(BaseModel):
    hidden: bool
    className: Optional[str]
    dayLabel: Optional[str]
    timeRange: Optional[str]
    title: str
    url: Optional[str]
    venue: Optional[str]
    city: Optional[str]
    genres: List[str]
    price: Optional[str]
    age: Optional[str]
    promoters: List[str]
    extraLinks: List[Dict[str, str]]
    dateISO: Optional[str]
    coordinates: Optional[EventLocation] = None

# Load events data
def load_events() -> List[Dict[str, Any]]:
    """Load events from the geocoded JSON file"""
    # Try geocoded file first
    geocoded_file = Path("events_all_geocoded.json")
    if geocoded_file.exists():
        with open(geocoded_file, 'r') as f:
            return json.load(f)
    
    # Fall back to latest parsed file
    latest_file = Path("19hz_events_latest.json")
    if latest_file.exists():
        with open(latest_file, 'r') as f:
            return json.load(f)
    
    # Fall back to original file
    original_file = Path("events-2025-08-29T19-48-28.json")
    if original_file.exists():
        with open(original_file, 'r') as f:
            return json.load(f)
    
    return []

# Cache events in memory
EVENTS_CACHE = None

def get_events_cached() -> List[Dict[str, Any]]:
    """Get cached events or load them"""
    global EVENTS_CACHE
    if EVENTS_CACHE is None:
        EVENTS_CACHE = load_events()
    return EVENTS_CACHE

# API Routes
@app.get("/")
async def read_root():
    """Serve the main HTML file"""
    # Try the new version first
    if Path("index_v2.html").exists():
        return FileResponse("index_v2.html")
    return FileResponse("index.html")

@app.get("/api/events", response_model=List[Dict[str, Any]])
async def get_events(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    city: Optional[str] = Query(None, description="Filter by city"),
    genre: Optional[str] = Query(None, description="Filter by genre"),
    day_of_week: Optional[int] = Query(None, description="Day of week (0=Monday, 6=Sunday)"),
    hidden: Optional[bool] = Query(False, description="Include hidden events"),
    limit: Optional[int] = Query(None, description="Limit number of results")
):
    """Get all events with optional filters"""
    events = get_events_cached()
    
    # Filter out hidden events unless requested
    if not hidden:
        events = [e for e in events if not e.get('hidden', False)]
    
    # Apply filters
    if start_date:
        events = [e for e in events if e.get('dateISO', '') >= start_date]
    
    if end_date:
        events = [e for e in events if e.get('dateISO', '') <= end_date]
    
    if city:
        events = [e for e in events if e.get('city', '').lower() == city.lower()]
    
    if genre:
        genre_lower = genre.lower()
        events = [e for e in events 
                 if any(genre_lower in g.lower() for g in e.get('genres', []))]
    
    if day_of_week is not None:
        events = [e for e in events 
                 if e.get('dateISO') and 
                 datetime.strptime(e['dateISO'], '%Y-%m-%d').weekday() == day_of_week]
    
    # Apply limit
    if limit:
        events = events[:limit]
    
    return events

@app.get("/api/events/today")
async def get_todays_events():
    """Get today's events"""
    today = date.today().isoformat()
    return await get_events(start_date=today, end_date=today)

@app.get("/api/events/weekend")
async def get_weekend_events():
    """Get this weekend's events (Friday-Sunday)"""
    events = get_events_cached()
    weekend_events = []
    
    for event in events:
        if event.get('dateISO'):
            try:
                event_date = datetime.strptime(event['dateISO'], '%Y-%m-%d')
                if event_date.weekday() in [4, 5, 6]:  # Friday, Saturday, Sunday
                    weekend_events.append(event)
            except:
                pass
    
    return weekend_events

@app.get("/api/events/stats")
async def get_stats():
    """Get statistics about the events"""
    events = get_events_cached()
    visible_events = [e for e in events if not e.get('hidden', False)]
    
    # Get unique values
    cities = list(set(e.get('city', '') for e in visible_events if e.get('city')))
    venues = list(set(e.get('venue', '') for e in visible_events if e.get('venue')))
    all_genres = []
    for e in visible_events:
        all_genres.extend(e.get('genres', []))
    genres = list(set(all_genres))
    
    # Date range
    dates = [e['dateISO'] for e in visible_events if e.get('dateISO')]
    dates.sort()
    
    return {
        "total_events": len(events),
        "visible_events": len(visible_events),
        "hidden_events": len(events) - len(visible_events),
        "unique_cities": len(cities),
        "unique_venues": len(venues),
        "unique_genres": len(genres),
        "date_range": {
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None
        },
        "cities": sorted(cities),
        "genres": sorted(genres)
    }

@app.get("/api/venues")
async def get_venues():
    """Get all unique venues with their locations"""
    events = get_events_cached()
    venues = {}
    
    for event in events:
        if event.get('venue') and event.get('coordinates'):
            key = f"{event['venue']}|{event.get('city', '')}"
            if key not in venues:
                venues[key] = {
                    "venue": event['venue'],
                    "city": event.get('city'),
                    "coordinates": event['coordinates'],
                    "event_count": 0
                }
            venues[key]['event_count'] += 1
    
    return list(venues.values())

@app.post("/api/refresh")
async def refresh_data():
    """Refresh the events cache"""
    global EVENTS_CACHE
    EVENTS_CACHE = None
    events = get_events_cached()
    return {"status": "success", "events_loaded": len(events)}

@app.get("/api/search")
async def search_events(
    q: str = Query(..., description="Search query"),
    field: Optional[str] = Query("all", description="Field to search (title, venue, genre, all)")
):
    """Search events by text"""
    events = get_events_cached()
    query = q.lower()
    results = []
    
    for event in events:
        if event.get('hidden'):
            continue
            
        if field == "title" or field == "all":
            if query in event.get('title', '').lower():
                results.append(event)
                continue
        
        if field == "venue" or field == "all":
            if query in event.get('venue', '').lower():
                results.append(event)
                continue
        
        if field == "genre" or field == "all":
            if any(query in g.lower() for g in event.get('genres', [])):
                results.append(event)
                continue
    
    return results

# Serve static files
@app.get("/events_all_geocoded.json")
async def get_geocoded_json():
    """Serve the geocoded events JSON file"""
    file_path = Path("events_all_geocoded.json")
    if file_path.exists():
        return FileResponse(file_path)
    raise HTTPException(status_code=404, detail="Geocoded events file not found")

@app.get("/events_organized.json")
async def get_organized_json():
    """Serve the organized events JSON file"""
    file_path = Path("events_organized.json")
    if file_path.exists():
        return FileResponse(file_path)
    raise HTTPException(status_code=404, detail="Organized events file not found")

@app.get("/events_tba.json")
async def get_tba_json():
    """Serve the TBA events JSON file"""
    file_path = Path("events_tba.json")
    if file_path.exists():
        return FileResponse(file_path)
    raise HTTPException(status_code=404, detail="TBA events file not found")

@app.get("/api/events/by-date/{date}")
async def get_events_by_date(date: str):
    """Get events for a specific date"""
    file_path = Path(f"events_by_date/events_{date}.json")
    if file_path.exists():
        with open(file_path, 'r') as f:
            events = json.load(f)
        return events
    return []

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# Run with: poetry run python server.py
if __name__ == "__main__":
    print("🚀 Starting SF Events API server...")
    print("📍 API docs: http://localhost:8001/docs")
    print("🗺️  Map view: http://localhost:8001")
    uvicorn.run("server:app", host="0.0.0.0", port=8001, reload=True)