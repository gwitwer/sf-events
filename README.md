# SF Bay Area Events Map

Interactive map viewer for SF Bay Area events with automatic geocoding and data parsing.

## Features
- 🗺️ Interactive map with clustered markers
- 📍 Automatic geocoding with caching
- 🎯 Filter by date, city, and genre
- 📊 Parse events from 19hz.info
- ⚡ Pre-geocoded data for instant loading

## Setup

```bash
# Install dependencies
poetry install

# Fetch latest events from 19hz.info
poetry run python fetch_19hz.py

# Parse HTML to JSON
poetry run python parse_19hz.py

# Geocode all events
poetry run python geocode_all_events.py

# Start local server
poetry run python -m http.server 8000
```

Then open http://localhost:8000 in your browser.

## Files
- `index.html` - Interactive map viewer
- `fetch_19hz.py` - Fetches HTML from 19hz.info
- `parse_19hz.py` - Parses HTML to JSON format
- `geocode_all_events.py` - Geocodes venues with smart caching
- `events_all_geocoded.json` - Complete dataset with coordinates