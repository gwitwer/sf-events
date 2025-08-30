# SF Bay Area Events Map

Interactive map viewer for SF Bay Area events with automatic geocoding and SQLite database.

## Features

- 🗺️ Interactive map with venue locations
- 📅 Date-based event filtering
- 🎵 Filter by genre, venue, city, and promoter
- 🔍 Search functionality
- 📍 Automatic geocoding of venues
- 🎭 TBA venue tracking and hints
- 💾 SQLite database with SQLAlchemy ORM

## Local Development

### Prerequisites
- Python 3.9+
- Poetry (optional, for dependency management)

### Setup with Poetry
```bash
# Install dependencies
poetry install

# Run database migration
poetry run python migrate_to_db.py

# Start the server
poetry run python server_db.py
```

### Setup without Poetry
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run database migration
python migrate_to_db.py

# Start the server
python server_db.py
```

Visit http://localhost:8001

## Deployment on Render

### Manual Deployment

1. Fork or clone this repository
2. Create a new Web Service on Render
3. Connect your GitHub repository
4. Render will automatically detect the `render.yaml` configuration
5. Deploy!

### Environment Variables (Optional)
- `PORT`: Server port (Render sets this automatically)
- `DATABASE_URL`: SQLite database path (default: `sqlite:///events.db`)

## API Endpoints

- `GET /` - Map interface
- `GET /api/events` - Get all events with filters
- `GET /api/events/today` - Today's events
- `GET /api/events/weekend` - Weekend events
- `GET /api/events/tba` - TBA venue events
- `GET /api/venues` - All venues
- `GET /api/genres` - All genres
- `GET /api/promoters` - All promoters
- `GET /api/search?q=query` - Search events
- `GET /api/stats` - Database statistics
- `GET /docs` - Interactive API documentation

## Data Sources

Events are scraped from 19hz.info and stored in a local SQLite database.

## Tech Stack

- **Backend**: FastAPI, SQLAlchemy, SQLite
- **Frontend**: Vanilla JavaScript, Leaflet.js
- **Geocoding**: Nominatim API
- **Deployment**: Render

## License

MIT
