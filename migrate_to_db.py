#!/usr/bin/env python3
"""
Migrate JSON events data to SQLite database
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import (
    Base, Event, Venue, Genre, Promoter, EventLink, TBAVenueHint,
    create_database, get_session
)


class EventMigrator:
    """Migrate JSON events to SQLite database"""
    
    def __init__(self, db_path: str = "events.db"):
        """Initialize migrator with database connection"""
        self.engine = create_database(db_path)
        self.session = get_session(self.engine)
        
        # Cache for deduplication
        self.venue_cache: Dict[str, Venue] = {}
        self.genre_cache: Dict[str, Genre] = {}
        self.promoter_cache: Dict[str, Promoter] = {}
        
        # Statistics
        self.stats = {
            'events_migrated': 0,
            'venues_created': 0,
            'genres_created': 0,
            'promoters_created': 0,
            'tba_venues': 0,
            'errors': []
        }
    
    def get_or_create_venue(self, venue_name: str, city: str, coordinates: Optional[dict] = None) -> Venue:
        """Get existing venue or create new one"""
        # Create cache key
        cache_key = f"{venue_name}|{city}"
        
        # Check cache first
        if cache_key in self.venue_cache:
            return self.venue_cache[cache_key]
        
        # Check if it's a TBA venue
        is_tba = bool(venue_name and re.search(r'TBA|TBD', venue_name, re.I))
        
        # Query database
        venue = self.session.query(Venue).filter(
            Venue.name == venue_name,
            Venue.city == city
        ).first()
        
        if not venue:
            # Create new venue
            venue = Venue(
                name=venue_name,
                city=city,
                is_tba=is_tba
            )
            
            # Add coordinates if available
            if coordinates:
                venue.latitude = coordinates.get('lat')
                venue.longitude = coordinates.get('lon')
                venue.display_name = coordinates.get('display_name')
                venue.is_approximate = coordinates.get('approximate', False)
            
            self.session.add(venue)
            self.session.flush()  # Get the ID without committing
            self.stats['venues_created'] += 1
            
            if is_tba:
                self.stats['tba_venues'] += 1
        
        # Cache it
        self.venue_cache[cache_key] = venue
        return venue
    
    def get_or_create_genre(self, genre_name: str) -> Genre:
        """Get existing genre or create new one"""
        # Check cache first
        if genre_name in self.genre_cache:
            return self.genre_cache[genre_name]
        
        # Query database
        genre = self.session.query(Genre).filter(Genre.name == genre_name).first()
        
        if not genre:
            # Create new genre
            genre = Genre(name=genre_name)
            self.session.add(genre)
            self.session.flush()
            self.stats['genres_created'] += 1
        
        # Cache it
        self.genre_cache[genre_name] = genre
        return genre
    
    def get_or_create_promoter(self, promoter_name: str) -> Promoter:
        """Get existing promoter or create new one"""
        # Check cache first
        if promoter_name in self.promoter_cache:
            return self.promoter_cache[promoter_name]
        
        # Query database
        promoter = self.session.query(Promoter).filter(Promoter.name == promoter_name).first()
        
        if not promoter:
            # Create new promoter
            promoter = Promoter(name=promoter_name)
            self.session.add(promoter)
            self.session.flush()
            self.stats['promoters_created'] += 1
        
        # Cache it
        self.promoter_cache[promoter_name] = promoter
        return promoter
    
    def migrate_event(self, event_data: dict) -> Optional[Event]:
        """Migrate a single event from JSON to database"""
        try:
            # Parse date
            date_str = event_data.get('dateISO')
            event_date = None
            if date_str:
                try:
                    event_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                except ValueError:
                    print(f"Warning: Invalid date format: {date_str}")
            
            # Get or create venue
            venue = None
            if event_data.get('venue'):
                venue = self.get_or_create_venue(
                    event_data['venue'],
                    event_data.get('city', ''),
                    event_data.get('coordinates')
                )
            
            # Create event
            event = Event(
                title=event_data.get('title', 'Untitled Event'),
                url=event_data.get('url'),
                hidden=event_data.get('hidden', False),
                date=event_date,
                day_label=event_data.get('dayLabel'),
                time_range=event_data.get('timeRange'),
                venue=venue,
                price=event_data.get('price'),
                age_restriction=event_data.get('age'),
                original_json=json.dumps(event_data),
                source='19hz'
            )
            
            # Add genres (deduplicate first)
            unique_genres = set(event_data.get('genres', []))
            for genre_name in unique_genres:
                if genre_name:
                    genre = self.get_or_create_genre(genre_name)
                    if genre not in event.genres:  # Avoid duplicates
                        event.genres.append(genre)
            
            # Add promoters
            for promoter_name in event_data.get('promoters', []):
                if promoter_name:
                    promoter = self.get_or_create_promoter(promoter_name)
                    event.promoters.append(promoter)
            
            # Add extra links
            for link_data in event_data.get('extraLinks', []):
                if link_data.get('href'):
                    link = EventLink(
                        text=link_data.get('text', ''),
                        href=link_data['href']
                    )
                    event.extra_links.append(link)
            
            # Add TBA venue hints if it's a TBA venue
            if venue and venue.is_tba and event_data.get('venue_hints'):
                for hint in event_data['venue_hints']:
                    tba_hint = TBAVenueHint(
                        event_id=event.id,
                        hint_type=hint.get('type'),
                        hint_text=hint.get('text'),
                        confidence=hint.get('confidence', 'low')
                    )
                    self.session.add(tba_hint)
            
            self.session.add(event)
            self.stats['events_migrated'] += 1
            
            return event
            
        except Exception as e:
            error_msg = f"Error migrating event '{event_data.get('title', 'Unknown')}': {e}"
            print(f"❌ {error_msg}")
            self.stats['errors'].append(error_msg)
            return None
    
    def migrate_from_file(self, json_file: str):
        """Migrate events from a JSON file"""
        print(f"📂 Loading events from {json_file}...")
        
        with open(json_file, 'r') as f:
            if 'organized' in json_file:
                # Handle organized format
                data = json.load(f)
                all_events = []
                for date_events in data.get('events_by_date', {}).values():
                    all_events.extend(date_events)
            else:
                # Handle regular array format
                all_events = json.load(f)
        
        print(f"📊 Found {len(all_events)} events to migrate")
        
        # Migrate each event
        for i, event_data in enumerate(all_events, 1):
            if i % 50 == 0:
                print(f"  Processing event {i}/{len(all_events)}...")
                try:
                    self.session.commit()  # Commit in batches
                except Exception as e:
                    print(f"⚠️  Commit failed, rolling back: {e}")
                    self.session.rollback()
            
            try:
                self.migrate_event(event_data)
            except Exception as e:
                print(f"⚠️  Error on event {i}, rolling back: {e}")
                self.session.rollback()
        
        # Final commit
        self.session.commit()
        
        print("\n✅ Migration complete!")
        self.print_stats()
    
    def migrate_tba_hints(self, tba_file: str = "events_tba.json"):
        """Migrate TBA venue hints from separate file"""
        if not Path(tba_file).exists():
            return
        
        print(f"\n📂 Loading TBA hints from {tba_file}...")
        
        with open(tba_file, 'r') as f:
            data = json.load(f)
        
        tba_events = data.get('events', [])
        
        for event_data in tba_events:
            # Find matching event by title and date
            event = self.session.query(Event).filter(
                Event.title == event_data.get('title'),
                Event.date == datetime.strptime(event_data['dateISO'], '%Y-%m-%d').date() if event_data.get('dateISO') else None
            ).first()
            
            if event and event_data.get('venue_hints'):
                for hint in event_data['venue_hints']:
                    # Check if hint already exists
                    existing = self.session.query(TBAVenueHint).filter(
                        TBAVenueHint.event_id == event.id,
                        TBAVenueHint.hint_type == hint.get('type'),
                        TBAVenueHint.hint_text == hint.get('text')
                    ).first()
                    
                    if not existing:
                        tba_hint = TBAVenueHint(
                            event_id=event.id,
                            hint_type=hint.get('type'),
                            hint_text=hint.get('text'),
                            confidence='low'
                        )
                        self.session.add(tba_hint)
        
        self.session.commit()
        print("✅ TBA hints migrated")
    
    def print_stats(self):
        """Print migration statistics"""
        print("\n" + "="*50)
        print("📊 Migration Statistics:")
        print("="*50)
        print(f"  • Events migrated: {self.stats['events_migrated']}")
        print(f"  • Venues created: {self.stats['venues_created']}")
        print(f"  • Genres created: {self.stats['genres_created']}")
        print(f"  • Promoters created: {self.stats['promoters_created']}")
        print(f"  • TBA venues: {self.stats['tba_venues']}")
        
        if self.stats['errors']:
            print(f"\n⚠️  Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:5]:
                print(f"  - {error}")
    
    def verify_migration(self):
        """Verify the migration was successful"""
        print("\n🔍 Verifying migration...")
        
        total_events = self.session.query(Event).count()
        total_venues = self.session.query(Venue).count()
        total_genres = self.session.query(Genre).count()
        total_promoters = self.session.query(Promoter).count()
        tba_venues = self.session.query(Venue).filter(Venue.is_tba == True).count()
        
        print(f"  • Total events in DB: {total_events}")
        print(f"  • Total venues in DB: {total_venues}")
        print(f"  • Total genres in DB: {total_genres}")
        print(f"  • Total promoters in DB: {total_promoters}")
        print(f"  • TBA venues in DB: {tba_venues}")
        
        # Sample query
        sample_events = self.session.query(Event).limit(3).all()
        print("\n📝 Sample events:")
        for event in sample_events:
            print(f"  - {event.title[:50]}")
            print(f"    Date: {event.date}, Venue: {event.venue.name if event.venue else 'None'}")
    
    def close(self):
        """Close database connection"""
        self.session.close()


def main():
    """Main migration function"""
    print("🚀 Starting database migration...")
    print("="*50)
    
    # Initialize migrator
    migrator = EventMigrator("events.db")
    
    try:
        # Determine which file to migrate
        files_to_try = [
            "events_organized.json",
            "events_all_geocoded.json", 
            "events-2025-08-29T19-48-28.json"
        ]
        
        migrated = False
        for file_path in files_to_try:
            if Path(file_path).exists():
                print(f"✅ Found {file_path}")
                migrator.migrate_from_file(file_path)
                migrated = True
                break
        
        if not migrated:
            print("❌ No events file found to migrate!")
            return
        
        # Migrate TBA hints if available
        migrator.migrate_tba_hints()
        
        # Verify migration
        migrator.verify_migration()
        
        print("\n✅ Database migration complete!")
        print(f"📁 Database saved to: events.db")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        raise
    
    finally:
        migrator.close()


if __name__ == "__main__":
    main()