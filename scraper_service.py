#!/usr/bin/env python3
"""
Background scraper service for fetching and geocoding events
"""

import asyncio
import json
import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from models import (
    Base, Event, Venue, Genre, Promoter, EventLink,
    create_database, get_session
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EventScraper:
    """Scrape events from 19hz.info"""
    
    def __init__(self):
        self.base_url = "https://19hz.info/eventlisting_BayArea.php"
        self.geocode_cache = {}
        
    def fetch_html(self) -> str:
        """Fetch the HTML content from 19hz"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; EventScraper/1.0)'
        }
        
        try:
            response = requests.get(self.base_url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Failed to fetch HTML: {e}")
            raise
    
    def parse_events(self, html_content: str) -> List[Dict]:
        """Parse events from HTML"""
        soup = BeautifulSoup(html_content, 'html.parser')
        events = []
        
        # Find all event rows in the table
        # Events are in TR elements with links
        all_rows = soup.find_all('tr')
        event_rows = [row for row in all_rows if row.find('a') and len(row.find_all('td')) >= 4]
        logger.info(f"Found {len(event_rows)} event rows to parse")
        
        for row in event_rows:
            try:
                event = self.parse_single_event(row)
                if event:
                    events.append(event)
            except Exception as e:
                logger.warning(f"Error parsing event: {e}")
                continue
        
        return events
    
    def parse_single_event(self, row) -> Optional[Dict]:
        """Parse a single event row from table"""
        event = {}
        
        # Get all TD cells
        cells = row.find_all('td')
        if len(cells) < 4:
            return None
        
        # First cell: Date and time
        date_cell = cells[0].get_text(strip=True)
        # Parse date like "Sat: Aug 30"
        date_match = re.search(r'(\w{3}): (\w{3}) (\d{1,2})', date_cell)
        if date_match:
            day_abbr, month_abbr, day_num = date_match.groups()
            event['dayLabel'] = f"{day_abbr}: {month_abbr} {day_num}"
            
            # Convert to ISO date (assuming current year)
            months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                     'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
            month_num = months.get(month_abbr, 1)
            # Check if year is in the last cell (hidden sort column)
            year = 2025  # Default
            if len(cells) > 5:
                year_text = cells[-1].get_text(strip=True)
                year_match = re.search(r'(\d{4})', year_text)
                if year_match:
                    year = int(year_match.group(1))
            
            event['dateISO'] = f"{year:04d}-{month_num:02d}-{int(day_num):02d}"
        
        # Parse time from first cell
        time_match = re.search(r'\((\d{1,2}(?::\d{2})?(?:am|pm)(?:\s*-\s*\d{1,2}(?::\d{2})?(?:am|pm))?)\)', date_cell, re.I)
        if time_match:
            event['timeRange'] = time_match.group(1)
        
        # Second cell: Title, venue, and links
        event_cell = cells[1]
        
        # Get title from first link
        title_link = event_cell.find('a')
        if title_link:
            event['title'] = title_link.get_text(strip=True)
            event['url'] = title_link.get('href', '')
        else:
            # Sometimes title is just text
            event_text = event_cell.get_text(strip=True)
            title_match = re.match(r'^([^@]+)', event_text)
            if title_match:
                event['title'] = title_match.group(1).strip()
        
        # Parse venue and city
        # The venue comes after @ and city is in parentheses
        # Note: HTML may be malformed with missing </td> tags
        event_html = str(event_cell)
        
        # Try to extract venue from raw HTML first (more reliable with malformed HTML)
        venue_match = re.search(r'@\s+([^(<]+?)(?:\s*\(([^)]+)\))?(?:<|$)', event_html)
        if venue_match:
            event['venue'] = venue_match.group(1).strip()
            event['city'] = venue_match.group(2).strip() if venue_match.group(2) else 'San Francisco'
        else:
            # Fallback to text extraction
            event_text = event_cell.get_text(' ', strip=True)
            venue_match = re.search(r'@\s+([^()]+?)(?:\s*\(([^)]+)\))?', event_text)
            if venue_match:
                event['venue'] = venue_match.group(1).strip()
                event['city'] = venue_match.group(2).strip() if venue_match.group(2) else 'San Francisco'
        
        # Third cell: Genres
        if len(cells) > 2:
            genres_text = cells[2].get_text(strip=True)
            # Split by comma
            genres = [g.strip() for g in genres_text.split(',') if g.strip()]
            event['genres'] = genres
        
        # Fourth cell: Price and age
        if len(cells) > 3:
            price_age_text = cells[3].get_text(strip=True)
            
            # Parse price
            price_match = re.search(r'(\$[\d\.\-]+(?: ?[-/] ?\$[\d\.]+)?|free)', price_age_text, re.I)
            if price_match:
                event['price'] = price_match.group(1)
            
            # Parse age
            age_match = re.search(r'(\d+\+|a/a|all ages)', price_age_text, re.I)
            if age_match:
                event['age'] = age_match.group(1)
        
        # Fifth cell: Promoter
        if len(cells) > 4:
            promoter_text = cells[4].get_text(strip=True)
            if promoter_text and promoter_text != '-':
                event['promoters'] = [promoter_text]
            else:
                event['promoters'] = []
        else:
            event['promoters'] = []
        
        # Get extra links from event cell
        extra_links = []
        for link in event_cell.find_all('a')[1:]:  # Skip first link (title)
            href = link.get('href', '')
            link_text = link.get_text(strip=True)
            if href and link_text:
                extra_links.append({'text': link_text, 'href': href})
        event['extraLinks'] = extra_links
        
        event['hidden'] = False
        
        return event
    
    def geocode_venue(self, venue_name: str, city: str) -> Optional[Dict]:
        """Geocode a venue using Nominatim API"""
        # Check cache first
        cache_key = f"{venue_name}|{city}"
        if cache_key in self.geocode_cache:
            return self.geocode_cache[cache_key]
        
        # Skip TBA venues
        if 'TBA' in venue_name or 'TBD' in venue_name:
            return None
        
        # Prepare search query
        search_query = f"{venue_name}, {city}, California"
        
        try:
            # Rate limit: 1 request per second
            time.sleep(1)
            
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': search_query,
                'format': 'jsonv2',
                'limit': 1,
                'countrycodes': 'us'
            }
            headers = {
                'User-Agent': 'SF-Events-Map/1.0'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            results = response.json()
            if results:
                location = results[0]
                coords = {
                    'lat': float(location['lat']),
                    'lon': float(location['lon']),
                    'display_name': location['display_name'],
                    'approximate': 'club' not in location.get('type', '').lower()
                }
                self.geocode_cache[cache_key] = coords
                logger.info(f"Geocoded {venue_name}: {coords['lat']}, {coords['lon']}")
                return coords
            else:
                # Try city center as fallback
                time.sleep(1)
                params['q'] = f"{city}, California"
                response = requests.get(url, params=params, headers=headers, timeout=10)
                results = response.json()
                
                if results:
                    location = results[0]
                    coords = {
                        'lat': float(location['lat']),
                        'lon': float(location['lon']),
                        'display_name': f"{venue_name} (approximate - city center)",
                        'approximate': True
                    }
                    self.geocode_cache[cache_key] = coords
                    return coords
        
        except Exception as e:
            logger.warning(f"Geocoding failed for {venue_name}: {e}")
        
        return None


class DatabaseUpdater:
    """Update database with scraped events"""
    
    def __init__(self, db_session: Session):
        self.session = db_session
        self.venue_cache = {}
        self.genre_cache = {}
        self.promoter_cache = {}
        
    def get_or_create_venue(self, venue_name: str, city: str, coordinates: Optional[Dict] = None) -> Venue:
        """Get existing venue or create new one"""
        cache_key = f"{venue_name}|{city}"
        
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
            venue = Venue(
                name=venue_name,
                city=city,
                is_tba=is_tba
            )
            
            if coordinates:
                venue.latitude = coordinates.get('lat')
                venue.longitude = coordinates.get('lon')
                venue.display_name = coordinates.get('display_name')
                venue.is_approximate = coordinates.get('approximate', False)
            
            self.session.add(venue)
            # Don't flush here - let it happen with the event
        elif coordinates and not venue.latitude:
            # Update coordinates if venue exists but lacks them
            venue.latitude = coordinates.get('lat')
            venue.longitude = coordinates.get('lon')
            venue.display_name = coordinates.get('display_name')
            venue.is_approximate = coordinates.get('approximate', False)
        
        self.venue_cache[cache_key] = venue
        return venue
    
    def get_or_create_genre(self, genre_name: str) -> Genre:
        """Get existing genre or create new one"""
        if genre_name in self.genre_cache:
            return self.genre_cache[genre_name]
        
        genre = self.session.query(Genre).filter(Genre.name == genre_name).first()
        
        if not genre:
            genre = Genre(name=genre_name)
            self.session.add(genre)
            # Don't flush here - let it happen with the event
        
        self.genre_cache[genre_name] = genre
        return genre
    
    def get_or_create_promoter(self, promoter_name: str) -> Promoter:
        """Get existing promoter or create new one"""
        if promoter_name in self.promoter_cache:
            return self.promoter_cache[promoter_name]
        
        promoter = self.session.query(Promoter).filter(Promoter.name == promoter_name).first()
        
        if not promoter:
            promoter = Promoter(name=promoter_name)
            self.session.add(promoter)
            # Don't flush here - let it happen with the event
        
        self.promoter_cache[promoter_name] = promoter
        return promoter
    
    def update_or_create_event(self, event_data: Dict, coordinates: Optional[Dict] = None) -> bool:
        """Update existing event or create new one"""
        try:
            # Parse date
            date_str = event_data.get('dateISO')
            event_date = None
            if date_str:
                event_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Check if event already exists
            existing = self.session.query(Event).filter(
                Event.title == event_data.get('title'),
                Event.date == event_date
            ).first()
            
            # Get or create venue
            venue = None
            if event_data.get('venue'):
                # Only pass coordinates if we need to update them
                venue = self.get_or_create_venue(
                    event_data['venue'],
                    event_data.get('city', 'San Francisco'),
                    coordinates
                )
            
            if existing:
                # Update existing event
                existing.url = event_data.get('url')
                existing.time_range = event_data.get('timeRange')
                existing.price = event_data.get('price')
                existing.age_restriction = event_data.get('age')
                existing.venue = venue
                existing.updated_at = datetime.now()
                
                # Update genres
                existing.genres.clear()
                for genre_name in set(event_data.get('genres', [])):
                    if genre_name:
                        genre = self.get_or_create_genre(genre_name)
                        existing.genres.append(genre)
                
                # Update promoters
                existing.promoters.clear()
                for promoter_name in event_data.get('promoters', []):
                    if promoter_name:
                        promoter = self.get_or_create_promoter(promoter_name)
                        existing.promoters.append(promoter)
                
                logger.info(f"Updated event: {existing.title}")
                return False  # Not new
            
            else:
                # Create new event
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
                
                # Add genres
                for genre_name in set(event_data.get('genres', [])):
                    if genre_name:
                        genre = self.get_or_create_genre(genre_name)
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
                
                self.session.add(event)
                # Flush after adding the event with all its relationships
                self.session.flush()
                logger.info(f"Created new event: {event.title}")
                return True  # New event
                
        except Exception as e:
            logger.error(f"Error updating/creating event: {e}")
            raise


async def scrape_and_update(days_ahead: Optional[int] = None):
    """Main function to scrape events and update database
    
    Args:
        days_ahead: Number of days ahead to process events (default from env or 14 days)
    """
    # Get days_ahead from environment variable or use default
    if days_ahead is None:
        days_ahead = int(os.environ.get('SCRAPE_DAYS_AHEAD', '14'))
    
    logger.info(f"Starting scrape and update process (next {days_ahead} days)...")
    
    try:
        # Initialize scraper
        scraper = EventScraper()
        
        # Fetch and parse events
        logger.info("Fetching HTML from 19hz.info...")
        html_content = scraper.fetch_html()
        
        logger.info("Parsing events...")
        all_events = scraper.parse_events(html_content)
        
        # Filter events to only process those within the specified timeframe
        cutoff_date = (datetime.now() + timedelta(days=days_ahead)).date()
        today = datetime.now().date()
        events = []
        for event in all_events:
            date_str = event.get('dateISO')
            if date_str:
                try:
                    event_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    if today <= event_date <= cutoff_date:
                        events.append(event)
                except ValueError:
                    # If date parsing fails, include it anyway
                    events.append(event)
            else:
                # If no date, include it
                events.append(event)
        
        logger.info(f"Filtered to {len(events)} events (out of {len(all_events)} total) for next {days_ahead} days")
        
        # Connect to database
        engine = create_engine("sqlite:///events.db", echo=False)
        Base.metadata.create_all(engine)
        session = get_session(engine)
        
        # Initialize database updater
        updater = DatabaseUpdater(session)
        
        # Load existing venues to check if we already have coordinates
        venue_coords_cache = {}
        existing_venues = session.query(Venue).filter(
            Venue.latitude.isnot(None),
            Venue.longitude.isnot(None)
        ).all()
        for v in existing_venues:
            cache_key = f"{v.name}|{v.city}"
            venue_coords_cache[cache_key] = {
                'lat': v.latitude,
                'lon': v.longitude,
                'display_name': v.display_name,
                'approximate': v.is_approximate
            }
        logger.info(f"Loaded {len(venue_coords_cache)} venues with existing coordinates")
        
        # Process events
        new_count = 0
        updated_count = 0
        geocoded_count = 0
        
        for i, event_data in enumerate(events, 1):
            if i % 10 == 0:
                logger.info(f"Processing event {i}/{len(events)}...")
            
            try:
                # Check if we need to geocode the venue
                coordinates = None
                venue_name = event_data.get('venue')
                city = event_data.get('city', 'San Francisco')
                
                if venue_name and 'TBA' not in venue_name:
                    cache_key = f"{venue_name}|{city}"
                    
                    # Check if we already have coordinates for this venue
                    if cache_key in venue_coords_cache:
                        coordinates = venue_coords_cache[cache_key]
                        logger.debug(f"Using cached coordinates for {venue_name}")
                    else:
                        # Only geocode if we don't have coordinates yet
                        coordinates = scraper.geocode_venue(venue_name, city)
                        if coordinates:
                            venue_coords_cache[cache_key] = coordinates
                            geocoded_count += 1
                
                # Update or create event
                is_new = updater.update_or_create_event(event_data, coordinates)
                if is_new:
                    new_count += 1
                else:
                    updated_count += 1
                
                # Commit periodically
                if i % 20 == 0:
                    session.commit()
                    
            except Exception as e:
                logger.error(f"Error processing event {i}: {e}")
                session.rollback()
                continue
        
        # Final commit
        session.commit()
        session.close()
        
        logger.info(f"Scraping complete! New events: {new_count}, Updated: {updated_count}, Geocoded venues: {geocoded_count}")
        
        # Clean up old events (older than 6 months)
        cutoff_date = datetime.now().date() - timedelta(days=180)
        session = get_session(engine)
        old_events = session.query(Event).filter(Event.date < cutoff_date).count()
        if old_events > 0:
            session.query(Event).filter(Event.date < cutoff_date).delete()
            session.commit()
            logger.info(f"Cleaned up {old_events} old events")
        session.close()
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise


# Background task runner
async def run_periodic_scraping():
    """Run scraping periodically"""
    while True:
        try:
            # Process events based on configured days ahead (defaults to 14)
            await scrape_and_update()
        except Exception as e:
            logger.error(f"Periodic scraping error: {e}")
        
        # Wait 12 hours
        await asyncio.sleep(12 * 60 * 60)


if __name__ == "__main__":
    # Run scraper once when executed directly
    asyncio.run(scrape_and_update())