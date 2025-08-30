#!/usr/bin/env python3
import json
import time
import datetime
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple

class VenueGeocoder:
    def __init__(self, cache_file='geocode_cache.json', rate_limit_seconds=1.0):
        self.cache_file = cache_file
        self.rate_limit_seconds = rate_limit_seconds
        self.cache = self.load_cache()
        self.last_request_time = 0
        
        # Nominatim requires a User-Agent
        self.headers = {
            'User-Agent': 'SF-Events-Map/1.0 (contact@example.com)'
        }
    
    def load_cache(self) -> Dict:
        """Load existing geocode cache from file"""
        if Path(self.cache_file).exists():
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        return {}
    
    def save_cache(self):
        """Save geocode cache to file"""
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f, indent=2)
        print(f"💾 Saved cache with {len(self.cache)} locations to {self.cache_file}")
    
    def rate_limit(self):
        """Ensure we respect the rate limit"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - elapsed)
        self.last_request_time = time.time()
    
    def geocode_location(self, venue: str, city: str) -> Optional[Dict]:
        """Geocode a single venue/city combination"""
        cache_key = f"{venue}|{city}"
        
        # Check cache first
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Rate limit before making request
        self.rate_limit()
        
        # Try venue + city first
        query = f"{venue}, {city}, CA"
        url = f"https://nominatim.openstreetmap.org/search"
        params = {
            'q': query,
            'format': 'jsonv2',
            'limit': 1,
            'countrycodes': 'us'
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data and len(data) > 0:
                result = {
                    'lat': float(data[0]['lat']),
                    'lon': float(data[0]['lon']),
                    'display_name': data[0]['display_name'],
                    'query': query,
                    'approximate': False
                }
                self.cache[cache_key] = result
                return result
                
        except Exception as e:
            print(f"❌ Error geocoding {venue}, {city}: {e}")
        
        # Fallback: Try just city
        print(f"⚠️  Venue not found, trying city center for: {venue}, {city}")
        self.rate_limit()
        
        city_query = f"{city}, CA"
        params['q'] = city_query
        
        try:
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data and len(data) > 0:
                result = {
                    'lat': float(data[0]['lat']),
                    'lon': float(data[0]['lon']),
                    'display_name': f"{venue} (approximate - city center)",
                    'query': city_query,
                    'approximate': True
                }
                self.cache[cache_key] = result
                return result
                
        except Exception as e:
            print(f"❌ Error geocoding city {city}: {e}")
        
        # Cache the failure too, so we don't retry
        self.cache[cache_key] = None
        return None
    
    def geocode_events(self, events: List[Dict], day_filter: Optional[str] = None) -> Dict:
        """Geocode all venues from events list"""
        # Filter by day if specified
        if day_filter:
            if day_filter.lower() == 'friday':
                events = [e for e in events 
                         if datetime.datetime.strptime(e['dateISO'], '%Y-%m-%d').weekday() == 4]
                print(f"🎯 Filtering for Friday events: {len(events)} events")
            elif day_filter.lower() == 'weekend':
                events = [e for e in events 
                         if datetime.datetime.strptime(e['dateISO'], '%Y-%m-%d').weekday() in [4, 5, 6]]
                print(f"🎯 Filtering for weekend events: {len(events)} events")
        
        # Get unique venues
        unique_venues = {}
        for event in events:
            if event.get('hidden'):
                continue
            key = f"{event['venue']}|{event['city']}"
            if key not in unique_venues:
                unique_venues[key] = {
                    'venue': event['venue'],
                    'city': event['city'],
                    'events': []
                }
            unique_venues[key]['events'].append(event)
        
        print(f"📍 Found {len(unique_venues)} unique venues to geocode")
        
        # Geocode each unique venue
        results = {
            'successful': 0,
            'failed': 0,
            'approximate': 0,
            'events_with_coords': []
        }
        
        for i, (key, venue_info) in enumerate(unique_venues.items(), 1):
            venue = venue_info['venue']
            city = venue_info['city']
            
            print(f"[{i}/{len(unique_venues)}] Geocoding: {venue}, {city}")
            
            location = self.geocode_location(venue, city)
            
            if location:
                results['successful'] += 1
                if location.get('approximate'):
                    results['approximate'] += 1
                
                # Add coordinates to all events at this venue
                for event in venue_info['events']:
                    event_with_coords = event.copy()
                    event_with_coords['coordinates'] = {
                        'lat': location['lat'],
                        'lon': location['lon'],
                        'display_name': location['display_name'],
                        'approximate': location.get('approximate', False)
                    }
                    results['events_with_coords'].append(event_with_coords)
            else:
                results['failed'] += 1
                print(f"   ❌ Failed to geocode: {venue}, {city}")
            
            # Save cache periodically
            if i % 10 == 0:
                self.save_cache()
        
        # Final save
        self.save_cache()
        
        return results

def geocode_day(geocoder, all_events, day_name, weekday_num):
    """Geocode events for a specific day"""
    # Filter events for this day
    day_events = [e for e in all_events 
                  if datetime.datetime.strptime(e['dateISO'], '%Y-%m-%d').weekday() == weekday_num]
    
    print(f"\n🗺️  Geocoding {day_name} events...")
    print(f"📅 Found {len(day_events)} {day_name} events")
    
    # Get unique venues for this day
    unique_venues = {}
    for event in day_events:
        if event.get('hidden'):
            continue
        key = f"{event['venue']}|{event['city']}"
        if key not in unique_venues:
            unique_venues[key] = {
                'venue': event['venue'],
                'city': event['city'],
                'events': []
            }
        unique_venues[key]['events'].append(event)
    
    print(f"📍 {len(unique_venues)} unique venues for {day_name}")
    
    # Geocode
    results = {
        'successful': 0,
        'failed': 0,
        'approximate': 0,
        'events_with_coords': []
    }
    
    for i, (key, venue_info) in enumerate(unique_venues.items(), 1):
        venue = venue_info['venue']
        city = venue_info['city']
        
        print(f"[{day_name} {i}/{len(unique_venues)}] Geocoding: {venue}, {city}")
        
        location = geocoder.geocode_location(venue, city)
        
        if location:
            results['successful'] += 1
            if location.get('approximate'):
                results['approximate'] += 1
            
            # Add coordinates to all events at this venue
            for event in venue_info['events']:
                event_with_coords = event.copy()
                event_with_coords['coordinates'] = {
                    'lat': location['lat'],
                    'lon': location['lon'],
                    'display_name': location['display_name'],
                    'approximate': location.get('approximate', False)
                }
                results['events_with_coords'].append(event_with_coords)
        else:
            results['failed'] += 1
            print(f"   ❌ Failed to geocode: {venue}, {city}")
        
        # Save cache periodically
        if i % 10 == 0:
            geocoder.save_cache()
    
    # Save results to file
    output_file = f'events_{day_name.lower()}_geocoded.json'
    with open(output_file, 'w') as f:
        json.dump(results['events_with_coords'], f, indent=2)
    
    print(f"\n✅ {day_name} complete: {len(results['events_with_coords'])} events saved to {output_file}")
    return results

def main():
    # Load all events
    print("📂 Loading events...")
    with open('events-2025-08-29T19-48-28.json', 'r') as f:
        all_events = json.load(f)
    
    # Initialize geocoder
    geocoder = VenueGeocoder()
    
    # Geocode Friday events (weekday 4)
    friday_results = geocode_day(geocoder, all_events, 'Friday', 4)
    
    # Geocode Saturday events (weekday 5)
    saturday_results = geocode_day(geocoder, all_events, 'Saturday', 5)
    
    # Final save of cache
    geocoder.save_cache()
    
    # Print summary
    print("\n" + "="*50)
    print("✅ GEOCODING COMPLETE!")
    print("="*50)
    print(f"📊 Friday Results:")
    print(f"   • Total events: {len(friday_results['events_with_coords'])}")
    print(f"   • Successful geocodes: {friday_results['successful']}")
    print(f"   • Approximate: {friday_results['approximate']}")
    print(f"   • Failed: {friday_results['failed']}")
    print(f"\n📊 Saturday Results:")
    print(f"   • Total events: {len(saturday_results['events_with_coords'])}")
    print(f"   • Successful geocodes: {saturday_results['successful']}")
    print(f"   • Approximate: {saturday_results['approximate']}")
    print(f"   • Failed: {saturday_results['failed']}")
    print(f"\n📁 Output files:")
    print(f"   • Friday events: events_friday_geocoded.json")
    print(f"   • Saturday events: events_saturday_geocoded.json")
    print(f"   • Geocode cache: geocode_cache.json")

if __name__ == '__main__':
    main()