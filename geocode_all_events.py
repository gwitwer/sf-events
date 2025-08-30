#!/usr/bin/env python3
import json
import time
import requests
from pathlib import Path
from typing import Dict, Optional

class SmartGeocoder:
    def __init__(self, cache_file='geocode_cache.json', rate_limit_seconds=1.0):
        self.cache_file = cache_file
        self.rate_limit_seconds = rate_limit_seconds
        self.cache = self.load_cache()
        self.last_request_time = 0
        self.new_geocodes = 0
        self.cache_hits = 0
        
        # Nominatim requires a User-Agent
        self.headers = {
            'User-Agent': 'SF-Events-Map/1.0 (contact@example.com)'
        }
    
    def load_cache(self) -> Dict:
        """Load existing geocode cache from file"""
        if Path(self.cache_file).exists():
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
                print(f"📂 Loaded cache with {len(cache)} locations")
                return cache
        return {}
    
    def save_cache(self):
        """Save geocode cache to file"""
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f, indent=2)
        print(f"💾 Saved cache with {len(self.cache)} locations")
    
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
            self.cache_hits += 1
            return self.cache[cache_key]
        
        # Rate limit before making request
        self.rate_limit()
        self.new_geocodes += 1
        
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

def main():
    # Load all events
    print("="*60)
    print("🗺️  SF EVENTS GEOCODER - COMPLETE DATASET")
    print("="*60)
    
    input_file = 'events-2025-08-29T19-48-28.json'
    output_file = 'events_all_geocoded.json'
    
    print(f"\n📂 Loading events from {input_file}...")
    with open(input_file, 'r') as f:
        all_events = json.load(f)
    
    # Filter out hidden events
    visible_events = [e for e in all_events if not e.get('hidden', False)]
    print(f"📊 Found {len(visible_events)} visible events (out of {len(all_events)} total)")
    
    # Get unique venues
    unique_venues = {}
    for event in visible_events:
        key = f"{event['venue']}|{event['city']}"
        if key not in unique_venues:
            unique_venues[key] = {
                'venue': event['venue'],
                'city': event['city'],
                'events': []
            }
        unique_venues[key]['events'].append(event)
    
    print(f"📍 Found {len(unique_venues)} unique venue/city combinations")
    
    # Initialize geocoder
    geocoder = SmartGeocoder()
    
    # Check how many we already have cached
    already_cached = sum(1 for key in unique_venues.keys() if key in geocoder.cache)
    to_geocode = len(unique_venues) - already_cached
    
    print(f"\n✅ Already cached: {already_cached} locations")
    print(f"🔄 Need to geocode: {to_geocode} new locations")
    
    if to_geocode > 0:
        estimated_time = to_geocode * geocoder.rate_limit_seconds
        print(f"⏱️  Estimated time: {estimated_time:.0f} seconds ({estimated_time/60:.1f} minutes)")
    
    print("\n" + "-"*40)
    print("Starting geocoding process...")
    print("-"*40 + "\n")
    
    # Geocode each unique venue
    events_with_coords = []
    successful = 0
    failed = 0
    approximate = 0
    
    for i, (key, venue_info) in enumerate(unique_venues.items(), 1):
        venue = venue_info['venue']
        city = venue_info['city']
        
        # Show progress
        if key not in geocoder.cache:
            print(f"[{i}/{len(unique_venues)}] Geocoding NEW: {venue}, {city}")
        
        location = geocoder.geocode_location(venue, city)
        
        if location:
            successful += 1
            if location and location.get('approximate'):
                approximate += 1
            
            # Add coordinates to all events at this venue
            for event in venue_info['events']:
                event_with_coords = event.copy()
                event_with_coords['coordinates'] = {
                    'lat': location['lat'],
                    'lon': location['lon'],
                    'display_name': location['display_name'],
                    'approximate': location.get('approximate', False)
                }
                events_with_coords.append(event_with_coords)
        else:
            failed += 1
            # Still include the event without coordinates
            for event in venue_info['events']:
                event_with_coords = event.copy()
                event_with_coords['coordinates'] = None
                events_with_coords.append(event_with_coords)
        
        # Save cache periodically (every 10 new geocodes)
        if geocoder.new_geocodes > 0 and geocoder.new_geocodes % 10 == 0:
            geocoder.save_cache()
    
    # Final save of cache
    geocoder.save_cache()
    
    # Save all geocoded events
    with open(output_file, 'w') as f:
        json.dump(events_with_coords, f, indent=2)
    
    # Print summary
    print("\n" + "="*60)
    print("✅ GEOCODING COMPLETE!")
    print("="*60)
    print(f"\n📊 Results:")
    print(f"   • Total events processed: {len(events_with_coords)}")
    print(f"   • Unique locations: {len(unique_venues)}")
    print(f"   • Successful geocodes: {successful}")
    print(f"   • Approximate locations: {approximate}")
    print(f"   • Failed geocodes: {failed}")
    print(f"\n🚀 Performance:")
    print(f"   • Cache hits: {geocoder.cache_hits}")
    print(f"   • New geocodes: {geocoder.new_geocodes}")
    print(f"   • Cache efficiency: {(geocoder.cache_hits / len(unique_venues) * 100):.1f}%")
    print(f"\n📁 Output files:")
    print(f"   • Complete geocoded events: {output_file}")
    print(f"   • Geocode cache: {geocoder.cache_file}")
    print(f"\n✨ The file '{output_file}' contains ALL events with coordinates!")
    print("   You can now load this file directly without any geocoding delays.")

if __name__ == '__main__':
    main()