#!/usr/bin/env python3
"""
Organize events by calendar date and create date-based JSON files
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Any

def load_all_events():
    """Load the geocoded events"""
    geocoded_file = Path("events_all_geocoded.json")
    if geocoded_file.exists():
        with open(geocoded_file, 'r') as f:
            return json.load(f)
    return []

def organize_by_date(events: List[Dict[str, Any]]):
    """Organize events by calendar date"""
    events_by_date = defaultdict(list)
    
    for event in events:
        if event.get('dateISO'):
            date = event['dateISO']
            events_by_date[date].append(event)
    
    return dict(sorted(events_by_date.items()))

def get_date_metadata(events_by_date: Dict[str, List[Dict]]):
    """Get metadata about the date range and available filters"""
    all_cities = set()
    all_genres = set()
    all_venues = set()
    all_promoters = set()
    date_range = {
        'start': None,
        'end': None,
        'dates': []
    }
    
    for date, date_events in events_by_date.items():
        date_range['dates'].append(date)
        
        for event in date_events:
            if not event.get('hidden'):
                if event.get('city'):
                    all_cities.add(event['city'])
                if event.get('venue'):
                    all_venues.add(event['venue'])
                if event.get('genres'):
                    all_genres.update(event['genres'])
                if event.get('promoters'):
                    all_promoters.update(event['promoters'])
    
    date_range['dates'].sort()
    if date_range['dates']:
        date_range['start'] = date_range['dates'][0]
        date_range['end'] = date_range['dates'][-1]
    
    return {
        'date_range': date_range,
        'available_filters': {
            'cities': sorted(list(all_cities)),
            'genres': sorted(list(all_genres)),
            'venues': sorted(list(all_venues)),
            'promoters': sorted(list(all_promoters))
        },
        'statistics': {
            'total_dates': len(date_range['dates']),
            'total_events': sum(len(events) for events in events_by_date.values()),
            'unique_cities': len(all_cities),
            'unique_genres': len(all_genres),
            'unique_venues': len(all_venues),
            'unique_promoters': len(all_promoters)
        }
    }

def identify_tba_events(events: List[Dict[str, Any]]):
    """Identify and categorize TBA events"""
    tba_events = []
    
    for event in events:
        if event.get('venue') and 'TBA' in event['venue'].upper():
            tba_event = event.copy()
            tba_event['is_tba'] = True
            
            # Add resolution hints
            hints = []
            
            # Check if there's a promoter with known venues
            if event.get('promoters'):
                hints.append({
                    'type': 'promoter_history',
                    'text': f"Check {', '.join(event['promoters'][:2])} usual venues"
                })
            
            # Check if venue might be in title
            title = event.get('title', '')
            if '@' in title or ':' in title:
                hints.append({
                    'type': 'title_hint',
                    'text': 'Venue name might be in event title'
                })
            
            # Suggest likely neighborhoods based on genre
            if event.get('genres'):
                genres_lower = [g.lower() for g in event['genres']]
                neighborhoods = []
                
                if any('techno' in g or 'house' in g for g in genres_lower):
                    neighborhoods.append('SOMA')
                if any('latin' in g or 'reggaeton' in g for g in genres_lower):
                    neighborhoods.append('Mission')
                if any('underground' in g or 'warehouse' in g for g in genres_lower):
                    neighborhoods.extend(['SOMA', 'Dogpatch'])
                
                if neighborhoods:
                    unique_neighborhoods = list(dict.fromkeys(neighborhoods))
                    hints.append({
                        'type': 'neighborhood',
                        'text': f"Likely in {', '.join(unique_neighborhoods)}"
                    })
            
            tba_event['venue_hints'] = hints
            tba_events.append(tba_event)
    
    return tba_events

def create_date_index(events_by_date: Dict[str, List[Dict]], metadata: Dict):
    """Create an index file with all dates and metadata"""
    index = {
        'generated_at': datetime.now().isoformat(),
        'metadata': metadata,
        'dates': {}
    }
    
    for date, events in events_by_date.items():
        visible_events = [e for e in events if not e.get('hidden')]
        tba_events = [e for e in visible_events if e.get('venue') and 'TBA' in e['venue'].upper()]
        
        # Get day of week
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        day_name = date_obj.strftime('%A')
        
        index['dates'][date] = {
            'day_of_week': day_name,
            'event_count': len(visible_events),
            'tba_count': len(tba_events),
            'cities': list(set(e.get('city', '') for e in visible_events if e.get('city'))),
            'genres': list(set(g for e in visible_events for g in e.get('genres', [])))[:10],  # Top 10 genres
            'has_events': len(visible_events) > 0
        }
    
    return index

def save_organized_data(events_by_date: Dict[str, List[Dict]], metadata: Dict, tba_events: List[Dict]):
    """Save all organized data"""
    output_dir = Path("events_by_date")
    output_dir.mkdir(exist_ok=True)
    
    # Save individual date files
    for date, events in events_by_date.items():
        filename = output_dir / f"events_{date}.json"
        with open(filename, 'w') as f:
            json.dump(events, f, indent=2)
    
    # Save index file
    index = create_date_index(events_by_date, metadata)
    with open(output_dir / "index.json", 'w') as f:
        json.dump(index, f, indent=2)
    
    # Save TBA events separately
    with open("events_tba.json", 'w') as f:
        json.dump({
            'total': len(tba_events),
            'events': tba_events,
            'generated_at': datetime.now().isoformat()
        }, f, indent=2)
    
    # Save complete organized file
    with open("events_organized.json", 'w') as f:
        json.dump({
            'metadata': metadata,
            'events_by_date': events_by_date,
            'tba_events': tba_events,
            'generated_at': datetime.now().isoformat()
        }, f, indent=2)
    
    return index

def main():
    print("📊 Organizing Events by Calendar Date")
    print("=" * 50)
    
    # Load events
    events = load_all_events()
    print(f"✅ Loaded {len(events)} total events")
    
    # Organize by date
    events_by_date = organize_by_date(events)
    print(f"📅 Events span {len(events_by_date)} unique dates")
    
    # Get metadata
    metadata = get_date_metadata(events_by_date)
    print(f"🏙️  {metadata['statistics']['unique_cities']} cities")
    print(f"🎵 {metadata['statistics']['unique_genres']} genres")
    print(f"📍 {metadata['statistics']['unique_venues']} venues")
    
    # Identify TBA events
    all_events_flat = [e for events in events_by_date.values() for e in events]
    tba_events = identify_tba_events(all_events_flat)
    print(f"❓ {len(tba_events)} TBA venue events identified")
    
    # Save organized data
    index = save_organized_data(events_by_date, metadata, tba_events)
    
    print("\n✅ Data Organization Complete!")
    print("\nCreated files:")
    print("  📁 events_by_date/ - Individual date files")
    print("  📄 events_by_date/index.json - Date index with metadata")
    print("  📄 events_tba.json - All TBA events with hints")
    print("  📄 events_organized.json - Complete organized dataset")
    
    # Show sample dates
    print("\n📅 Sample dates with events:")
    dates = list(events_by_date.keys())[:5]
    for date in dates:
        count = len([e for e in events_by_date[date] if not e.get('hidden')])
        tba_count = len([e for e in events_by_date[date] 
                        if not e.get('hidden') and e.get('venue') and 'TBA' in e['venue'].upper()])
        print(f"  • {date}: {count} events ({tba_count} TBA)")
    
    return index

if __name__ == '__main__':
    main()