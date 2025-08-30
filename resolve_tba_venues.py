#!/usr/bin/env python3
"""
Smart TBA Venue Resolver
Attempts to find actual venues for TBA events using multiple strategies
"""

import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import requests
from bs4 import BeautifulSoup

class TBAResolver:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.promoter_venues = {}
        self.resolved_venues = {}
        
    def load_events(self, filename='events_all_geocoded.json'):
        """Load events and identify TBA venues"""
        with open(filename, 'r') as f:
            events = json.load(f)
        
        # Find TBA events
        self.tba_events = [
            e for e in events 
            if e.get('venue') and re.search(r'TBA|TBD|tba|tbd', e['venue'])
        ]
        
        # Build promoter history
        for event in events:
            if not re.search(r'TBA|TBD', event.get('venue', ''), re.I):
                for promoter in event.get('promoters', []):
                    if promoter not in self.promoter_venues:
                        self.promoter_venues[promoter] = []
                    venue_city = f"{event.get('venue')} ({event.get('city')})"
                    if venue_city not in self.promoter_venues[promoter]:
                        self.promoter_venues[promoter].append(venue_city)
        
        print(f"📊 Found {len(self.tba_events)} TBA events")
        print(f"📚 Built promoter history for {len(self.promoter_venues)} promoters")
        
        return self.tba_events
    
    def strategy_1_promoter_history(self, event):
        """Use promoter's historical venues"""
        promoters = event.get('promoters', [])
        suggestions = []
        
        for promoter in promoters:
            if promoter in self.promoter_venues:
                venues = self.promoter_venues[promoter]
                if venues:
                    suggestions.extend(venues[:3])  # Top 3 venues
        
        if suggestions:
            return {
                'strategy': 'promoter_history',
                'confidence': 'medium',
                'suggestions': suggestions,
                'reason': f"Promoter {', '.join(promoters)} usually uses these venues"
            }
        return None
    
    def strategy_2_event_page_scrape(self, event):
        """Scrape event URL for venue info"""
        url = event.get('url')
        if not url:
            return None
        
        # Skip certain domains that won't have venue info
        if any(domain in url for domain in ['instagram.com', 'facebook.com']):
            return None
        
        try:
            # Only for RA.co links for now (they're reliable)
            if 'ra.co' in url:
                response = self.session.get(url, timeout=5)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for venue info in common patterns
                venue_elem = soup.find('span', string=re.compile(r'venue|location', re.I))
                if venue_elem:
                    venue_text = venue_elem.find_next_sibling()
                    if venue_text:
                        return {
                            'strategy': 'event_page_scrape',
                            'confidence': 'high',
                            'venue': venue_text.get_text().strip(),
                            'source': url
                        }
        except Exception as e:
            pass
        
        return None
    
    def strategy_3_title_analysis(self, event):
        """Extract venue hints from event title"""
        title = event.get('title', '')
        
        # Common patterns in titles
        patterns = [
            r'@\s*([^,]+)',  # @ Venue
            r'at\s+([^,]+)',  # at Venue
            r':\s*([^,]+)',   # Event: Venue
        ]
        
        for pattern in patterns:
            match = re.search(pattern, title, re.I)
            if match:
                potential_venue = match.group(1).strip()
                # Filter out obvious non-venues
                if not re.search(r'TBA|TBD|feat|ft\.|presents', potential_venue, re.I):
                    return {
                        'strategy': 'title_analysis',
                        'confidence': 'low',
                        'venue': potential_venue,
                        'reason': f"Extracted from title: '{title}'"
                    }
        
        return None
    
    def strategy_4_neighborhood_inference(self, event):
        """Infer likely neighborhoods from event type"""
        genres = event.get('genres', [])
        city = event.get('city', '')
        
        if city != 'San Francisco':
            return None
        
        # Genre to neighborhood mapping (SF specific)
        neighborhood_map = {
            'techno': ['SOMA', 'Mission'],
            'house': ['SOMA', 'Mission', 'Castro'],
            'reggaeton': ['Mission', 'SOMA'],
            'latin': ['Mission'],
            'underground': ['SOMA', 'Potrero Hill'],
            'warehouse': ['SOMA', 'Dogpatch']
        }
        
        suggestions = []
        for genre in genres:
            for key, neighborhoods in neighborhood_map.items():
                if key in genre.lower():
                    suggestions.extend(neighborhoods)
        
        if suggestions:
            # Get unique neighborhoods
            unique = list(dict.fromkeys(suggestions))
            return {
                'strategy': 'neighborhood_inference',
                'confidence': 'very_low',
                'neighborhoods': unique[:3],
                'reason': f"Based on genres: {', '.join(genres[:3])}"
            }
        
        return None
    
    def resolve_all(self):
        """Try to resolve all TBA venues"""
        results = []
        
        for i, event in enumerate(self.tba_events, 1):
            print(f"\n[{i}/{len(self.tba_events)}] Resolving: {event.get('title', 'Unknown')[:60]}...")
            
            resolution = {
                'event': event,
                'resolutions': []
            }
            
            # Try each strategy
            strategies = [
                self.strategy_1_promoter_history,
                self.strategy_2_event_page_scrape,
                self.strategy_3_title_analysis,
                self.strategy_4_neighborhood_inference
            ]
            
            for strategy in strategies:
                result = strategy(event)
                if result:
                    resolution['resolutions'].append(result)
                    print(f"  ✓ {result['strategy']}: {result.get('confidence', 'unknown')} confidence")
            
            if not resolution['resolutions']:
                print(f"  ✗ No resolution found")
            
            results.append(resolution)
            
            # Rate limit for scraping
            if i % 5 == 0:
                time.sleep(1)
        
        return results
    
    def save_results(self, results):
        """Save resolution results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tba_resolutions_{timestamp}.json"
        
        # Prepare summary
        summary = {
            'total_tba_events': len(self.tba_events),
            'resolved': len([r for r in results if r['resolutions']]),
            'unresolved': len([r for r in results if not r['resolutions']]),
            'timestamp': timestamp,
            'resolutions': results
        }
        
        with open(filename, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\n📊 Summary:")
        print(f"  • Total TBA events: {summary['total_tba_events']}")
        print(f"  • Resolved: {summary['resolved']}")
        print(f"  • Unresolved: {summary['unresolved']}")
        print(f"  • Results saved to: {filename}")
        
        # Show some examples
        print(f"\n🎯 Sample resolutions:")
        for result in results[:3]:
            if result['resolutions']:
                event = result['event']
                best = result['resolutions'][0]
                print(f"\n  Event: {event.get('title', '')[:50]}")
                print(f"  Strategy: {best['strategy']}")
                print(f"  Result: {best.get('venue') or best.get('suggestions', [])[:2]}")

def main():
    resolver = TBAResolver()
    
    print("🔍 TBA Venue Resolver")
    print("=" * 50)
    
    # Load events
    resolver.load_events()
    
    # Resolve TBA venues
    results = resolver.resolve_all()
    
    # Save results
    resolver.save_results(results)
    
    print("\n✅ Done! You can now:")
    print("  1. Review the resolutions in the JSON file")
    print("  2. Manually verify high-confidence matches")
    print("  3. Update the geocoded events with confirmed venues")

if __name__ == '__main__':
    main()