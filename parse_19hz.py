#!/usr/bin/env python3
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import sys

def normalize_text(element):
    """Normalize text by replacing <br> with spaces and cleaning whitespace"""
    if not element:
        return ""
    
    # Clone and replace <br> tags with spaces
    html = str(element)
    html = re.sub(r'<br\s*/?>', ' ', html)
    
    # Parse and get text
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text()
    
    # Clean whitespace
    text = re.sub(r'\s+\n', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip()

def parse_19hz_html(html_file, include_hidden=True):
    """Parse 19hz HTML file and extract event data"""
    
    print(f"📂 Opening HTML file: {html_file}")
    
    # Read HTML file
    with open(html_file, 'r', encoding='utf-8', errors='ignore') as f:
        html_content = f.read()
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find tbody
    tbody = soup.find('tbody')
    if not tbody:
        print("❌ No <tbody> found in HTML")
        return []
    
    # Get all rows
    rows = tbody.find_all('tr')
    print(f"📊 Found {len(rows)} table rows")
    
    events = []
    
    for row in rows:
        # Check if row is hidden
        style = row.get('style', '')
        is_hidden = 'display: none' in style or 'display:none' in style
        
        if not include_hidden and is_hidden:
            continue
        
        tds = row.find_all('td')
        if len(tds) < 7:
            continue  # Skip rows with insufficient columns
        
        # TD0: Day and time (e.g., "Fri: Aug 29 (3pm-9pm)")
        td0_text = normalize_text(tds[0])
        day_label = None
        time_range = None
        
        if td0_text:
            match = re.match(r'^(.+?)\s*\((.+?)\)\s*$', td0_text)
            if match:
                day_label = match.group(1).strip()
                time_range = match.group(2).strip()
            else:
                day_label = td0_text
        
        # TD1: Title, URL, Venue, City
        td1 = tds[1]
        link = td1.find('a') if td1 else None
        title = link.get_text().strip() if link else normalize_text(td1)
        url = link.get('href') if link else None
        
        # Extract venue and city
        venue = None
        city = None
        if td1:
            full_text = normalize_text(td1)
            # Remove title to get venue/city part
            after_title = full_text.replace(title, '', 1).strip() if title else full_text
            
            # Match "@ Venue (City)" pattern
            venue_match = re.match(r'@\s*(.*?)\s*(?:\((.*?)\))?\s*$', after_title)
            if venue_match:
                venue = venue_match.group(1).strip() if venue_match.group(1) else None
                city = venue_match.group(2).strip() if venue_match.group(2) else None
        
        # TD2: Genres
        genres_text = normalize_text(tds[2]) if len(tds) > 2 else ""
        genres = [g.strip() for g in genres_text.split(',') if g.strip()]
        
        # TD3: Price | Age
        price = None
        age = None
        if len(tds) > 3:
            price_age_text = normalize_text(tds[3])
            parts = [p.strip() for p in price_age_text.split('|') if p.strip()]
            
            if len(parts) == 1:
                # Determine if it's age or price
                if re.search(r'\d+\+|all ages', parts[0], re.IGNORECASE):
                    age = parts[0]
                else:
                    price = parts[0]
            elif len(parts) >= 2:
                price = parts[0]
                age = parts[1]
        
        # TD4: Promoters
        promoters_text = normalize_text(tds[4]) if len(tds) > 4 else ""
        promoters = [p.strip() for p in promoters_text.split(',') if p.strip()]
        
        # TD5: Extra links
        extra_links = []
        if len(tds) > 5:
            links = tds[5].find_all('a')
            extra_links = [
                {'text': link.get_text().strip(), 'href': link.get('href')}
                for link in links
            ]
        
        # TD6: Date (YYYY/MM/DD format)
        date_iso = None
        if len(tds) > 6:
            shrink = tds[6].find(class_='shrink')
            if shrink:
                date_text = shrink.get_text().strip()
                # Convert YYYY/MM/DD to YYYY-MM-DD
                date_iso = date_text.replace('/', '-')
            else:
                date_text = normalize_text(tds[6])
                if date_text:
                    date_iso = date_text.replace('/', '-')
        
        # Build event object
        event = {
            'hidden': is_hidden,
            'className': row.get('class', [None])[0] if row.get('class') else None,
            'dayLabel': day_label,
            'timeRange': time_range,
            'title': title,
            'url': url,
            'venue': venue,
            'city': city,
            'genres': genres,
            'price': price,
            'age': age,
            'promoters': promoters,
            'extraLinks': extra_links,
            'dateISO': date_iso
        }
        
        events.append(event)
    
    return events

def main():
    """Main function to parse 19hz HTML and save as JSON"""
    
    # Find the most recent HTML file
    html_files = list(Path('.').glob('19hz_events_*.html'))
    
    if not html_files:
        print("❌ No 19hz HTML files found. Run fetch_19hz.py first!")
        sys.exit(1)
    
    # Use the most recent file
    html_file = sorted(html_files)[-1]
    print(f"📄 Using HTML file: {html_file}")
    
    # Parse HTML
    print("\n🔍 Parsing HTML...")
    events = parse_19hz_html(html_file, include_hidden=True)
    
    # Filter stats
    visible_events = [e for e in events if not e['hidden']]
    hidden_events = [e for e in events if e['hidden']]
    
    print(f"\n📊 Parsing Results:")
    print(f"   • Total events: {len(events)}")
    print(f"   • Visible events: {len(visible_events)}")
    print(f"   • Hidden events: {len(hidden_events)}")
    
    # Date range
    dates = [e['dateISO'] for e in events if e['dateISO']]
    if dates:
        dates.sort()
        print(f"   • Date range: {dates[0]} to {dates[-1]}")
    
    # Venue/city stats
    venues = set((e['venue'], e['city']) for e in visible_events if e['venue'])
    cities = set(e['city'] for e in visible_events if e['city'])
    
    print(f"   • Unique venues: {len(venues)}")
    print(f"   • Cities: {len(cities)}")
    
    # Save to JSON
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_file = f"19hz_events_parsed_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(events, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Saved {len(events)} events to: {output_file}")
    
    # Also save a "latest" version for convenience
    latest_file = "19hz_events_latest.json"
    with open(latest_file, 'w', encoding='utf-8') as f:
        json.dump(events, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Also saved as: {latest_file}")
    
    # Sample output
    print("\n📝 Sample event:")
    if visible_events:
        print(json.dumps(visible_events[0], indent=2))
    
    return output_file

if __name__ == '__main__':
    try:
        output_file = main()
        print(f"\n🎉 Success! You can now:")
        print(f"   1. Run geocoding: python3 geocode_all_events.py")
        print(f"   2. View on map: open index.html")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)