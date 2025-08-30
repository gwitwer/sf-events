#!/usr/bin/env python3
import json
import os
from collections import defaultdict

def split_events_by_day(input_file='events-2025-08-29T19-48-28.json', output_dir='events_by_day'):
    # Read the main events file
    with open(input_file, 'r') as f:
        events = json.load(f)
    
    # Group events by date
    events_by_date = defaultdict(list)
    for event in events:
        date = event.get('dateISO', 'unknown')
        events_by_date[date].append(event)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Write each day's events to a separate file
    file_list = []
    for date, day_events in sorted(events_by_date.items()):
        output_file = os.path.join(output_dir, f'events_{date}.json')
        with open(output_file, 'w') as f:
            json.dump(day_events, f, indent=2)
        file_list.append(f'{output_file}: {len(day_events)} events')
    
    # Create an index file with metadata
    index = {
        'total_events': len(events),
        'dates': sorted(events_by_date.keys()),
        'events_per_day': {date: len(events) for date, events in events_by_date.items()},
        'files': [f'events_{date}.json' for date in sorted(events_by_date.keys())]
    }
    
    with open(os.path.join(output_dir, 'index.json'), 'w') as f:
        json.dump(index, f, indent=2)
    
    print(f"✅ Split {len(events)} events into {len(events_by_date)} files")
    print(f"📁 Output directory: {output_dir}/")
    print("\nFiles created:")
    for file_info in file_list:
        print(f"  - {file_info}")
    print(f"  - {output_dir}/index.json (metadata)")

if __name__ == '__main__':
    split_events_by_day()