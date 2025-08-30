#!/usr/bin/env python3
import requests
from datetime import datetime
import sys

def fetch_19hz_html():
    """Fetch the HTML from 19hz.info Bay Area events page"""
    
    url = "https://19hz.info/eventlisting_BayArea.php"
    
    # Headers to mimic a real browser request
    # Note: removed Accept-Encoding to get uncompressed response
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }
    
    print(f"🌐 Fetching HTML from: {url}")
    print("-" * 50)
    
    try:
        # Make the request
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Save the HTML
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"19hz_events_{timestamp}.html"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        # Print success info
        print(f"✅ Success!")
        print(f"📊 Response details:")
        print(f"   • Status code: {response.status_code}")
        print(f"   • Content type: {response.headers.get('content-type', 'unknown')}")
        print(f"   • Content length: {len(response.text):,} characters")
        print(f"   • Encoding: {response.encoding}")
        print(f"\n💾 HTML saved to: {filename}")
        
        # Check if it looks like the events page
        if "eventListing" in response.text or "event" in response.text.lower():
            print("✅ Looks like valid events HTML!")
            
            # Quick stats about the content
            import re
            table_count = len(re.findall(r'<table', response.text, re.IGNORECASE))
            tr_count = len(re.findall(r'<tr', response.text, re.IGNORECASE))
            link_count = len(re.findall(r'<a\s+href', response.text, re.IGNORECASE))
            
            print(f"\n📈 Quick analysis:")
            print(f"   • Tables found: {table_count}")
            print(f"   • Table rows found: {tr_count}")
            print(f"   • Links found: {link_count}")
        else:
            print("⚠️  Warning: Content might not be the events page")
        
        return filename
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching the page: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"   Status code: {e.response.status_code}")
            print(f"   Response headers: {dict(e.response.headers)}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def test_cors_options():
    """Test if the server allows CORS requests"""
    url = "https://19hz.info/eventlisting_BayArea.php"
    
    print("\n🔍 Testing CORS headers...")
    print("-" * 50)
    
    try:
        # Try OPTIONS request first
        response = requests.options(url, timeout=10)
        print(f"OPTIONS request status: {response.status_code}")
        
        # Check for CORS headers
        cors_headers = {
            'Access-Control-Allow-Origin': response.headers.get('Access-Control-Allow-Origin', 'Not set'),
            'Access-Control-Allow-Methods': response.headers.get('Access-Control-Allow-Methods', 'Not set'),
            'Access-Control-Allow-Headers': response.headers.get('Access-Control-Allow-Headers', 'Not set')
        }
        
        print("\nCORS Headers:")
        for header, value in cors_headers.items():
            print(f"   • {header}: {value}")
        
        if cors_headers['Access-Control-Allow-Origin'] != 'Not set':
            print("\n✅ Server supports CORS!")
        else:
            print("\n❌ No CORS headers found (expected for most sites)")
            print("   This means browser-based fetching won't work, but Python scripts work fine!")
            
    except Exception as e:
        print(f"Could not test CORS: {e}")

if __name__ == "__main__":
    # Fetch the HTML
    saved_file = fetch_19hz_html()
    
    # Test CORS (just for information)
    test_cors_options()
    
    if saved_file:
        print(f"\n🎉 Done! You can now parse the HTML from: {saved_file}")
        print("\nNext steps:")
        print("1. Parse the HTML to extract event data")
        print("2. Convert to JSON format matching your existing structure")
        print("3. Run geocoding on the new events")
    else:
        print("\n❌ Failed to fetch the HTML")
        sys.exit(1)