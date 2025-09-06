#!/usr/bin/env python3
"""
Test script to verify Logfire logging is working
Run with: LOGFIRE_WRITE_TOKEN=your_token poetry run python test_logfire.py
"""

import os
import sys

# Check if token is provided
if not os.environ.get('LOGFIRE_WRITE_TOKEN'):
    print("⚠️  No LOGFIRE_WRITE_TOKEN found in environment")
    print("   Logfire will work in development mode but won't send logs to the cloud")
    print("   To test with a real token, run:")
    print("   LOGFIRE_WRITE_TOKEN=your_token poetry run python test_logfire.py")
    print()

import logfire

# Configure Logfire
try:
    logfire.configure()
    print("✅ Logfire configured successfully")
except Exception as e:
    print(f"❌ Failed to configure Logfire: {e}")
    sys.exit(1)

# Test different log levels and structured data
print("\nTesting Logfire logging...")

# 1. Page visit log
logfire.info(
    'Test: Page visit',
    service='web',
    event_type='page_view',
    path='/',
    user_agent='Mozilla/5.0 Test Browser',
    ip='127.0.0.1'
)
print("✅ Logged page visit")

# 2. API request log
logfire.info(
    'Test: API request',
    service='api',
    event_type='api_request',
    endpoint='/api/events',
    filters={'city': 'San Francisco', 'genre': 'electronic'}
)
print("✅ Logged API request")

# 3. Scraping metrics
logfire.info(
    'Test: Scraping completed',
    service='scraper',
    event_type='scrape_complete',
    new_events=15,
    updated_events=234,
    geocoded_venues=3,
    total_processed=249,
    days_ahead=14
)
print("✅ Logged scraping metrics")

# 4. Cron job log
logfire.info(
    'Test: Cron job',
    service='cron',
    event_type='cron_success',
    response={'status': 'success', 'events_count': 249}
)
print("✅ Logged cron job execution")

# 5. Error log
logfire.error(
    'Test: Error example',
    service='scraper',
    event_type='scrape_error',
    error='Connection timeout to 19hz.info'
)
print("✅ Logged error")

print("\n✨ All test logs sent successfully!")
print("   Check your Logfire dashboard to see the logs")