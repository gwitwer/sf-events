#!/usr/bin/env python3
"""
Trigger scraping via API call - works with Render's architecture
"""

import os
import sys
import time
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def trigger_scrape():
    """Trigger scraping via API endpoint"""
    # Get the service URL from environment or use default
    service_url = os.environ.get('SERVICE_URL', 'https://sf-events.onrender.com')
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to trigger scrape at {service_url}/api/scrape (attempt {attempt + 1})")
            
            # Wake up the service first if it's sleeping
            wake_response = requests.get(f"{service_url}/health", timeout=30)
            logger.info(f"Health check response: {wake_response.status_code}")
            
            # Give it a moment to fully wake up
            time.sleep(2)
            
            # Trigger the scrape
            response = requests.post(f"{service_url}/api/scrape", timeout=300)
            response.raise_for_status()
            
            logger.info(f"Scrape triggered successfully: {response.json()}")
            return True
            
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(10)  # Wait before retry
            else:
                raise
    
    return False

if __name__ == "__main__":
    try:
        if trigger_scrape():
            logger.info("Scheduled scrape completed successfully!")
            sys.exit(0)
        else:
            logger.error("Scheduled scrape failed!")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)