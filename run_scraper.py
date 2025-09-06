#!/usr/bin/env python3
"""
Trigger scraping via API call - works with Render's architecture
"""

import os
import sys
import time
import requests
import logging
import logfire

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Logfire if token is available
if os.environ.get('LOGFIRE_WRITE_TOKEN'):
    logfire.configure(token=os.environ.get('LOGFIRE_WRITE_TOKEN'))
    logger.info('Logfire initialized for cron job')

def trigger_scrape():
    """Trigger scraping via API endpoint"""
    # Get the service URL from environment or use default
    service_url = os.environ.get('SERVICE_URL', 'https://sf-events.onrender.com')
    
    logfire.info(
        'Cron job started',
        service='cron',
        event_type='cron_start',
        service_url=service_url
    )
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to trigger scrape at {service_url}/api/scrape (attempt {attempt + 1})")
            
            # Wake up the service first if it's sleeping
            wake_response = requests.get(f"{service_url}/health", timeout=60)
            logger.info(f"Health check response: {wake_response.status_code}")
            
            logfire.info(
                'Health check',
                service='cron',
                event_type='health_check',
                status_code=wake_response.status_code,
                attempt=attempt + 1
            )
            
            # Give it a moment to fully wake up
            time.sleep(2)
            
            # Trigger the scrape with longer timeout (10 minutes)
            response = requests.post(
                f"{service_url}/api/scrape", 
                timeout=600,
                headers={'User-Agent': 'Python/CronJob'}  # Identify as cron
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"Scrape triggered successfully: {result}")
            
            logfire.info(
                'Cron job completed',
                service='cron',
                event_type='cron_success',
                response=result
            )
            return True
            
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            logfire.warning(
                'Cron attempt failed',
                service='cron',
                event_type='cron_attempt_failed',
                attempt=attempt + 1,
                error=str(e)
            )
            if attempt < max_retries - 1:
                time.sleep(10)  # Wait before retry
            else:
                logfire.error(
                    'Cron job failed',
                    service='cron',
                    event_type='cron_failed',
                    error=str(e)
                )
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