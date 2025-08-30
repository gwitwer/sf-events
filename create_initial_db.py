#!/usr/bin/env python3
"""
Create an initial database with scraped data
Run this locally and commit the events.db file
"""

import asyncio
from scraper_service import scrape_and_update
from models import create_database

async def main():
    print("Creating initial database with scraped data...")
    
    # Create fresh database
    create_database("events.db")
    
    # Scrape and populate
    await scrape_and_update()
    
    print("Initial database created successfully!")
    print("You can now commit events.db to the repo for faster startup")

if __name__ == "__main__":
    asyncio.run(main())