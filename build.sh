#!/usr/bin/env bash
# Build script for Render deployment

set -e

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Setting up database..."
# The database will be created and populated on server startup
# via the scraper_service which runs automatically

echo "Build complete!"