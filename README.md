# YoutubeAnalyser

A Python tool for analyzing YouTube watch history data exported from Google Takeout, with filtering of YouTube Shorts to analyse watch paterns

## Overview

This project processes YouTube watch history JSON files to identify viewing patterns and statistics. Since Google Takeout doesn't distinguish between regular videos and Shorts, this tool filters them out by scraping channel short pages and caching video IDs in a local database.

## Why This Approach?

The YouTube API has a 50 requests/day limit, making it impractical for analyzing thousands of videos. Checking individual video durations via `yt-dlp` would require downloading metadata for every video (40,000+ in some cases). 

Instead, what I did was:
1. Groups videos by channel
2. Fetches all Shorts IDs from each channel's `/shorts` page (one request per channel)
3. Caches results in a local database
4. Marks videos as Shorts based on the cached data

This reduces ~40,000 potential requests down to ~100-200 channel requests.

## Installation

## Installation

```bash
pip install pandas yt-dlp
```

## Usage

```bash
# Basic usage
python parser.py

# Verbose output
python parser.py -v
```

Place your Google Takeout JSON file at `Verlauf/wiedergabeverlauf.json` before running.

## Future Improvements

- [ ] CLI argument for JSON
- [ ] Support for Parquet/Feather file formats to save dataframe
- [ ] Additional parsing metrics

## Requirements

- Python 3.x
- pandas
- yt-dlp
- sqlite3 (included in Python standard library)
