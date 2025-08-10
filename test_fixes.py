#!/usr/bin/env python3
# I am in hell right now.
"""
Test the index page fixes on specific works.
This tests the actual scraper logic without downloading everything.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
from vicifons_scraper_optimized import ComprehensiveVicifonsDownloader
import pywikibot


async def test_single_work(downloader, work_title):
    """Test index detection and chapter extraction for a single work."""
    print(f"\n{'='*60}")
    print(f"TESTING FIXES FOR: {work_title}")
    print(f"{'='*60}")
    
    try:
        # Get the page
        page = pywikibot.Page(downloader.site, work_title)
        
        if not page.exists():
            print(f"❌ Page does not exist: {work_title}")
            return
        
        print(f"✅ Page exists: {work_title}")
        
        # Get page text
        page_text = page.text
        
        # Test index detection
        is_index = downloader.is_index_page(page_text)
        print(f"📋 Index detected: {is_index}")
        
        if is_index:
            # Test chapter extraction
            chapters = await downloader.find_chapters_comprehensive(page, page_text)
            print(f"📚 Found {len(chapters)} chapters:")
            
            # Show first 10 chapters
            for i, chapter in enumerate(chapters[:10], 1):
                print(f"  {i}. {chapter}")
                
            if len(chapters) > 10:
                print(f"  ... and {len(chapters) - 10} more")
            
            # Test if first few chapters actually exist
            print(f"\n🔍 Verifying chapter existence:")
            for chapter in chapters[:5]:
                try:
                    chapter_page = pywikibot.Page(downloader.site, chapter)
                    exists = chapter_page.exists()
                    print(f"  {chapter}: {'EXISTS' if exists else 'NOT FOUND'}")
                except Exception as e:
                    print(f"  {chapter}: ERROR - {e}")
        else:
            print(f"❌ Not detected as index page")
            
    except Exception as e:
        print(f"❌ Error testing {work_title}: {e}")


async def main():
    """Test the fixes on our problematic works."""
    print("Testing fixes for index page detection and chapter extraction...")
    
    # Initialize downloader
    downloader = ComprehensiveVicifonsDownloader("test_output", single_folder=True, max_concurrent=1, use_cache=False)
    
    # Test works that we know should work
    test_works = [
        "Aeneis",  # Should work - detected as index, 12 chapters
        "Noctes Atticae",  # Should work - detected as index, 20 chapters
        "Commentarii de bello Gallico",  # Should NOW work - was false negative
    ]
    
    for work in test_works:
        await test_single_work(downloader, work)
        await asyncio.sleep(0.5)  # Be nice to the server


if __name__ == "__main__":
    asyncio.run(main())