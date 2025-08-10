#!/usr/bin/env python3
#This feels like I'm in hell.
"""
Quick test of a few key problematic works.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
from vicifons_scraper_optimized import ComprehensiveVicifonsDownloader
import pywikibot

async def quick_test():
    """Test just a few key works."""
    downloader = ComprehensiveVicifonsDownloader("quick_test_output", cache_duration_hours=1)
    
    # Key test cases
    test_cases = [
        "Aeneis",  # Should be index with 12 chapters
        "Ab Urbe Condita - Periochae",  # The problematic case you mentioned
        "Commentarii de bello Gallico",  # Should be index
        "Cato Maior de Senectute",  # Should be single page
    ]
    
    for work in test_cases:
        print(f"\n{'='*60}")
        print(f"TESTING: {work}")
        print(f"{'='*60}")
        
        try:
            page = pywikibot.Page(downloader.site, work)
            if not page.exists():
                print(f"❌ Does not exist: {work}")
                continue
                
            page_text = page.text
            is_index = downloader.is_index_page(page_text)
            
            if is_index:
                chapters = await downloader.find_chapters_comprehensive(page, page_text)
                print(f"📋 INDEX: {len(chapters)} chapters found")
                
                # Show first few
                for i, chapter in enumerate(chapters[:5]):
                    print(f"  {i+1}. {chapter}")
                if len(chapters) > 5:
                    print(f"  ... +{len(chapters)-5} more")
                    
                # Test download one chapter
                if chapters:
                    print(f"\n📥 Testing download of: {chapters[0]}")
                    result = await downloader.download_single_page(chapters[0])
                    if result and result.get('success'):
                        print(f"✅ Downloaded successfully ({result.get('text_length', 0)} chars)")
                    else:
                        print(f"❌ Download failed: {result.get('error', 'unknown')}")
            else:
                print(f"📄 SINGLE PAGE")
                print(f"\n📥 Testing download...")
                result = await downloader.download_single_page(work)
                if result and result.get('success'):
                    print(f"✅ Downloaded successfully ({result.get('text_length', 0)} chars)")
                else:
                    print(f"❌ Download failed: {result.get('error', 'unknown')}")
                    
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(quick_test())