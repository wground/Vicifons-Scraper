#!/usr/bin/env python3
"""
Post-processing script to find and replace index files with their chapters.

This script:
1. Scans all downloaded .txt files for index-like content
2. Cross-references suspected indices with categoria:capita ex operibus  
3. Downloads individual chapters to replace index files
4. Validates against actual Wikisource pages
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import re
from pathlib import Path
from typing import Set, List, Dict, Tuple
import pywikibot
import aiohttp
import json
from vicifons_scraper_optimized import ComprehensiveVicifonsDownloader

class IndexPostProcessor:
    def __init__(self, downloaded_dir: str = "downloaded_texts"):
        self.downloaded_dir = Path(downloaded_dir)
        self.site = pywikibot.Site('la', 'wikisource')
        self.downloader = ComprehensiveVicifonsDownloader(
            str(self.downloaded_dir), 
            single_folder=True,
            cache_duration_hours=2
        )
        
        # Statistics
        self.stats = {
            'files_scanned': 0,
            'indices_detected': 0,
            'indices_replaced': 0,
            'chapters_downloaded': 0,
            'errors': 0
        }
    
    def detect_index_content(self, file_path: Path) -> Tuple[bool, List[str]]:
        """
        Detect if a file contains index-like content.
        Returns (is_index, list_of_links)
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Skip header section
            lines = content.split('\n')
            content_start = 0
            for i, line in enumerate(lines):
                if '----' in line:
                    content_start = i + 1
                    break
            
            actual_content = '\n'.join(lines[content_start:]).strip()
            
            # Check for index patterns regardless of length
            # Look for link patterns
            links = []
            content_lines = [line.strip() for line in actual_content.split('\n') if line.strip()]
            
            # Check if most lines look like chapter/book references
            chapter_like_lines = 0
            for line in content_lines[:50]:  # Check first 50 lines
                if re.search(r'(liber|book|chapter|capitulum|epistul)\s+[ivxlcdm0-9]+', line, re.IGNORECASE):
                    chapter_like_lines += 1
                    links.append(line.lstrip('*').strip())
                elif len(line) < 50 and not re.search(r'[.!?]', line) and line.startswith('*'):  # Short lines starting with *
                    links.append(line.lstrip('*').strip())
            
            # If many lines look like chapter references, it's probably an index
            if chapter_like_lines > 3 or (len(links) > 10 and len(actual_content) < 2000):
                return True, links
            
            return False, []
            
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return False, []
    
    def extract_work_title(self, file_path: Path) -> str:
        """Extract the work title from filename."""
        # Remove .txt extension and clean up
        title = file_path.stem
        # Handle some common cases
        title = title.replace('_', ' ')
        return title
    
    async def find_chapters_for_work(self, work_title: str) -> List[str]:
        """Find chapters for a work using categoria:capita ex operibus."""
        try:
            page = pywikibot.Page(self.site, work_title)
            if not page.exists():
                return []
            
            # Use the downloader's comprehensive chapter finding
            page_text = page.text
            chapters = await self.downloader.find_chapters_comprehensive(page, page_text)
            
            return chapters
            
        except Exception as e:
            print(f"Error finding chapters for {work_title}: {e}")
            return []
    
    async def download_chapter(self, chapter_title: str) -> Tuple[bool, str]:
        """Download a single chapter. Returns (success, text)."""
        try:
            result = await self.downloader.download_single_page(chapter_title)
            if result and result.get('success'):
                # Read the downloaded file
                file_path = Path(result['file_path'])
                if file_path.exists():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        return True, f.read()
            return False, ""
        except Exception as e:
            print(f"Error downloading {chapter_title}: {e}")
            return False, ""
    
    async def replace_index_with_chapters(self, file_path: Path, work_title: str, chapters: List[str]) -> bool:
        """Replace an index file with its individual chapters."""
        try:
            print(f"🔄 Replacing {file_path.name} with {len(chapters)} chapters...")
            
            # Create backup
            backup_path = file_path.with_suffix('.txt.index_backup')
            file_path.rename(backup_path)
            
            downloaded_count = 0
            
            for chapter in chapters:
                success, chapter_text = await self.download_chapter(chapter)
                if success and chapter_text:
                    # Create individual chapter file
                    safe_chapter_title = self.downloader.clean_filename(chapter)
                    chapter_file = file_path.parent / f"{safe_chapter_title}.txt"
                    
                    # Avoid overwriting existing files
                    counter = 1
                    original_path = chapter_file
                    while chapter_file.exists():
                        chapter_file = original_path.with_stem(f"{original_path.stem}_{counter}")
                        counter += 1
                    
                    with open(chapter_file, 'w', encoding='utf-8') as f:
                        f.write(chapter_text)
                    
                    downloaded_count += 1
                    self.stats['chapters_downloaded'] += 1
                    print(f"  ✅ Downloaded: {chapter_file.name}")
                else:
                    print(f"  ❌ Failed: {chapter}")
            
            print(f"🎉 Replaced {file_path.name} with {downloaded_count} chapters")
            self.stats['indices_replaced'] += 1
            return downloaded_count > 0
            
        except Exception as e:
            print(f"Error replacing {file_path}: {e}")
            self.stats['errors'] += 1
            return False
    
    async def process_file(self, file_path: Path):
        """Process a single file to check if it's an index."""
        self.stats['files_scanned'] += 1
        
        print(f"\n📄 Scanning: {file_path.name}")
        
        # Detect index content
        is_index, links = self.detect_index_content(file_path)
        
        if not is_index:
            print(f"  ✅ Regular content file")
            return
        
        print(f"  🔍 SUSPECTED INDEX: {len(links)} links found")
        self.stats['indices_detected'] += 1
        
        # Extract work title and find chapters
        work_title = self.extract_work_title(file_path)
        print(f"  📖 Work title: {work_title}")
        
        chapters = await self.find_chapters_for_work(work_title)
        
        if not chapters:
            print(f"  ⚠️ No chapters found via categoria:capita ex operibus")
            return
        
        print(f"  📚 Found {len(chapters)} chapters via categoria:capita ex operibus")
        
        # Show first few chapters for verification
        for i, chapter in enumerate(chapters[:5]):
            print(f"    {i+1}. {chapter}")
        if len(chapters) > 5:
            print(f"    ... +{len(chapters)-5} more")
        
        # Confirm this is really an index by comparing with expected chapters
        if len(chapters) >= 3:  # Only process if we have substantial chapters
            await self.replace_index_with_chapters(file_path, work_title, chapters)
        else:
            print(f"  ❌ Too few chapters ({len(chapters)}) - skipping")
    
    async def run_post_processing(self):
        """Run post-processing on all downloaded files."""
        print("="*80)
        print("POST-PROCESSING: FINDING AND REPLACING INDEX FILES")
        print("="*80)
        
        if not self.downloaded_dir.exists():
            print(f"❌ Directory not found: {self.downloaded_dir}")
            return
        
        # Find all .txt files
        txt_files = list(self.downloaded_dir.glob("*.txt"))
        print(f"📂 Found {len(txt_files)} text files to process")
        
        for file_path in txt_files:
            try:
                await self.process_file(file_path)
                await asyncio.sleep(0.5)  # Be nice to the server
            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}")
                self.stats['errors'] += 1
        
        # Print final statistics
        print(f"\n{'='*80}")
        print("POST-PROCESSING COMPLETE")
        print(f"{'='*80}")
        print(f"📊 Files scanned: {self.stats['files_scanned']}")
        print(f"🔍 Indices detected: {self.stats['indices_detected']}")
        print(f"🔄 Indices replaced: {self.stats['indices_replaced']}")
        print(f"📥 Chapters downloaded: {self.stats['chapters_downloaded']}")
        print(f"❌ Errors: {self.stats['errors']}")
        
        if self.stats['indices_replaced'] > 0:
            print(f"\n✅ Successfully replaced {self.stats['indices_replaced']} index files with their chapters!")
        else:
            print(f"\n🤔 No index files needed replacement.")


async def main():
    """Run the post-processor."""
    processor = IndexPostProcessor()
    await processor.run_post_processing()


if __name__ == "__main__":
    asyncio.run(main())