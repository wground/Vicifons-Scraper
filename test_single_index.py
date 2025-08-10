#!/usr/bin/env python3
"""
Test post-processing on a single index file.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
from pathlib import Path
from post_process_indices import IndexPostProcessor

async def test_single_file():
    """Test the post-processor on Ab Urbe Condita - Periochae.txt"""
    processor = IndexPostProcessor()
    
    test_file = Path("downloaded_texts/Ab Urbe Condita - Periochae.txt")
    
    if not test_file.exists():
        print(f"File not found: {test_file}")
        return
    
    print("Testing post-processor on Ab Urbe Condita - Periochae.txt...")
    await processor.process_file(test_file)
    
    print(f"\nStatistics:")
    print(f"Files scanned: {processor.stats['files_scanned']}")
    print(f"Indices detected: {processor.stats['indices_detected']}")
    print(f"Indices replaced: {processor.stats['indices_replaced']}")
    print(f"Chapters downloaded: {processor.stats['chapters_downloaded']}")

if __name__ == "__main__":
    asyncio.run(test_single_file())