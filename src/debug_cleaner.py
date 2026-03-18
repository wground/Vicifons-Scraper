#!/usr/bin/env python3
"""
Debug the enhanced cleaner to see why it's failing
"""

import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from modules.enhanced_cleaner import EnhancedTextCleaner
from modules.utils import setup_logging

async def debug_single_file():
    """Debug cleaning of a single file."""
    
    # Setup logging
    logger = setup_logging('DEBUG')
    
    # Test file path
    test_file = Path('test_enhanced_output/raw_scraped/Aeneis_Liber I.txt')
    
    if not test_file.exists():
        print(f"Test file not found: {test_file}")
        return
    
    # Read file content
    with open(test_file, 'r') as f:
        content = f.read()
    
    print("="*60)
    print("DEBUG ENHANCED CLEANER")
    print("="*60)
    print(f"File: {test_file.name}")
    print(f"Original length: {len(content)} chars")
    print(f"First 200 chars: {content[:200]}")
    
    # Initialize cleaner
    config = {'output_dir': 'debug_output'}
    cleaner = EnhancedTextCleaner(config)
    
    # Test each step
    print("\n--- Step 1: Metadata Removal ---")
    step1 = cleaner.aggressive_metadata_removal(content)
    print(f"After metadata removal: {len(step1)} chars")
    print(f"Sample: {step1[:200]}")
    
    print("\n--- Step 2: Abbreviation Expansion ---")
    step2 = cleaner.expand_abbreviations(step1)
    print(f"After abbreviations: {len(step2)} chars")
    print(f"Sample: {step2[:200]}")
    
    print("\n--- Step 3: Content Patterns ---")
    step3 = step2
    for pattern, replacement in cleaner.content_cleaning_patterns:
        step3 = pattern.sub(replacement, step3)
    print(f"After content cleaning: {len(step3)} chars")
    print(f"Sample: {step3[:200]}")
    
    print("\n--- Step 4: Orthography ---")
    step4 = cleaner.orthography.standardize(step3)
    print(f"After orthography: {len(step4)} chars")
    print(f"Sample: {step4[:200]}")
    
    print("\n--- Step 5: Final Validation ---")
    step5, is_valid = cleaner.final_latin_validation(step4)
    print(f"After validation: {len(step5)} chars")
    print(f"Is valid: {is_valid}")
    print(f"Sample: {step5[:200]}")
    
    # Test full process
    print("\n--- Full Process Test ---")
    try:
        result = await cleaner.clean_single_file_enhanced(test_file)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_single_file())