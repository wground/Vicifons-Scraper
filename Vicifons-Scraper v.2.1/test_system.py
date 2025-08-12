#!/usr/bin/env python3
"""
Test the Combined Latin Processor system

This script tests the modular system with a small subset of test works
to verify that the scraper and cleaner work correctly together.
"""

import asyncio
import sys
import logging
from pathlib import Path

# Add modules to path
sys.path.append(str(Path(__file__).parent))

from modules.test_works import get_works_by_priority
from modules.scraper import VicifonsScraper
from modules.cleaner import TextCleaner
from modules.orthography import OrthographyStandardizer
from modules.utils import setup_logging

async def test_scraper():
    """Test the scraper with high-priority works."""
    print("\n" + "="*60)
    print("TESTING SCRAPER MODULE")
    print("="*60)
    
    # Get high-priority test works (should include Caesar)
    test_works = get_works_by_priority('high')[:3]  # Test first 3
    print(f"Testing {len(test_works)} high-priority works:")
    for work in test_works:
        print(f"  - {work['title']} ({work['author']})")
    
    # Configure scraper
    config = {
        'output_dir': 'test_output',
        'max_concurrent': 2,
        'use_cache': False  # Don't use cache for testing
    }
    
    # Test scraper
    scraper = VicifonsScraper(config)
    
    try:
        results = await scraper.scrape_works(test_works)
        
        print(f"\nScraper Results:")
        print(f"  Success: {results['success_count']}")
        print(f"  Failures: {results['failure_count']}")
        print(f"  Total files: {results['total_files']}")
        
        # Show details for failed works
        for detail in results['details']:
            if not detail.get('success', False):
                print(f"  Failed: {detail.get('title', 'Unknown')} - {detail.get('error', 'Unknown error')}")
        
        return results['success_count'] > 0
        
    except Exception as e:
        print(f"Scraper test failed: {e}")
        return False

async def test_cleaner():
    """Test the text cleaner."""
    print("\n" + "="*60)
    print("TESTING CLEANER MODULE")
    print("="*60)
    
    # Check if we have scraped files to clean
    raw_dir = Path('test_output/raw_scraped')
    
    if not raw_dir.exists():
        print("No scraped files found to test cleaner")
        return False
    
    # Configure cleaner
    config = {
        'output_dir': 'test_output',
        'enable_nlp': False  # Skip NLP for basic test
    }
    
    # Test cleaner
    cleaner = TextCleaner(config)
    
    try:
        results = await cleaner.clean_directory(raw_dir)
        
        print(f"\nCleaner Results:")
        print(f"  Cleaned: {results['success_count']}")
        print(f"  Skipped: {results['skipped_count']}")
        print(f"  Failed: {results['failure_count']}")
        
        # Show some examples
        successful_files = [d for d in results['details'] if d.get('success') and d.get('action') == 'cleaned']
        if successful_files:
            print(f"\nSuccessfully cleaned files:")
            for detail in successful_files[:3]:  # Show first 3
                print(f"  - {detail['filename']} -> {detail['output_filename']}")
                print(f"    Type: {detail.get('text_type', 'unknown')}")
                stats = detail.get('stats', {})
                print(f"    Words: {stats.get('word_count', 0)}, Lines: {stats.get('line_count', 0)}")
        
        return results['success_count'] > 0
        
    except Exception as e:
        print(f"Cleaner test failed: {e}")
        return False

def test_orthography():
    """Test the orthography standardizer."""
    print("\n" + "="*60)
    print("TESTING ORTHOGRAPHY MODULE")
    print("="*60)
    
    # Test orthography standardizer
    ortho = OrthographyStandardizer()
    
    # Test cases with various issues
    test_cases = [
        "Gallia est omnis divisa in partes tres, quarum unam incolunt Belgae",
        "Michi videtur quod nichil est melius quam studere",
        "M. Tullius Cicero consul populum Romanum defendit",
        "Caesar exercitum in Galliam duxit et hostes vicit",
    ]
    
    print("Testing orthography standardization:")
    
    for i, test_text in enumerate(test_cases, 1):
        print(f"\nTest {i}:")
        print(f"  Original: {test_text}")
        
        # Analyze original
        analysis = ortho.analyze_text(test_text)
        print(f"  Analysis: {analysis['word_count']} words, "
              f"diacritics: {analysis['has_diacritics']}, "
              f"j-letters: {analysis['has_j_letters']}")
        
        # Standardize
        standardized = ortho.standardize(test_text)
        print(f"  Standardized: {standardized}")
    
    return True

async def main():
    """Run all tests."""
    print("Combined Latin Processor - System Test")
    print("="*60)
    
    # Setup logging
    logger = setup_logging('DEBUG')
    
    # Run tests
    test_results = {}
    
    # Test 1: Scraper
    test_results['scraper'] = await test_scraper()
    
    # Test 2: Cleaner (depends on scraper)
    if test_results['scraper']:
        test_results['cleaner'] = await test_cleaner()
    else:
        test_results['cleaner'] = False
        print("\nSkipping cleaner test (no scraped files)")
    
    # Test 3: Orthography (independent)
    test_results['orthography'] = test_orthography()
    
    # Summary
    print("\n" + "="*60)
    print("TEST RESULTS SUMMARY")
    print("="*60)
    
    for test_name, passed in test_results.items():
        status = "PASSED" if passed else "FAILED"
        print(f"{test_name.title()}: {status}")
    
    total_passed = sum(test_results.values())
    total_tests = len(test_results)
    
    print(f"\nOverall: {total_passed}/{total_tests} tests passed")
    
    if total_passed == total_tests:
        print("✅ All tests passed! System is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))