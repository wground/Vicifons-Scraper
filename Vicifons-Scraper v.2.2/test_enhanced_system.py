#!/usr/bin/env python3
"""
Test Enhanced Combined Latin Processor System

Tests the improved system with:
- Better export metadata removal
- Classical/Post-Classical categorization  
- Prose/Poetry categorization
- Comprehensive abbreviation expansion
- LLM-ready output validation
"""

import asyncio
import sys
import logging
from pathlib import Path

# Add modules to path
sys.path.append(str(Path(__file__).parent))

from modules.updated_test_works import get_enhanced_test_works, get_categorization_test_works
from modules.scraper import VicifonsScraper
from modules.enhanced_cleaner import EnhancedTextCleaner
from modules.orthography import OrthographyStandardizer
from modules.utils import setup_logging

async def test_enhanced_scraper():
    """Test the scraper with categorization test works."""
    print("\n" + "="*60)
    print("TESTING ENHANCED SCRAPER")
    print("="*60)
    
    # Get categorization test works (should include both periods/genres)
    test_works = get_categorization_test_works()
    print(f"Testing {len(test_works)} categorization test works:")
    
    period_genre_counts = {}
    for work in test_works:
        key = f"{work['period']}/{work['genre']}"
        period_genre_counts[key] = period_genre_counts.get(key, 0) + 1
        print(f"  - {work['title']} ({work['author']}) - {key}")
    
    print(f"\nDistribution: {period_genre_counts}")
    
    # Configure scraper
    config = {
        'output_dir': 'test_enhanced_output',
        'max_concurrent': 2,
        'use_cache': False
    }
    
    # Test scraper
    scraper = VicifonsScraper(config)
    
    try:
        results = await scraper.scrape_works(test_works)
        
        print(f"\nScraper Results:")
        print(f"  Success: {results['success_count']}")
        print(f"  Failures: {results['failure_count']}")
        print(f"  Total files: {results['total_files']}")
        
        return results['success_count'] > 0
        
    except Exception as e:
        print(f"Enhanced scraper test failed: {e}")
        return False

async def test_enhanced_cleaner():
    """Test the enhanced text cleaner with categorization."""
    print("\n" + "="*60)
    print("TESTING ENHANCED CLEANER")
    print("="*60)
    
    # Check if we have scraped files
    raw_dir = Path('test_enhanced_output/raw_scraped')
    
    if not raw_dir.exists():
        print("No scraped files found to test enhanced cleaner")
        return False
    
    # Configure enhanced cleaner
    config = {
        'output_dir': 'test_enhanced_output',
        'enable_nlp': False
    }
    
    # Test enhanced cleaner
    cleaner = EnhancedTextCleaner(config)
    
    try:
        results = await cleaner.clean_directory_enhanced(raw_dir)
        
        print(f"\nEnhanced Cleaner Results:")
        print(f"  Successfully cleaned: {results['success_count']}")
        print(f"  Failed: {results['failure_count']}")
        print(f"  Total processed: {results['total_files']}")
        
        print(f"\nCategorization Results:")
        for category, count in results['categories'].items():
            if count > 0:
                print(f"  {category}: {count} files")
        
        # Verify categorized output structure
        output_dir = Path('test_enhanced_output/cleaned_texts')
        categories_found = []
        
        for category_dir in ['classical/prose', 'classical/poetry', 'post_classical/prose', 'post_classical/poetry']:
            full_path = output_dir / category_dir
            if full_path.exists():
                files = list(full_path.glob('*.txt'))
                if files:
                    categories_found.append(f"{category_dir}: {len(files)} files")
        
        if categories_found:
            print(f"\nVerified Output Structure:")
            for cat in categories_found:
                print(f"  {cat}")
        
        return results['success_count'] > 0
        
    except Exception as e:
        print(f"Enhanced cleaner test failed: {e}")
        return False

async def test_content_quality():
    """Test that cleaned content is LLM-ready (Latin only, no metadata)."""
    print("\n" + "="*60)
    print("TESTING CONTENT QUALITY")
    print("="*60)
    
    output_dir = Path('test_enhanced_output/cleaned_texts')
    
    if not output_dir.exists():
        print("No cleaned files found for quality testing")
        return False
    
    # Find some cleaned files to test
    test_files = []
    for category in ['classical/prose', 'classical/poetry', 'post_classical/prose']:
        cat_dir = output_dir / category
        if cat_dir.exists():
            files = list(cat_dir.glob('*.txt'))[:2]  # Test 2 files per category
            test_files.extend(files)
    
    if not test_files:
        print("No cleaned files found in categorized directories")
        return False
    
    print(f"Testing content quality of {len(test_files)} files:")
    
    quality_results = {
        'latin_only': 0,
        'no_metadata': 0,
        'proper_orthography': 0,
        'failed_quality': 0
    }
    
    for file_path in test_files[:3]:  # Test first 3 files
        print(f"\n  Testing: {file_path.name}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Test 1: No export metadata
            metadata_indicators = [
                'exported from wikisource',
                'about this digital edition',
                'accurimbono',
                'we distribute our books',
                'creative commons',
                'the following users'
            ]
            
            has_metadata = any(indicator.lower() in content.lower() 
                             for indicator in metadata_indicators)
            
            if not has_metadata:
                quality_results['no_metadata'] += 1
                print(f"    ✓ No export metadata found")
            else:
                print(f"    ✗ Export metadata still present")
            
            # Test 2: Check for Latin content
            latin_words = ['et', 'in', 'ad', 'cum', 'de', 'per', 'pro', 'est', 'sunt', 'qui', 'quae', 'sed']
            latin_word_count = sum(1 for word in latin_words if word in content.lower())
            
            if latin_word_count >= 5:
                quality_results['latin_only'] += 1
                print(f"    ✓ Latin content confirmed ({latin_word_count} common Latin words)")
            else:
                print(f"    ✗ Insufficient Latin indicators")
            
            # Test 3: Check orthography standardization
            has_j_letters = 'j' in content or 'J' in content
            has_v_vowels = bool(re.search(r'[aeiou]v[aeiou]', content, re.IGNORECASE))
            
            if not has_j_letters and not has_v_vowels:
                quality_results['proper_orthography'] += 1
                print(f"    ✓ Orthography standardized (no j/v issues)")
            else:
                print(f"    ✗ Orthography not fully standardized")
            
            # Show sample of content (first 200 chars)
            sample = content[:200].replace('\n', ' ')
            print(f"    Sample: {sample}...")
            
        except Exception as e:
            quality_results['failed_quality'] += 1
            print(f"    ✗ Quality test failed: {e}")
    
    print(f"\nQuality Test Summary:")
    total_tested = len(test_files[:3])
    for test_name, passed_count in quality_results.items():
        if test_name != 'failed_quality':
            print(f"  {test_name}: {passed_count}/{total_tested}")
    
    # Pass if majority of tests passed
    avg_score = sum(v for k, v in quality_results.items() if k != 'failed_quality') / 3
    return avg_score >= total_tested * 0.7  # 70% pass rate

def test_abbreviation_expansion():
    """Test abbreviation expansion functionality."""
    print("\n" + "="*60)
    print("TESTING ABBREVIATION EXPANSION")
    print("="*60)
    
    # Test orthography standardizer directly
    ortho = OrthographyStandardizer()
    
    test_cases = [
        ("M. Tullius Cicero consul populum Romanum defendit", "praenomen expansion"),
        ("C. Iulius Caesar imperator Galliam vicit", "praenomen expansion"),
        ("D. M. S. hic iacet Marcus", "religious abbreviation"),
        ("i. e. id est veritas", "common abbreviation"),
        ("q. de re publica locutus est", "conjunction abbreviation")
    ]
    
    print("Testing abbreviation expansion:")
    
    expansions_found = 0
    for test_text, description in test_cases:
        print(f"\n  Test: {description}")
        print(f"    Original: {test_text}")
        
        # Test the expand_abbreviations method specifically
        from modules.enhanced_cleaner import EnhancedTextCleaner
        config = {'output_dir': 'test_output'}
        cleaner = EnhancedTextCleaner(config)
        expanded = cleaner.expand_abbreviations(test_text)
        
        print(f"    Expanded: {expanded}")
        
        if expanded != test_text:
            expansions_found += 1
            print(f"    ✓ Expansion applied")
        else:
            print(f"    - No expansion needed/applied")
    
    print(f"\nAbbreviation expansion working: {expansions_found > 0}")
    return expansions_found > 0

async def main():
    """Run enhanced system tests."""
    print("Enhanced Combined Latin Processor - System Test")
    print("="*60)
    
    # Setup logging
    logger = setup_logging('DEBUG')
    
    # Run tests
    test_results = {}
    
    # Test 1: Enhanced Scraper
    test_results['scraper'] = await test_enhanced_scraper()
    
    # Test 2: Enhanced Cleaner with Categorization
    if test_results['scraper']:
        test_results['cleaner'] = await test_enhanced_cleaner()
    else:
        test_results['cleaner'] = False
        print("\nSkipping enhanced cleaner test (no scraped files)")
    
    # Test 3: Content Quality for LLM Training
    if test_results['cleaner']:
        test_results['quality'] = await test_content_quality()
    else:
        test_results['quality'] = False
        print("\nSkipping quality test (no cleaned files)")
    
    # Test 4: Abbreviation Expansion
    test_results['abbreviations'] = test_abbreviation_expansion()
    
    # Summary
    print("\n" + "="*60)
    print("ENHANCED SYSTEM TEST RESULTS")
    print("="*60)
    
    test_descriptions = {
        'scraper': 'Enhanced Scraper',
        'cleaner': 'Enhanced Cleaner with Categorization', 
        'quality': 'LLM Training Data Quality',
        'abbreviations': 'Abbreviation Expansion'
    }
    
    for test_name, passed in test_results.items():
        status = "PASSED" if passed else "FAILED"
        desc = test_descriptions.get(test_name, test_name)
        print(f"{desc}: {status}")
    
    total_passed = sum(test_results.values())
    total_tests = len(test_results)
    
    print(f"\nOverall: {total_passed}/{total_tests} tests passed")
    
    if total_passed == total_tests:
        print("✅ All enhanced tests passed! System ready for LLM training data generation.")
        return 0
    elif total_passed >= total_tests * 0.75:
        print("⚠️  Most tests passed. System functional with minor issues.")
        return 0 
    else:
        print("❌ Multiple tests failed. System needs fixes.")
        return 1

if __name__ == "__main__":
    import re
    sys.exit(asyncio.run(main()))