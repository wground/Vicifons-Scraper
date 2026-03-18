#!/usr/bin/env python3
"""
Test Script for Combined Latin Processor Improvements

This script tests the major improvements made to address missing works:
1. Enhanced Scriptor namespace searching
2. Expanded list of major works and authors 
3. Chapter/book heading removal
4. Parallel processing capability

Usage:
    python test_improvements.py
"""

import asyncio
import sys
from pathlib import Path

# Add modules to path
sys.path.append(str(Path(__file__).parent))

from modules.scraper import VicifonsScraper
from modules.enhanced_cleaner import EnhancedTextCleaner
from modules.updated_test_works import get_enhanced_test_works

async def test_scraper_improvements():
    """Test the enhanced scraper functionality."""
    print("🔍 Testing Enhanced Scraper Improvements")
    print("=" * 50)
    
    config = {
        'output_dir': 'test_improvements_output',
        'max_concurrent': 5,
        'use_cache': False
    }
    
    scraper = VicifonsScraper(config)
    
    # Test 1: Check if known works are properly detected
    test_works = [
        {'title': 'Commentarii de bello Gallico', 'author': 'Caesar'},
        {'title': 'Naturalis Historia', 'author': 'Plinius Maior'},
        {'title': 'Noctes Atticae', 'author': 'Gellius'},
        {'title': 'Ab Urbe Condita', 'author': 'Livius'}
    ]
    
    print(f"Testing {len(test_works)} major works detection:")
    
    for work in test_works:
        title_lower = work['title'].lower()
        
        # Check if it's in known works
        if title_lower in scraper.known_work_patterns:
            chapters = scraper.known_work_patterns[title_lower]['chapters']
            print(f"✅ {work['title']}: {len(chapters)} chapters defined")
        else:
            print(f"❌ {work['title']}: Not found in known works")
    
    # Test 2: Test a small scraping operation
    print(f"\n🚀 Testing small scrape operation...")
    try:
        results = await scraper.scrape_works(test_works[:2])  # Just test 2 works
        
        print(f"✅ Scraper test completed:")
        print(f"   Success: {results.get('success_count', 0)}")
        print(f"   Failures: {results.get('failure_count', 0)}")
        print(f"   Total files: {results.get('total_files', 0)}")
        
        return True
    except Exception as e:
        print(f"❌ Scraper test failed: {e}")
        return False

def test_cleaner_improvements():
    """Test the enhanced cleaner functionality."""
    print("\n🧹 Testing Enhanced Cleaner Improvements")
    print("=" * 50)
    
    config = {
        'output_dir': 'test_improvements_output',
        'enable_nlp': False
    }
    
    cleaner = EnhancedTextCleaner(config)
    
    # Test heading removal
    test_text = """Title: Test Work
Source: https://example.com
Category: Test
Text Type: prose
--------------------------------------------------

I

Liber Primus

Gallia est omnis divisa in partes tres, quarum unam incolunt Belgae, 
aliam Aquitani, tertiam qui ipsorum lingua Celtae, nostra Galli appellantur.

II

Liber Secundus  

Hi omnes lingua, institutis, legibus inter se differunt. Gallos ab Aquitanis 
Garumna flumen, a Belgis Matrona et Sequana dividit.

FINIS

"""
    
    print("Testing heading removal on sample text...")
    
    # Test the remove_chapter_headings method directly
    cleaned_text = cleaner.remove_chapter_headings(test_text)
    
    # Check if headings were removed
    headings_removed = [
        'I' not in cleaned_text.split('\n'),
        'Liber Primus' not in cleaned_text,
        'II' not in cleaned_text.split('\n'),
        'Liber Secundus' not in cleaned_text,
        'FINIS' not in cleaned_text
    ]
    
    if all(headings_removed):
        print("✅ Heading removal working correctly")
    else:
        print("❌ Some headings were not removed")
        print("Cleaned text preview:")
        print(cleaned_text[:300] + "...")
    
    return all(headings_removed)

def test_parallel_processing():
    """Test that parallel processing mode is available."""
    print("\n⚡ Testing Parallel Processing Capability")
    print("=" * 50)
    
    # Test that the CombinedLatinProcessor has the parallel method
    try:
        from combined_latin_processor import CombinedLatinProcessor
        
        config = {'output_dir': 'test', 'max_concurrent': 5}
        processor = CombinedLatinProcessor(config)
        
        # Check if the parallel method exists
        if hasattr(processor, 'process_test_works_parallel'):
            print("✅ Parallel processing method available")
            return True
        else:
            print("❌ Parallel processing method not found")
            return False
            
    except Exception as e:
        print(f"❌ Error testing parallel processing: {e}")
        return False

def test_major_works_coverage():
    """Test that we have good coverage of major Latin works."""
    print("\n📚 Testing Major Works Coverage")
    print("=" * 50)
    
    config = {'output_dir': 'test', 'max_concurrent': 5}
    scraper = VicifonsScraper(config)
    
    # Check coverage of major classical works
    important_works = [
        'commentarii de bello gallico',    # Caesar
        'commentarii de bello civili',     # Caesar  
        'naturalis historia',              # Pliny
        'ab urbe condita',                 # Livy
        'noctes atticae',                  # Gellius
        'aeneis',                          # Virgil
        'metamorphoses (ovidius)',         # Ovid
        'de rerum natura',                 # Lucretius
        'institutio oratoria',             # Quintilian
        'confessiones',                    # Augustine
        'de civitate dei',                 # Augustine
    ]
    
    covered = 0
    for work in important_works:
        if work in scraper.known_work_patterns:
            covered += 1
            chapters = len(scraper.known_work_patterns[work]['chapters'])
            print(f"✅ {work.title()}: {chapters} chapters")
        else:
            print(f"❌ {work.title()}: Not covered")
    
    coverage_percent = (covered / len(important_works)) * 100
    print(f"\nCoverage: {covered}/{len(important_works)} works ({coverage_percent:.1f}%)")
    
    return coverage_percent >= 80  # 80% coverage target

async def main():
    """Run all improvement tests."""
    print("🔬 Combined Latin Processor - Improvements Test Suite")
    print("=" * 60)
    
    # Run all tests
    test_results = {}
    
    # Test 1: Enhanced scraper
    test_results['scraper'] = await test_scraper_improvements()
    
    # Test 2: Enhanced cleaner  
    test_results['cleaner'] = test_cleaner_improvements()
    
    # Test 3: Parallel processing
    test_results['parallel'] = test_parallel_processing()
    
    # Test 4: Major works coverage
    test_results['coverage'] = test_major_works_coverage()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    for test_name, passed in test_results.items():
        status = "PASSED" if passed else "FAILED"
        emoji = "✅" if passed else "❌"
        print(f"{emoji} {test_name.replace('_', ' ').title()}: {status}")
    
    total_passed = sum(test_results.values())
    total_tests = len(test_results)
    
    print(f"\nOverall: {total_passed}/{total_tests} tests passed")
    
    if total_passed == total_tests:
        print("🎉 All improvements working correctly!")
        return 0
    else:
        print("⚠️  Some improvements need attention")
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))