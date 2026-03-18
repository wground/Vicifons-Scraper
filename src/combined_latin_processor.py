#!/usr/bin/env python3
"""
Combined Latin Processor
A unified, modular system that scrapes and cleans Latin texts from Vicifons.

This system combines the functionality of the Vicifons scraper and text cleaner
into a unified, maintainable architecture with proper separation of concerns.

Features:
- Modular design with separate scripts for each function
- Improved index page detection for works like Caesar's commentaries
- Smart text cleaning with LatinCy integration
- Orthography standardization for LLM training data
- Test-driven development with problematic works subset

Author: Willow Groundwater-Schuldt & Claude
"""

import sys
import os
import asyncio
import logging
from pathlib import Path
from typing import List, Dict, Optional
import argparse
import time

# Add modules directory to path
sys.path.append(str(Path(__file__).parent / "modules"))

# Import our modular components
from modules.scraper import VicifonsScraper
from modules.enhanced_cleaner import EnhancedTextCleaner
from modules.updated_test_works import get_enhanced_test_works
from modules.utils import setup_logging, create_directories

class CombinedLatinProcessor:
    """Main orchestrator for the combined scraper and cleaner."""
    
    def __init__(self, config: Dict):
        """Initialize the processor with configuration."""
        self.config = config
        self.logger = setup_logging(config.get('log_level', 'INFO'))
        self.output_dir = Path(config['output_dir'])
        
        # Initialize components
        self.scraper = VicifonsScraper(config)
        self.cleaner = EnhancedTextCleaner(config)
        
        # Create output directories
        create_directories(self.output_dir)
        
    async def process_test_works(self) -> Dict:
        """Process the test set of problematic/critical works."""
        self.logger.info("Processing test works (50-100 critical/problematic texts)")
        
        test_works = get_enhanced_test_works()
        results = {
            'scraped': 0,
            'cleaned': 0,
            'failed': 0,
            'details': []
        }
        
        # Scrape test works
        scrape_results = await self.scraper.scrape_works(test_works)
        results['scraped'] = scrape_results['success_count']
        
        # Clean scraped texts with enhanced categorization
        if scrape_results['success_count'] > 0:
            raw_dir = self.output_dir / "raw_scraped"
            clean_results = await self.cleaner.clean_directory_enhanced(raw_dir)
            results['cleaned'] = clean_results['success_count']
            results['categories'] = clean_results.get('categories', {})
        
        results['failed'] = len(test_works) - results['scraped']
        results['details'] = scrape_results['details']
        
        return results
    
    async def process_test_works_parallel(self) -> Dict:
        """Process test works with parallel scraping and cleaning."""
        self.logger.info("Processing test works with parallel scraping and cleaning")
        
        test_works = get_enhanced_test_works()
        results = {
            'scraped': 0,
            'cleaned': 0,
            'failed': 0,
            'details': []
        }
        
        # Start scraping task
        scrape_task = asyncio.create_task(self.scraper.scrape_works(test_works))
        
        # Monitor scraping and start cleaning files as they become available
        raw_dir = self.output_dir / "raw_scraped"
        processed_files = set()
        
        # Polling loop to check for new files and clean them
        while not scrape_task.done():
            # Check for new files to clean
            if raw_dir.exists():
                current_files = set(raw_dir.glob('*.txt'))
                new_files = current_files - processed_files
                
                if new_files:
                    # Clean new files concurrently
                    clean_tasks = []
                    for file_path in new_files:
                        if file_path.stat().st_size > 100:  # Only process files with content
                            task = self.cleaner.clean_single_file_enhanced(file_path)
                            clean_tasks.append(task)
                            processed_files.add(file_path)
                    
                    if clean_tasks:
                        clean_results = await asyncio.gather(*clean_tasks, return_exceptions=True)
                        for result in clean_results:
                            if isinstance(result, dict) and result.get('success'):
                                results['cleaned'] += 1
            
            # Wait before checking again
            await asyncio.sleep(2.0)
        
        # Get scraping results
        scrape_results = await scrape_task
        results['scraped'] = scrape_results['success_count']
        results['failed'] = scrape_results['failure_count']
        results['details'] = scrape_results['details']
        
        # Clean any remaining files
        if raw_dir.exists():
            remaining_files = set(raw_dir.glob('*.txt')) - processed_files
            if remaining_files:
                clean_tasks = [self.cleaner.clean_single_file_enhanced(f) for f in remaining_files]
                clean_results = await asyncio.gather(*clean_tasks, return_exceptions=True)
                for result in clean_results:
                    if isinstance(result, dict) and result.get('success'):
                        results['cleaned'] += 1
        
        return results
    
    async def process_full_corpus(self) -> Dict:
        """Process the full corpus from Vicifons."""
        self.logger.info("Processing full corpus from Vicifons (thousands of texts)")
        
        # Ensure comprehensive works list exists
        await self._ensure_comprehensive_works_list()
        
        # Use the comprehensive corpus scraping method (XML dump based)
        scrape_results = await self.scraper.scrape_comprehensive_corpus()
        
        # Clean all scraped texts
        total_cleaned = 0
        if scrape_results['success_count'] > 0:
            raw_dir = self.output_dir / "raw_scraped"
            clean_results = await self.cleaner.clean_directory_enhanced(raw_dir)
            total_cleaned = clean_results['success_count']
        
        return {
            'scraped': scrape_results['success_count'],
            'failed': scrape_results['failure_count'],
            'cleaned': total_cleaned,
            'total_files': scrape_results['total_files'],
            'categories_processed': scrape_results['categories_processed'],
            'details': scrape_results['details'][:10]  # First 10 details for summary
        }
    
    async def _ensure_comprehensive_works_list(self):
        """Ensure the comprehensive works list exists, create if needed."""
        # Prefer the filtered works list
        filtered_file = self.output_dir.parent / "filtered_latin_works.json"
        json_file = self.output_dir.parent / "all_latin_works.json"
        
        if filtered_file.exists():
            self.logger.info(f"Using existing filtered works list: {filtered_file}")
            return
        
        if not json_file.exists():
            self.logger.info("Comprehensive works list not found, generating from XML dump...")
            
            # Import and run the filtered extractor (preferred)
            import sys
            sys.path.append(str(Path(__file__).parent / "modules"))
            
            try:
                from modules.filtered_extractor import FilteredLatinExtractor
                import json
                
                xml_file = self.output_dir.parent / "LawikiSource Dump Jul 20 2025.xml"
                if xml_file.exists():
                    self.logger.info(f"Extracting filtered works from XML dump: {xml_file}")
                    extractor = FilteredLatinExtractor()
                    works = extractor.extract_filtered_latin_works(str(xml_file))
                    extractor.save_categorized_works(works, str(filtered_file))
                    self.logger.info(f"Generated filtered works list: {len(works)} works")
                else:
                    self.logger.warning(f"XML dump not found at: {xml_file}")
                    self.logger.info("Will fall back to category-based scraping")
                    
            except Exception as e:
                self.logger.error(f"Error generating filtered works list: {e}")
                self.logger.info("Will fall back to category-based scraping")
        else:
            self.logger.info(f"Using existing comprehensive works list: {json_file}")
    
    async def run(self, mode: str = "test") -> Dict:
        """Run the processor in the specified mode."""
        start_time = time.time()
        
        self.logger.info(f"Starting Combined Latin Processor in {mode} mode")
        
        if mode == "test":
            results = await self.process_test_works()
        elif mode == "test-parallel":
            results = await self.process_test_works_parallel()
        elif mode == "full":
            results = await self.process_full_corpus()
        else:
            raise ValueError(f"Unknown mode: {mode}")
        
        elapsed = time.time() - start_time
        results['elapsed_time'] = elapsed
        
        self.logger.info(f"Processing complete in {elapsed:.1f}s")
        self.logger.info(f"Results: {results}")
        
        return results

def get_user_preferences():
    """Get user preferences through interactive prompts."""
    print("=" * 60)
    print("COMBINED LATIN PROCESSOR - CONFIGURATION")
    print("=" * 60)
    
    # Processing mode
    print("\nProcessing modes:")
    print("1. test - Process critical/problematic works (50-100 texts)")
    print("2. test-parallel - Same as test but with concurrent cleaning")
    print("3. full - Process the entire Vicifons corpus (5k+ texts)")
    
    while True:
        try:
            mode_choice = input("\nSelect processing mode [1-3] (default: 1): ").strip()
            if not mode_choice:
                mode_choice = "1"
            
            if mode_choice == "1":
                mode = "test"
                break
            elif mode_choice == "2":
                mode = "test-parallel"
                break
            elif mode_choice == "3":
                mode = "full"
                break
            else:
                print("Invalid choice. Please select 1, 2, or 3.")
        except KeyboardInterrupt:
            print("\nExiting...")
            sys.exit(0)
    
    # Concurrency settings
    print("\nConcurrency settings:")
    print("Higher values = faster processing but more resource usage")
    print("- Conservative (10): Slow but stable")
    print("- Balanced (25): Good speed/stability balance") 
    print("- Aggressive (50): Fast but may overwhelm server")
    print("- Custom: Enter your own value")
    
    while True:
        try:
            conc_choice = input("\nSelect concurrency [conservative/balanced/aggressive/custom] (default: balanced): ").strip().lower()
            if not conc_choice:
                conc_choice = "balanced"
            
            if conc_choice in ["conservative", "c"]:
                max_concurrent = 10
                speed_mode = "normal"
                break
            elif conc_choice in ["balanced", "b"]:
                max_concurrent = 25
                speed_mode = "fast"
                break
            elif conc_choice in ["aggressive", "a"]:
                max_concurrent = 50
                speed_mode = "maximum"
                break
            elif conc_choice in ["custom", "cu"]:
                custom_val = input("Enter custom concurrency (5-100): ").strip()
                try:
                    max_concurrent = int(custom_val)
                    if 5 <= max_concurrent <= 100:
                        speed_mode = "custom"
                        break
                    else:
                        print("Value must be between 5 and 100.")
                except ValueError:
                    print("Please enter a valid number.")
            else:
                print("Invalid choice. Please select conservative, balanced, aggressive, or custom.")
        except KeyboardInterrupt:
            print("\nExiting...")
            sys.exit(0)
    
    # Additional options
    print("\nAdditional options:")
    
    use_cache = True
    cache_choice = input("Use caching for faster repeated runs? [Y/n] (default: Y): ").strip().lower() ##Caching might be inefficient, needs to be revised; could be cause of slow-down midway through scraping?
    if cache_choice in ['n', 'no']:
        use_cache = False
    
    enable_nlp = False
    nlp_choice = input("Enable advanced Latin NLP processing (slower)? [y/N] (default: N): ").strip().lower()
    if nlp_choice in ['y', 'yes']:
        enable_nlp = True
    
    output_dir = input("Output directory (default: processed_latin_texts): ").strip()
    if not output_dir:
        output_dir = "processed_latin_texts"
    
    log_level = "INFO"
    debug_choice = input("Enable debug logging? [y/N] (default: N): ").strip().lower()
    if debug_choice in ['y', 'yes']:
        log_level = "DEBUG"
    
    print("\n" + "=" * 60)
    print("CONFIGURATION SUMMARY")
    print("=" * 60)
    print(f"Mode: {mode}")
    print(f"Max concurrent downloads: {max_concurrent}")
    print(f"Use caching: {use_cache}")
    print(f"Enable NLP: {enable_nlp}")
    print(f"Output directory: {output_dir}")
    print(f"Log level: {log_level}")
    print("=" * 60)
    
    confirm = input("\nProceed with these settings? [Y/n]: ").strip().lower()
    if confirm in ['n', 'no']:
        print("Configuration cancelled. Exiting...")
        sys.exit(0)
    
    return {
        'mode': mode,
        'max_concurrent': max_concurrent,
        'speed_mode': speed_mode,
        'use_cache': use_cache,
        'enable_nlp': enable_nlp,
        'output_dir': output_dir,
        'log_level': log_level
    }

def main():
    """Main entry point."""
    # Check if running interactively or with command line args
    import sys
    if len(sys.argv) == 1:
        # Interactive mode - get user preferences
        user_prefs = get_user_preferences()
        mode = user_prefs['mode']
        max_concurrent = user_prefs['max_concurrent']
        speed_mode = user_prefs['speed_mode']
        use_cache = user_prefs['use_cache']
        enable_nlp = user_prefs['enable_nlp']
        output_dir = user_prefs['output_dir']
        log_level = user_prefs['log_level']
    else:
        # Command line mode - use argparse
        parser = argparse.ArgumentParser(
            description="Combined Latin text scraper and cleaner for Vicifons",
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        
        parser.add_argument(
            '--mode', 
            choices=['test', 'test-parallel', 'full'], 
            default='test',
            help='Processing mode: test (50-100 works), test-parallel (with concurrent cleaning), or full corpus'
        )
        
        parser.add_argument(
            '--output-dir',
            default='processed_latin_texts',
            help='Output directory for processed texts'
        )
        
        parser.add_argument(
            '--max-concurrent',
            type=int,
            default=25,  # Increased from 10 for better speed
            help='Maximum concurrent downloads'
        )
        
        parser.add_argument(
            '--speed-mode',
            choices=['normal', 'fast', 'maximum'],
            default='fast',
            help='Speed mode: normal (10), fast (25), maximum (50) concurrent downloads'
        )
        
        parser.add_argument(
            '--use-cache',
            action='store_true',
            default=True,
            help='Use caching for scraping'
        )
        
        parser.add_argument(
            '--enable-nlp',
            action='store_true',
            help='Enable LatinCy NLP processing'
        )
        
        parser.add_argument(
            '--log-level',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
            default='INFO',
            help='Logging level'
        )
        
        args = parser.parse_args()
        
        mode = args.mode
        max_concurrent = args.max_concurrent
        speed_mode = args.speed_mode
        use_cache = args.use_cache
        enable_nlp = args.enable_nlp
        output_dir = args.output_dir
        log_level = args.log_level
    
    # Adjust concurrency based on speed mode (only for command line mode)
    if len(sys.argv) > 1:
        speed_settings = {
            'normal': 10,
            'fast': 25,
            'maximum': 50
        }
        
        if args.max_concurrent == 25:  # Default value, use speed mode
            max_concurrent = speed_settings[args.speed_mode]
        else:  # User specified value, use that
            max_concurrent = args.max_concurrent
    
    # Create configuration
    config = {
        'output_dir': output_dir,
        'max_concurrent': max_concurrent,
        'use_cache': use_cache,
        'enable_nlp': enable_nlp,
        'log_level': log_level,
        'speed_mode': speed_mode,
    }
    
    # Run processor
    processor = CombinedLatinProcessor(config)
    
    try:
        results = asyncio.run(processor.run(mode))
        
        print("\n" + "="*60)
        print("COMBINED LATIN PROCESSOR - RESULTS")
        print("="*60)
        print(f"Mode: {mode}")
        print(f"Speed mode: {speed_mode} ({max_concurrent} concurrent)")
        print(f"Scraped texts: {results.get('scraped', 0)}")
        print(f"Cleaned texts: {results.get('cleaned', 0)}")
        print(f"Processing time: {results.get('elapsed_time', 0):.1f}s")
        
        if mode in ['test', 'test-parallel']:
            print(f"Failed texts: {results.get('failed', 0)}")
            if mode == 'test-parallel':
                print("✅ Used parallel processing (download while cleaning)")
        elif mode == 'full':
            print(f"Failed texts: {results.get('failed', 0)}")
            print(f"Total files created: {results.get('total_files', 0)}")
            print(f"Categories processed: {results.get('categories_processed', 0)}")
            
            # Performance stats
            if results.get('elapsed_time', 0) > 0:
                texts_per_second = results.get('scraped', 0) / results.get('elapsed_time', 1)
                print(f"Performance: {texts_per_second:.2f} texts/second")
        
        print("="*60)
        
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())