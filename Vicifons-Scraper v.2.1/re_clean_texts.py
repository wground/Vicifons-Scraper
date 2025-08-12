#!/usr/bin/env python3
"""
Re-clean existing scraped Latin texts with improved cleaning logic.

This script applies the enhanced cleaning process to already scraped texts
without requiring re-scraping from Vicifons.
"""

import asyncio
import sys
import logging
from pathlib import Path
from typing import Dict

# Add modules directory to path
sys.path.append(str(Path(__file__).parent / "modules"))

from modules.enhanced_cleaner import EnhancedTextCleaner
from modules.utils import setup_logging, ProgressTracker

class TextReCleaner:
    """Re-cleaner for existing scraped texts with improved cleaning."""
    
    def __init__(self, input_dir: Path, output_dir: Path = None):
        """Initialize the re-cleaner."""
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir) if output_dir else self.input_dir.parent / "re_cleaned_texts"
        
        # Setup logging
        self.logger = setup_logging('INFO')
        
        # Configure enhanced cleaner
        config = {
            'output_dir': str(self.output_dir),
            'enable_nlp': False,  # Disable NLP for faster processing
            'log_level': 'INFO'
        }
        
        self.cleaner = EnhancedTextCleaner(config)
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Initialized TextReCleaner")
        self.logger.info(f"Input directory: {self.input_dir}")
        self.logger.info(f"Output directory: {self.output_dir}")
    
    async def re_clean_all_texts(self) -> Dict:
        """Re-clean all text files in the input directory."""
        if not self.input_dir.exists():
            self.logger.error(f"Input directory does not exist: {self.input_dir}")
            return {'success': False, 'error': 'Input directory not found'}
        
        # Find all .txt files
        text_files = list(self.input_dir.rglob('*.txt'))
        
        if not text_files:
            self.logger.warning(f"No .txt files found in {self.input_dir}")
            return {'success': False, 'error': 'No text files found'}
        
        self.logger.info(f"Found {len(text_files)} text files to re-clean")
        
        # Process files
        results = await self.cleaner.clean_directory_enhanced(self.input_dir)
        
        self.logger.info("Re-cleaning complete!")
        self.logger.info(f"Successfully cleaned: {results['success_count']}")
        self.logger.info(f"Failed to clean: {results['failure_count']}")
        
        # Log categorization results
        if 'categories' in results:
            self.logger.info("Categorization breakdown:")
            for category, count in results['categories'].items():
                if count > 0:
                    self.logger.info(f"  {category}: {count} files")
        
        return {
            'success': True,
            'total_files': len(text_files),
            'cleaned_files': results['success_count'],
            'failed_files': results['failure_count'],
            'categories': results.get('categories', {}),
            'output_directory': str(self.output_dir)
        }
    
    async def re_clean_single_file(self, file_path: Path) -> Dict:
        """Re-clean a single text file."""
        if not file_path.exists():
            return {'success': False, 'error': 'File not found'}
        
        self.logger.info(f"Re-cleaning single file: {file_path.name}")
        
        result = await self.cleaner.clean_single_file_enhanced(file_path)
        
        if result.get('success'):
            self.logger.info(f"Successfully re-cleaned: {file_path.name}")
            self.logger.info(f"Output: {result.get('output_path')}")
            self.logger.info(f"Category: {result.get('period')}/{result.get('genre')}")
        else:
            self.logger.error(f"Failed to re-clean: {file_path.name}")
            self.logger.error(f"Error: {result.get('error')}")
        
        return result

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Re-clean existing scraped Latin texts with improved cleaning",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Re-clean all texts in the default scraped directory
  python re_clean_texts.py
  
  # Re-clean texts from a specific directory
  python re_clean_texts.py --input-dir /path/to/scraped/texts
  
  # Re-clean with custom output directory
  python re_clean_texts.py --output-dir /path/to/cleaned/texts
  
  # Re-clean a single file
  python re_clean_texts.py --single-file /path/to/file.txt
        """
    )
    
    parser.add_argument(
        '--input-dir',
        default='processed_latin_texts/raw_scraped',
        help='Directory containing scraped texts to re-clean (default: processed_latin_texts/raw_scraped)'
    )
    
    parser.add_argument(
        '--output-dir',
        help='Output directory for cleaned texts (default: processed_latin_texts/cleaned_texts)'
    )
    
    parser.add_argument(
        '--single-file',
        help='Re-clean a single file instead of entire directory'
    )
    
    args = parser.parse_args()
    
    # Resolve paths
    input_path = Path(args.input_dir).resolve()
    output_path = Path(args.output_dir).resolve() if args.output_dir else None
    
    # Initialize re-cleaner
    re_cleaner = TextReCleaner(input_path, output_path)
    
    try:
        if args.single_file:
            # Re-clean single file
            file_path = Path(args.single_file).resolve()
            result = asyncio.run(re_cleaner.re_clean_single_file(file_path))
            
            if result.get('success'):
                print(f"\n✅ Successfully re-cleaned: {file_path.name}")
                print(f"Output: {result.get('output_path')}")
                print(f"Category: {result.get('period')}/{result.get('genre')}")
            else:
                print(f"\n❌ Failed to re-clean: {file_path.name}")
                print(f"Error: {result.get('error')}")
                return 1
        else:
            # Re-clean all files
            result = asyncio.run(re_cleaner.re_clean_all_texts())
            
            if result.get('success'):
                print(f"\n✅ Re-cleaning completed successfully!")
                print(f"Total files processed: {result['total_files']}")
                print(f"Successfully cleaned: {result['cleaned_files']}")
                print(f"Failed to clean: {result['failed_files']}")
                print(f"Output directory: {result['output_directory']}")
                
                if result.get('categories'):
                    print(f"\nCategorization breakdown:")
                    for category, count in result['categories'].items():
                        if count > 0:
                            print(f"  {category}: {count} files")
            else:
                print(f"\n❌ Re-cleaning failed: {result.get('error')}")
                return 1
        
    except KeyboardInterrupt:
        print("\nRe-cleaning interrupted by user")
        return 1
    except Exception as e:
        print(f"Error during re-cleaning: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())