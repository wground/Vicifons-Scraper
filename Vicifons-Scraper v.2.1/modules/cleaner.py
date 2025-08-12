#!/usr/bin/env python3
"""
Text Cleaner Module

Modular text cleaner for Latin texts with:
- Intelligent content cleaning
- Metadata removal
- Index file detection and handling
- LatinCy integration for NLP processing
- Orthography standardization
"""

import asyncio
import aiofiles
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import unicodedata

from .utils import clean_filename, ProgressTracker, validate_latin_text, detect_text_type, calculate_text_stats
from .orthography import OrthographyStandardizer

class TextCleaner:
    """Modular text cleaner for Latin texts."""
    
    def __init__(self, config: Dict):
        """Initialize text cleaner with configuration."""
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.output_dir = Path(config['output_dir']) / "cleaned_texts"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize orthography standardizer
        self.orthography = OrthographyStandardizer()
        
        # NLP processing
        self.enable_nlp = config.get('enable_nlp', False)
        self.nlp_processor = None
        
        if self.enable_nlp:
            try:
                self._initialize_latincy()
            except ImportError as e:
                self.logger.warning(f"LatinCy not available: {e}")
                self.enable_nlp = False
        
        # Cleaning patterns
        self.metadata_patterns = self._compile_metadata_patterns()
        self.content_patterns = self._compile_content_patterns()
        
        self.logger.info(f"Initialized TextCleaner with output: {self.output_dir}")
    
    def _initialize_latincy(self):
        """Initialize LatinCy for NLP processing."""
        try:
            import spacy
            # Try to load LatinCy model
            self.nlp_processor = spacy.load("la_core_web_sm")
            self.logger.info("LatinCy loaded successfully")
        except OSError:
            self.logger.warning("LatinCy model not found. Install with: pip install latincy")
            self.enable_nlp = False
    
    def _compile_metadata_patterns(self) -> List[re.Pattern]:
        """Compile patterns for metadata removal."""
        patterns = [
            # Headers added by scraper
            r'^Title:.*$',
            r'^Source:.*$', 
            r'^Scraped:.*$',
            r'^Type:.*$',
            r'^Parent Work:.*$',
            r'^Category:.*$',
            r'^Text Type:.*$',
            r'^-{10,}$',  # Separator lines
            
            # Vicifons metadata
            r'{{Scriptor\|[^}]+}}',
            r'{{Opus\|[^}]+}}',
            r'{{Caput\|[^}]+}}',
            r'{{Header[^}]*}}',
            
            # Navigation templates
            r'{{Navigatio[^}]*}}',
            r'{{Prev.*?Next[^}]*}}',
            
            # Categories (shouldn't be in main text but just in case)
            r'\[\[Categoria:[^\]]+\]\]',
            r'\[\[Category:[^\]]+\]\]',
            
            # File/image references
            r'\[\[File:[^\]]+\]\]',
            r'\[\[Fasciculus:[^\]]+\]\]',
            r'\[\[Image:[^\]]+\]\]',
        ]
        
        return [re.compile(p, re.IGNORECASE | re.MULTILINE) for p in patterns]
    
    def _compile_content_patterns(self) -> List[Tuple[re.Pattern, str]]:
        """Compile patterns for content cleaning."""
        patterns = [
            # Remove HTML remnants
            (re.compile(r'<[^>]+>'), ''),
            
            # Remove reference markers
            (re.compile(r'\[(?:Note|Nota):[^\]]+\]'), ''),
            (re.compile(r'\{\{(?:ref|nota)[^}]*\}\}'), ''),
            
            # Remove editorial brackets (but preserve text)
            (re.compile(r'\[(\w+)\]'), r'\1'),  # [word] -> word
            
            # Clean up formatting
            (re.compile(r"'''([^']+)'''"), r'\1'),  # Remove bold
            (re.compile(r"''([^']+)''"), r'\1'),    # Remove italic
            
            # Fix spacing issues
            (re.compile(r'\s+'), ' '),              # Multiple spaces to single
            (re.compile(r'\n\s*\n\s*\n+'), '\n\n'), # Multiple newlines to double
            
            # Remove page numbers and references
            (re.compile(r'\b\d+\s*\|\s*\d+\b'), ''), # Page numbers like "123 | 456"
            (re.compile(r'\{\{\s*p\.\s*\d+\s*\}\}'), ''), # {{p.123}}
        ]
        
        return patterns
    
    def detect_problematic_index_file(self, content: str, filename: str) -> bool:
        """
        Detect if a file is incorrectly an index file that should be excluded.
        This fixes the issue where legitimate text files were tagged as index files.
        """
        # Quick content checks
        if len(content.strip()) < 100:
            return True  # Too short to be real content
        
        # Count links vs text ratio
        links = re.findall(r'\[\[[^\]]+\]\]', content)
        words = re.findall(r'\w+', content)
        
        if len(words) == 0:
            return True  # No actual words
        
        link_to_word_ratio = len(links) / len(words)
        
        # If more than 30% of content is links, likely an index
        if link_to_word_ratio > 0.3:
            self.logger.debug(f"High link ratio detected in {filename}: {link_to_word_ratio:.2f}")
            return True
        
        # Check for index-specific patterns
        index_indicators = [
            r'==\s*(?:Index|Liber|Book|Chapter)',
            r'^\s*\*\s*\[\[.*\]\]\s*$',  # Lines that are just links
            r'{{Scriptor\|',
            r'thumb\|.*center',  # Centered images
        ]
        
        indicator_matches = sum(1 for pattern in index_indicators 
                              if re.search(pattern, content, re.IGNORECASE | re.MULTILINE))
        
        # Strong indicators of index page
        if indicator_matches >= 2:
            self.logger.debug(f"Index indicators found in {filename}: {indicator_matches}")
            return True
        
        # Check filename patterns that suggest index files
        filename_lower = filename.lower()
        index_filename_patterns = [
            'index', 'contents', 'toc', 'list',
            'periochae.txt.txt',  # Specific problematic pattern
            '.txt.txt',           # Double extension suggests processing error
        ]
        
        if any(pattern in filename_lower for pattern in index_filename_patterns):
            # But only if content also looks like an index
            if len(links) >= 5 and len(words) < 500:
                return True
        
        return False
    
    def clean_metadata(self, content: str) -> str:
        """Remove metadata headers and wiki markup."""
        # Remove metadata patterns
        for pattern in self.metadata_patterns:
            content = pattern.sub('', content)
        
        # Apply content cleaning patterns
        for pattern, replacement in self.content_patterns:
            content = pattern.sub(replacement, content)
        
        return content.strip()
    
    def clean_wikitext_remnants(self, content: str) -> str:
        """Clean remaining wikitext markup."""
        # Convert remaining wikilinks to plain text
        content = re.sub(r'\[\[([^\]|]+)\|([^\]]+)\]\]', r'\2', content)  # [[link|display]] -> display
        content = re.sub(r'\[\[([^\]]+)\]\]', r'\1', content)  # [[link]] -> link
        
        # Remove remaining templates
        content = re.sub(r'\{\{[^{}]*\}\}', '', content)
        
        # Remove comments
        content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)
        
        # Clean up tables
        content = re.sub(r'\{\|.*?\|\}', '', content, flags=re.DOTALL)
        
        return content
    
    def validate_cleaned_content(self, content: str, original_filename: str) -> Tuple[bool, str]:
        """Validate that cleaned content is legitimate Latin text."""
        if not content or len(content.strip()) < 50:
            return False, "too_short"
        
        # Use Latin validation
        validation = validate_latin_text(content)
        
        if not validation['is_latin']:
            return False, validation['reason']
        
        # Check for minimum content requirements
        words = re.findall(r'\w+', content)
        if len(words) < 20:
            return False, "insufficient_words"
        
        # Check for reasonable content density
        lines = [line.strip() for line in content.split('\n') if line.strip()]
        if len(lines) == 0:
            return False, "no_content_lines"
        
        avg_line_length = sum(len(line) for line in lines) / len(lines)
        if avg_line_length < 10:  # Very short lines suggest formatting issues
            return False, "lines_too_short"
        
        return True, "valid"
    
    async def clean_single_file(self, input_path: Path) -> Dict:
        """Clean a single text file."""
        self.logger.debug(f"Cleaning file: {input_path.name}")
        
        try:
            # Read original file
            async with aiofiles.open(input_path, 'r', encoding='utf-8') as f:
                original_content = await f.read()
            
            if not original_content.strip():
                return {
                    'filename': input_path.name,
                    'success': False,
                    'error': 'empty_file',
                    'action': 'skipped'
                }
            
            # Check if this is a problematic index file
            if self.detect_problematic_index_file(original_content, input_path.name):
                self.logger.debug(f"Skipping index file: {input_path.name}")
                return {
                    'filename': input_path.name,
                    'success': True,
                    'action': 'skipped_index',
                    'reason': 'detected_as_index_file'
                }
            
            # Clean the content
            cleaned = original_content
            
            # Step 1: Remove metadata
            cleaned = self.clean_metadata(cleaned)
            
            # Step 2: Clean wikitext remnants
            cleaned = self.clean_wikitext_remnants(cleaned)
            
            # Step 3: Standardize orthography
            if hasattr(self.orthography, 'standardize'):
                cleaned = self.orthography.standardize(cleaned)
            
            # Step 4: Final cleanup
            cleaned = self._final_cleanup(cleaned)
            
            # Validate cleaned content
            is_valid, reason = self.validate_cleaned_content(cleaned, input_path.name)
            
            if not is_valid:
                return {
                    'filename': input_path.name,
                    'success': False,
                    'error': f'validation_failed_{reason}',
                    'action': 'rejected'
                }
            
            # Determine text type
            text_type = detect_text_type(cleaned)
            
            # Calculate statistics
            stats = calculate_text_stats(cleaned)
            
            # Create output filename and path
            clean_filename = input_path.stem
            if clean_filename.endswith('.txt'):
                clean_filename = clean_filename[:-4]
            
            output_filename = f"{clean_filename}_cleaned.txt"
            output_path = self.output_dir / output_filename
            
            # Add processing header
            header = (
                f"# Cleaned Latin Text\n"
                f"# Original: {input_path.name}\n"
                f"# Processed: {cleaned[:50]}...\n"
                f"# Type: {text_type}\n"
                f"# Stats: {stats['word_count']} words, {stats['line_count']} lines\n"
                f"# ---\n\n"
            )
            
            # Save cleaned file
            async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
                await f.write(cleaned)
            
            # NLP processing if enabled
            nlp_results = None
            if self.enable_nlp and self.nlp_processor:
                nlp_results = await self._process_with_nlp(cleaned)
            
            return {
                'filename': input_path.name,
                'output_filename': output_filename,
                'success': True,
                'action': 'cleaned',
                'text_type': text_type,
                'stats': stats,
                'nlp_results': nlp_results,
                'validation_reason': reason
            }
            
        except Exception as e:
            self.logger.error(f"Error cleaning {input_path.name}: {e}")
            return {
                'filename': input_path.name,
                'success': False,
                'error': str(e),
                'action': 'failed'
            }
    
    def _final_cleanup(self, content: str) -> str:
        """Final cleanup and normalization."""
        # Normalize Unicode
        content = unicodedata.normalize('NFKC', content)
        
        # Fix punctuation spacing
        content = re.sub(r'\s+([,.;:!?])', r'\1', content)  # Remove space before punctuation
        content = re.sub(r'([,.;:!?])(\w)', r'\1 \2', content)  # Add space after punctuation
        
        # Clean up quotes
        content = re.sub(r'"([^"]*)"', r'"\1"', content)  # Normalize quotes
        
        # Final whitespace cleanup
        content = re.sub(r'[ \t]+', ' ', content)  # Multiple spaces to single
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)  # Multiple newlines to double
        content = re.sub(r'^\s+|\s+$', '', content)  # Trim
        
        return content
    
    async def _process_with_nlp(self, text: str) -> Optional[Dict]:
        """Process text with LatinCy NLP pipeline."""
        if not self.nlp_processor:
            return None
        
        try:
            # Process text (limit length for performance)
            max_chars = 10000
            text_sample = text[:max_chars] if len(text) > max_chars else text
            
            doc = self.nlp_processor(text_sample)
            
            # Extract linguistic features
            results = {
                'tokens': len(doc),
                'sentences': len(list(doc.sents)),
                'pos_tags': {},
                'lemmas': [],
                'named_entities': []
            }
            
            # Count POS tags
            for token in doc:
                pos = token.pos_
                results['pos_tags'][pos] = results['pos_tags'].get(pos, 0) + 1
            
            # Extract sample lemmas (first 50)
            results['lemmas'] = [token.lemma_ for token in doc[:50] if token.lemma_]
            
            # Extract named entities
            for ent in doc.ents:
                results['named_entities'].append({
                    'text': ent.text,
                    'label': ent.label_,
                    'start': ent.start_char,
                    'end': ent.end_char
                })
            
            return results
            
        except Exception as e:
            self.logger.debug(f"NLP processing failed: {e}")
            return None
    
    async def clean_directory(self, input_dir: Path) -> Dict:
        """Clean all text files in a directory."""
        if not input_dir.exists():
            self.logger.error(f"Input directory does not exist: {input_dir}")
            return {'success_count': 0, 'failure_count': 0, 'details': []}
        
        # Find all text files
        text_files = list(input_dir.rglob('*.txt'))
        
        if not text_files:
            self.logger.warning(f"No .txt files found in {input_dir}")
            return {'success_count': 0, 'failure_count': 0, 'details': []}
        
        self.logger.info(f"Cleaning {len(text_files)} files from {input_dir}")
        
        progress = ProgressTracker(len(text_files), "Cleaning files")
        results = {
            'success_count': 0,
            'failure_count': 0,
            'skipped_count': 0,
            'total_files': len(text_files),
            'details': []
        }
        
        # Process files in batches
        batch_size = 10  # Text processing is I/O bound
        
        for i in range(0, len(text_files), batch_size):
            batch = text_files[i:i + batch_size]
            tasks = [self.clean_single_file(file_path) for file_path in batch]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    results['failure_count'] += 1
                    results['details'].append({
                        'success': False,
                        'error': str(result)
                    })
                else:
                    if result.get('success') and result.get('action') == 'cleaned':
                        results['success_count'] += 1
                    elif result.get('action') in ['skipped_index', 'skipped']:
                        results['skipped_count'] += 1
                    else:
                        results['failure_count'] += 1
                    
                    results['details'].append(result)
                
                progress.update()
        
        progress.finish()
        
        self.logger.info(
            f"Cleaning complete: {results['success_count']} cleaned, "
            f"{results['skipped_count']} skipped, {results['failure_count']} failed"
        )
        
        return results