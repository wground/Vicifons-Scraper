#!/usr/bin/env python3
"""
Vicifons Scraper Module

Modular scraper for Latin texts from la.wikisource.org with improved:
- Index page detection (fixes Caesar's commentaries issue)
- Chapter extraction and validation
- Better error handling and recovery
- Cleaner architecture
"""

import asyncio
import aiohttp
import aiofiles
import pywikibot
import re
import logging
from pathlib import Path
from typing import Set, List, Dict, Optional, Tuple
import time
from datetime import datetime, timedelta
import json
import hashlib

from .utils import clean_filename, ProgressTracker, format_duration

class VicifonsScraper:
    """Modular scraper for Vicifons Latin texts."""
    
    def __init__(self, config: Dict):
        """Initialize scraper with configuration."""
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize pywikibot site
        self.site = pywikibot.Site('la', 'wikisource')
        
        # Configuration
        self.output_dir = Path(config['output_dir']) / "raw_scraped"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_concurrent = config.get('max_concurrent', 10)
        self.use_cache = config.get('use_cache', True)
        self.cache_dir = self.output_dir.parent / "cache"
        self.cache_dir.mkdir(exist_ok=True)
        
        # State tracking
        self.scraped_works = set()
        self.failed_works = set()
        
        # Enhanced index detection patterns (fixes Caesar issue)
        self.index_patterns = self._compile_index_patterns()
        
        # Author-specific patterns for known works
        self.known_work_patterns = self._setup_known_works()
        
        self.logger.info(f"Initialized VicifonsScraper with output: {self.output_dir}")
        
        # Rate limiting for concurrent requests
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
    
    def _compile_index_patterns(self) -> List[re.Pattern]:
        """Compile enhanced regex patterns for index detection."""
        patterns = [
            # Standard chapter/book patterns
            r'\[\[([^|\]]+)\|[^|\]]*(?:Liber|Book|Chapter|Capitulum)\s+[IVXLCDM0-9]+',
            r'\[\[([^|\]]+/(?:Liber|Book|Chapter|Capitulum)\s+[IVXLCDM0-9]+)',
            
            # Caesar-specific patterns (the missing piece!)
            r'\[\[([^|\]]+)\|Liber\s+[IVXLCDM]+\]\]',  # [[Work|Liber I]]
            r'\[\[([^|\]]+/Liber\s+[IVXLCDM]+)\]\]',   # [[Work/Liber I]]
            
            # List-style index patterns
            r'^\*\s*\[\[([^|\]]+)\]',    # * [[Chapter]]
            r'^\*\s*\[\[([^|\]]+)\|',    # * [[Chapter|Display]]
            
            # Generic link patterns with Roman numerals
            r'\[\[([^|\]]+)\|[IVXLCDM]+\]',
            
            # Medieval/special patterns
            r'\[\[([^|\]]+)\|\s*[0-9]+\.',  # [[Work|1.]]
        ]
        
        return [re.compile(p, re.IGNORECASE | re.MULTILINE) for p in patterns]
    
    def _setup_known_works(self) -> Dict:
        """Set up patterns for known multi-part works."""
        return {
            'commentarii de bello gallico': {
                'chapters': [f"Commentarii de bello Gallico/Liber {i}" 
                           for i in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII']],
                'type': 'historical_prose'
            },
            'commentarii de bello civili': {
                'chapters': [f"Commentarii de bello civili/Liber {i}" 
                           for i in ['I', 'II', 'III']],
                'type': 'historical_prose'
            },
            'aeneis': {
                'chapters': [f"Aeneis/Liber {i}" 
                           for i in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII']],
                'type': 'epic_poetry'
            },
            'noctes atticae': {
                'chapters': [f"Noctes Atticae/Liber {i}" 
                           for i in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 
                                   'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX']],
                'type': 'miscellany_prose'
            },
            'metamorphoses (ovidius)': {
                'chapters': [f"Metamorphoses (Ovidius)/Liber {i}" 
                           for i in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 
                                   'XI', 'XII', 'XIII', 'XIV', 'XV']],
                'type': 'didactic_poetry'
            },
            # ENHANCEMENT: Add missing major works
            'naturalis historia': {
                'chapters': [f"Naturalis Historia/Liber {i}" for i in [
                    'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X',
                    'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX',
                    'XXI', 'XXII', 'XXIII', 'XXIV', 'XXV', 'XXVI', 'XXVII', 'XXVIII', 'XXIX', 'XXX',
                    'XXXI', 'XXXII', 'XXXIII', 'XXXIV', 'XXXV', 'XXXVI', 'XXXVII'
                ]],
                'type': 'scientific_prose'
            },
            'ab urbe condita': {
                'chapters': [f"Ab Urbe Condita/Liber {i}" for i in [
                    'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 
                    'XXI', 'XXII', 'XXIII', 'XXIV', 'XXV', 'XXVI', 'XXVII', 'XXVIII', 'XXIX', 'XXX',
                    'XXXI', 'XXXII', 'XXXIII', 'XXXIV', 'XXXV', 'XXXVI', 'XXXVII', 'XXXVIII', 'XXXIX', 'XL',
                    'XLI', 'XLII', 'XLIII', 'XLIV', 'XLV'
                ]],
                'type': 'historical_prose'
            },
            'annales (tacitus)': {
                'chapters': [f"Annales (Tacitus)/Liber {i}" for i in [
                    'I', 'II', 'III', 'IV', 'V', 'VI', 'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI'
                ]],  # Only surviving books
                'type': 'historical_prose'
            },
            'historiae (tacitus)': {
                'chapters': [f"Historiae (Tacitus)/Liber {i}" for i in ['I', 'II', 'III', 'IV', 'V']],
                'type': 'historical_prose'
            },
            # ENHANCEMENT: Add more major multi-part works
            'de rerum natura': {
                'chapters': [f"De rerum natura/Liber {i}" for i in ['I', 'II', 'III', 'IV', 'V', 'VI']],
                'type': 'didactic_poetry'
            },
            'institutio oratoria': {
                'chapters': [f"Institutio oratoria/Liber {i}" for i in [
                    'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII'
                ]],
                'type': 'rhetorical_prose'
            },
            'epistulae morales': {
                'chapters': [f"Epistulae morales/Liber {i}" for i in [
                    'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X',
                    'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX'
                ]],
                'type': 'philosophical_prose'
            },
            'de civitate dei': {
                'chapters': [f"De civitate Dei/Liber {i}" for i in [
                    'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X',
                    'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX',
                    'XXI', 'XXII'
                ]],
                'type': 'christian_prose'
            },
            'confessiones': {
                'chapters': [f"Confessiones/Liber {i}" for i in [
                    'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII'
                ]],
                'type': 'autobiographical_prose'
            },
            'bellum iugurthinum': {
                'chapters': [f"Bellum Iugurthinum/Capitulum {i}" for i in range(1, 115)],  # 114 chapters
                'type': 'historical_prose'
            },
            'bellum catilinae': {
                'chapters': [f"Bellum Catilinae/Capitulum {i}" for i in range(1, 62)],  # 61 chapters
                'type': 'historical_prose'
            },
            'georgica': {
                'chapters': [f"Georgica/Liber {i}" for i in ['I', 'II', 'III', 'IV']],
                'type': 'didactic_poetry'
            },
            'eclogae': {
                'chapters': [f"Eclogae/Ecloga {i}" for i in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X']],
                'type': 'pastoral_poetry'
            },
            'ars amatoria': {
                'chapters': [f"Ars amatoria/Liber {i}" for i in ['I', 'II', 'III']],
                'type': 'didactic_poetry'
            },
            'fasti': {
                'chapters': [f"Fasti/Liber {i}" for i in ['I', 'II', 'III', 'IV', 'V', 'VI']],
                'type': 'elegiac_poetry'
            },
            'tristia': {
                'chapters': [f"Tristia/Liber {i}" for i in ['I', 'II', 'III', 'IV', 'V']],
                'type': 'elegiac_poetry'
            },
            'epistulae ex ponto': {
                'chapters': [f"Epistulae ex Ponto/Liber {i}" for i in ['I', 'II', 'III', 'IV']],
                'type': 'elegiac_poetry'
            }
        }
    
    def detect_index_page(self, text: str, title: str = "") -> Tuple[bool, int]:
        """
        Enhanced index page detection with better accuracy.
        
        Returns:
            (is_index, confidence_score)
        """
        if not text:
            return False, 0
        
        # Check against known works first
        title_lower = title.lower().strip()
        if title_lower in self.known_work_patterns:
            self.logger.debug(f"Known multi-part work detected: {title}")
            return True, 100
        
        # Count potential chapter links
        chapter_links = 0
        for pattern in self.index_patterns:
            matches = pattern.findall(text)
            chapter_links += len(matches)
        
        # Calculate text-to-link ratio
        clean_text = re.sub(r'\[\[[^\]]+\]\]', '', text)  # Remove all links
        clean_text = re.sub(r'\{\{[^}]+\}\}', '', clean_text)  # Remove templates
        clean_text = re.sub(r'<[^>]+>', '', clean_text)  # Remove HTML tags
        word_count = len(re.findall(r'\w+', clean_text))
        
        # Enhanced decision logic
        confidence = 0
        
        # Chapter link indicators
        if chapter_links >= 8:
            confidence += 40
        elif chapter_links >= 5:
            confidence += 30
        elif chapter_links >= 3:
            confidence += 20
        
        # Text density indicators (index pages have less content)
        if word_count > 0:
            link_density = chapter_links / (word_count / 10)  # Links per 10 words
            if link_density > 2.0:
                confidence += 30
            elif link_density > 1.0:
                confidence += 20
        
        # Special index indicators
        index_markers = [
            r'==\s*(?:Liber|Book|Chapter)',
            r'INDEX',
            r'thumb.*center',  # Central images often indicate index pages
            r'{{Scriptor\|',   # Author template
        ]
        
        marker_count = sum(1 for marker in index_markers 
                          if re.search(marker, text, re.IGNORECASE))
        confidence += marker_count * 10
        
        # Short text with many links is likely an index
        if word_count < 200 and chapter_links >= 3:
            confidence += 20
        
        is_index = confidence >= 50
        
        if is_index:
            self.logger.info(f"Index page detected: {title} (confidence: {confidence})")
        else:
            self.logger.debug(f"Not index page: {title} (confidence: {confidence})")
        
        return is_index, confidence
    
    def extract_chapter_links(self, text: str, title: str = "") -> List[str]:
        """Extract chapter links with enhanced logic."""
        # Check known works first
        title_lower = title.lower().strip()
        if title_lower in self.known_work_patterns:
            chapters = self.known_work_patterns[title_lower]['chapters']
            self.logger.info(f"Using known chapters for {title}: {len(chapters)} chapters")
            return chapters
        
        # Extract using patterns
        all_links = []
        for pattern in self.index_patterns:
            matches = pattern.findall(text)
            all_links.extend(matches)
        
        # Clean and deduplicate
        unique_links = []
        seen = set()
        
        for link in all_links:
            if not link or link in seen:
                continue
            
            # Skip invalid links
            if any(prefix in link.lower() for prefix in [
                'category:', 'categoria:', 'file:', 'fasciculus:', 
                'template:', 'formula:', 'help:', 'auxilium:',
                'fr:', 'en:', 'de:', 'it:', 'es:'
            ]):
                continue
            
            # Skip very short links
            if len(link.strip()) < 3:
                continue
            
            unique_links.append(link.strip())
            seen.add(link)
        
        self.logger.info(f"Extracted {len(unique_links)} chapter links from {title}")
        return unique_links
    
    async def verify_chapter_exists(self, chapter_title: str) -> bool:
        """Verify that a chapter page exists on Vicifons."""
        try:
            page = pywikibot.Page(self.site, chapter_title)
            return page.exists()
        except Exception as e:
            self.logger.debug(f"Error checking existence of {chapter_title}: {e}")
            return False
    
    async def download_text_content(self, session: aiohttp.ClientSession, 
                                  page: pywikibot.Page) -> Optional[str]:
        """Download text content using ws-export API with fallback."""
        try:
            # Try ws-export API first (cleaner output)
            export_url = "https://ws-export.wmcloud.org/tool/book.php"
            params = {
                'lang': 'la',
                'page': page.title(),
                'format': 'txt'
            }
            
            async with session.get(export_url, params=params, timeout=30) as response:
                if response.status == 200:
                    content = await response.text()
                    if content and len(content.strip()) > 100:
                        # Clean export metadata
                        lines = content.split('\n')
                        clean_lines = []
                        for line in lines:
                            # Skip export metadata lines
                            if not any(marker in line.lower() for marker in [
                                'exported by', 'generated by', 'wikisource export',
                                'ws-export', 'source:', 'https://la.wikisource.org'
                            ]):
                                clean_lines.append(line)
                        
                        cleaned = '\n'.join(clean_lines).strip()
                        if len(cleaned) > 50:
                            return cleaned
            
            # Fallback to direct pywikibot extraction
            return self._extract_with_pywikibot(page)
            
        except Exception as e:
            self.logger.debug(f"Download failed for {page.title()}: {e}")
            return self._extract_with_pywikibot(page)
    
    def _extract_with_pywikibot(self, page: pywikibot.Page) -> Optional[str]:
        """Fallback text extraction using pywikibot."""
        try:
            raw_text = page.text
            if len(raw_text.strip()) < 50:
                return None
            
            # Clean wikitext
            text = raw_text
            
            # Remove templates
            text = re.sub(r'\{\{[^{}]*\}\}', '', text)
            
            # Remove HTML tags
            text = re.sub(r'<[^>]+>', '', text)
            
            # Remove categories
            text = re.sub(r'\[\[Category:[^\]]+\]\]', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\[\[Categoria:[^\]]+\]\]', '', text, flags=re.IGNORECASE)
            
            # Convert wikilinks to plain text
            text = re.sub(r'\[\[([^\]|]+)\|([^\]]+)\]\]', r'\2', text)  # [[link|display]] -> display
            text = re.sub(r'\[\[([^\]]+)\]\]', r'\1', text)  # [[link]] -> link
            
            # Clean whitespace
            text = re.sub(r'\n\s*\n+', '\n\n', text)
            text = re.sub(r'[ \t]+', ' ', text)
            
            return text.strip() if len(text.strip()) > 50 else None
            
        except Exception as e:
            self.logger.error(f"Fallback extraction failed for {page.title()}: {e}")
            return None
    
    async def scrape_single_work(self, work_data: Dict) -> Dict:
        """Scrape a single work and return results."""
        title = work_data['title']
        
        self.logger.info(f"Scraping work: {title}")
        
        try:
            # Get the main page
            page = pywikibot.Page(self.site, title)
            
            if not page.exists():
                self.logger.warning(f"Page does not exist: {title}")
                return {
                    'title': title,
                    'success': False,
                    'error': 'page_not_found',
                    'files_created': 0
                }
            
            # Get page text
            page_text = page.text
            
            # Enhanced index page detection using pre-categorized data
            is_index_pre = work_data.get('is_index_likely', False)
            is_index_detected, confidence = self.detect_index_page(page_text, title)
            
            # Use pre-categorization as a hint but still verify
            is_index = is_index_pre or is_index_detected
            
            if is_index_pre and not is_index_detected:
                self.logger.debug(f"Pre-categorized as index but not detected: {title}")
            elif not is_index_pre and is_index_detected:
                self.logger.debug(f"Detected as index but not pre-categorized: {title}")
            
            files_created = 0
            
            if is_index:
                # Handle index page - extract and download chapters
                self.logger.info(f"Processing index page: {title}")
                
                chapters = self.extract_chapter_links(page_text, title)
                if not chapters:
                    return {
                        'title': title,
                        'success': False,
                        'error': 'no_chapters_found',
                        'files_created': 0
                    }
                
                # Download chapters concurrently
                async with aiohttp.ClientSession() as session:
                    chapter_tasks = []
                    for chapter_title in chapters:
                        task = self._download_chapter(session, chapter_title, title, work_data)
                        chapter_tasks.append(task)
                    
                    # Process chapters in batches
                    batch_size = self.max_concurrent
                    for i in range(0, len(chapter_tasks), batch_size):
                        batch = chapter_tasks[i:i + batch_size]
                        results = await asyncio.gather(*batch, return_exceptions=True)
                        
                        for result in results:
                            if isinstance(result, dict) and result.get('success'):
                                files_created += 1
                        
                        # Brief pause between batches
                        if i + batch_size < len(chapter_tasks):
                            await asyncio.sleep(0.5)
                
            else:
                # Handle single work
                async with aiohttp.ClientSession() as session:
                    content = await self.download_text_content(session, page)
                    
                    if content and len(content.strip()) > 100:
                        # Save the work
                        filename = clean_filename(title) + '.txt'
                        filepath = self.output_dir / filename
                        
                        # Add enhanced metadata header with pre-categorization
                        header_lines = [
                            f"Title: {title}",
                            f"Author: {work_data.get('author', 'Unknown')}",
                            f"Period: {work_data.get('period', 'unknown')}",
                            f"Work Type: {work_data.get('work_type', 'prose')}",
                            f"Completeness: {work_data.get('completeness', 'unknown')}",
                            f"Priority: {work_data.get('priority', 'normal')}",
                            f"Source: {page.full_url()}",
                            f"Scraped: {datetime.now().isoformat()}",
                            f"Content Type: single_work",
                            f"Pre-categorized: {work_data.get('source_type', 'unknown')}"
                        ]
                        
                        header = '\n'.join(header_lines) + f"\n{'-' * 50}\n\n"
                        
                        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                            await f.write(header + content)
                        
                        files_created = 1
                        self.logger.info(f"Saved single work: {filename}")
                    else:
                        return {
                            'title': title,
                            'success': False,
                            'error': 'no_content_extracted',
                            'files_created': 0
                        }
            
            self.scraped_works.add(title)
            return {
                'title': title,
                'success': True,
                'files_created': files_created,
                'is_index': is_index
            }
            
        except Exception as e:
            self.logger.error(f"Error scraping {title}: {e}")
            self.failed_works.add(title)
            return {
                'title': title,
                'success': False,
                'error': str(e),
                'files_created': 0
            }
    
    async def _download_chapter(self, session: aiohttp.ClientSession, 
                               chapter_title: str, parent_work: str, parent_metadata: Dict = None) -> Dict:
        """Download a single chapter."""
        try:
            # Check if chapter exists
            if not await self.verify_chapter_exists(chapter_title):
                self.logger.debug(f"Chapter does not exist: {chapter_title}")
                return {'success': False, 'error': 'chapter_not_found'}
            
            # Get chapter page
            chapter_page = pywikibot.Page(self.site, chapter_title)
            content = await self.download_text_content(session, chapter_page)
            
            if not content or len(content.strip()) < 50:
                return {'success': False, 'error': 'no_content'}
            
            # Create filename
            safe_parent = clean_filename(parent_work)
            safe_chapter = clean_filename(chapter_title.split('/')[-1] if '/' in chapter_title else chapter_title)
            filename = f"{safe_parent}_{safe_chapter}.txt"
            filepath = self.output_dir / filename
            
            # Add enhanced metadata header for chapter
            if parent_metadata:
                header_lines = [
                    f"Title: {chapter_title}",
                    f"Parent Work: {parent_work}",
                    f"Author: {parent_metadata.get('author', 'Unknown')}",
                    f"Period: {parent_metadata.get('period', 'unknown')}",
                    f"Work Type: {parent_metadata.get('work_type', 'prose')}",
                    f"Source: {chapter_page.full_url()}",
                    f"Scraped: {datetime.now().isoformat()}",
                    f"Content Type: chapter",
                    f"Pre-categorized: {parent_metadata.get('source_type', 'unknown')}"
                ]
            else:
                header_lines = [
                    f"Title: {chapter_title}",
                    f"Parent Work: {parent_work}",
                    f"Source: {chapter_page.full_url()}",
                    f"Scraped: {datetime.now().isoformat()}",
                    f"Content Type: chapter"
                ]
            
            header = '\n'.join(header_lines) + f"\n{'-' * 50}\n\n"
            
            # Save file
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(header + content)
            
            self.logger.debug(f"Saved chapter: {filename}")
            return {'success': True, 'filename': filename}
            
        except Exception as e:
            self.logger.error(f"Error downloading chapter {chapter_title}: {e}")
            return {'success': False, 'error': str(e)}
    
    async def scrape_works(self, works: List[Dict]) -> Dict:
        """Scrape a list of works with critical work prioritization."""
        self.logger.info(f"Starting to scrape {len(works)} works")
        
        progress = ProgressTracker(len(works), "Scraping works")
        results = {
            'success_count': 0,
            'failure_count': 0,
            'total_files': 0,
            'details': []
        }
        
        # Separate critical works for special handling
        critical_works = [w for w in works if w.get('priority') == 'critical']
        other_works = [w for w in works if w.get('priority') != 'critical']
        
        # Process critical works first with enhanced handling
        if critical_works:
            self.logger.info(f"Processing {len(critical_works)} CRITICAL works first")
            
            for work in critical_works:
                self.logger.info(f"Processing CRITICAL: {work['title']}")
                
                # Enhanced scraping for critical works
                result = await self.scrape_critical_work_enhanced(work)
                
                if isinstance(result, Exception):
                    results['failure_count'] += 1
                    results['details'].append({
                        'title': work['title'],
                        'success': False,
                        'error': str(result),
                        'priority': 'critical'
                    })
                elif result.get('success'):
                    results['success_count'] += 1
                    results['total_files'] += result.get('files_created', 0)
                    results['details'].append(result)
                    self.logger.info(f"CRITICAL SUCCESS: {work['title']} - {result.get('files_created', 0)} files")
                else:
                    results['failure_count'] += 1
                    results['details'].append(result)
                    self.logger.warning(f"CRITICAL FAILED: {work['title']}")
                
                progress.update()
        
        # Process other works concurrently in batches
        if other_works:
            batch_size = min(self.max_concurrent, 5)  # Smaller batches for scraping
            
            for i in range(0, len(other_works), batch_size):
                batch = other_works[i:i + batch_size]
                tasks = [self.scrape_single_work(work) for work in batch]
                
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, Exception):
                        results['failure_count'] += 1
                        results['details'].append({
                            'success': False,
                            'error': str(result)
                        })
                    elif result.get('success'):
                        results['success_count'] += 1
                        results['total_files'] += result.get('files_created', 0)
                        results['details'].append(result)
                    else:
                        results['failure_count'] += 1
                        results['details'].append(result)
                    
                    progress.update()
                
                # Pause between batches to be respectful
                if i + batch_size < len(other_works):
                    await asyncio.sleep(1.0)
        
        progress.finish()
        
        self.logger.info(f"Scraping complete: {results['success_count']} works, {results['total_files']} files")
        return results
    
    async def scrape_works_enhanced(self, works: List[Dict]) -> Dict:
        """Enhanced scraping with pre-categorized metadata."""
        self.logger.info(f"Starting enhanced scraping of {len(works)} pre-categorized works")
        
        # Use the existing scrape_works method but with enhanced metadata
        results = await self.scrape_works(works)
        
        # Add enhanced metadata to results
        results['pre_categorized'] = True
        results['metadata'] = {
            'periods': {period: len([w for w in works if w.get('period') == period]) 
                       for period in ['classical', 'post_classical', 'unknown']},
            'types': {work_type: len([w for w in works if w.get('work_type') == work_type]) 
                     for work_type in ['prose', 'poetry', 'unknown']},
            'completeness': {comp: len([w for w in works if w.get('completeness') == comp]) 
                           for comp in ['complete', 'partial', 'fragment', 'unknown']}
        }
        
        return results
    
    async def scrape_chapter(self, chapter_title: str, parent_work: str) -> Dict:
        """Scrape a single chapter (alias for _download_chapter)."""
        async with aiohttp.ClientSession() as session:
            return await self._download_chapter(session, chapter_title, parent_work)
    
    async def scrape_critical_work_enhanced(self, work: Dict) -> Dict:
        """Enhanced scraping for critical works with known patterns and retries."""
        title = work['title']
        title_lower = title.lower().strip()
        
        # Strategy 1: Use known work patterns if available
        if title_lower in self.known_work_patterns:
            self.logger.info(f"Using known pattern for critical work: {title}")
            chapters = self.known_work_patterns[title_lower]['chapters']
            
            files_created = 0
            successful_chapters = []
            
            for chapter in chapters:
                try:
                    chapter_result = await self.scrape_chapter(chapter, title)
                    if chapter_result and chapter_result.get('success'):
                        files_created += 1
                        successful_chapters.append(chapter)
                        self.logger.debug(f"Critical work chapter success: {chapter}")
                    else:
                        self.logger.warning(f"Critical work chapter failed: {chapter}")
                except Exception as e:
                    self.logger.warning(f"Critical work chapter exception {chapter}: {e}")
                    continue
            
            if files_created > 0:
                return {
                    'success': True,
                    'files_created': files_created,
                    'title': title,
                    'strategy': 'known_patterns',
                    'chapters_found': successful_chapters,
                    'priority': 'critical'
                }
        
        # Strategy 2: Regular scraping with retries for critical works
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Critical work regular scraping attempt {attempt + 1}/{max_retries}: {title}")
                
                result = await self.scrape_single_work(work)
                if result and result.get('success'):
                    result['priority'] = 'critical'
                    result['attempts'] = attempt + 1
                    return result
                    
                self.logger.warning(f"Critical work attempt {attempt + 1} unsuccessful: {result}")
                    
            except Exception as e:
                self.logger.warning(f"Critical work attempt {attempt + 1} exception: {e}")
                if attempt == max_retries - 1:
                    return {
                        'success': False,
                        'error': f'max_retries_exceeded: {str(e)}',
                        'title': title,
                        'priority': 'critical'
                    }
                    
            # Wait before retry with exponential backoff
            await asyncio.sleep(1.5 ** attempt)
        
        return {
            'success': False,
            'error': 'all_strategies_failed',
            'title': title,
            'priority': 'critical'
        }
    
    async def scrape_category(self, category: str) -> Dict:
        """Scrape all works from a Vicifons category."""
        self.logger.info(f"Scraping category: {category}")
        
        try:
            # Get category page
            category_page = pywikibot.Category(self.site, category)
            
            # Get all pages in category
            pages = []
            for page in category_page.articles():
                if page.namespace() == 0:  # Main namespace only
                    pages.append({
                        'title': page.title(),
                        'author': self._extract_author_from_title(page.title()),
                        'estimated_period': self._estimate_period_from_category(category),
                        'categories': [category]
                    })
            
            self.logger.info(f"Found {len(pages)} pages in category {category}")
            
            # Scrape all pages
            return await self.scrape_works(pages)
            
        except Exception as e:
            self.logger.error(f"Error scraping category {category}: {e}")
            return {'success_count': 0, 'failure_count': 0, 'details': []}
    
    async def scrape_comprehensive_corpus(self) -> Dict:
        """Scrape comprehensive corpus using filtered and pre-categorized XML dump analysis."""
        self.logger.info("Loading filtered and categorized Latin works list from XML dump analysis")
        
        # Try filtered works first, fallback to comprehensive
        import json
        filtered_file = Path(self.config['output_dir']).parent / "filtered_latin_works.json"
        comprehensive_file = Path(self.config['output_dir']).parent / "all_latin_works.json"
        
        works_data = None
        source_type = None
        
        # Try filtered file first (preferred)
        if filtered_file.exists():
            try:
                with open(filtered_file, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)
                    works_data = file_data.get('works', file_data)  # Handle both formats
                    source_type = 'filtered'
                    self.logger.info(f"Using filtered and categorized works list: {len(works_data)} works")
            except Exception as e:
                self.logger.warning(f"Error loading filtered works: {e}")
        
        # Fallback to comprehensive file
        if works_data is None and comprehensive_file.exists():
            try:
                with open(comprehensive_file, 'r', encoding='utf-8') as f:
                    works_data = json.load(f)
                    source_type = 'comprehensive'
                    self.logger.info(f"Using comprehensive works list: {len(works_data)} works")
            except Exception as e:
                self.logger.warning(f"Error loading comprehensive works: {e}")
        
        # Final fallback to category-based scraping
        if works_data is None:
            self.logger.error("No works files found")
            self.logger.info("Falling back to category-based scraping...")
            return await self.scrape_all_categories_fallback()
        
        try:
            # Convert to expected format with enhanced metadata
            works = []
            for work_data in works_data:
                work_dict = {
                    'title': work_data['title'],
                    'author': work_data.get('author', 'Unknown'),
                    'priority': work_data.get('priority', 'normal'),
                    'content_length': work_data.get('content_length', 0),
                    # Enhanced metadata for pre-categorized works
                    'period': work_data.get('period', 'unknown'),
                    'work_type': work_data.get('work_type', 'prose'),
                    'completeness': work_data.get('completeness', 'unknown'),
                    'is_index_likely': work_data.get('is_index_likely', False),
                    'source_type': source_type
                }
                works.append(work_dict)
            
            self.logger.info(f"Loaded {len(works)} works from {source_type} analysis")
            
            # Log categorization statistics
            if source_type == 'filtered':
                from collections import Counter
                periods = Counter(work['period'] for work in works)
                types = Counter(work['work_type'] for work in works)
                priorities = Counter(work['priority'] for work in works)
                
                self.logger.info(f"Work distribution - Periods: {dict(periods)}")
                self.logger.info(f"Work distribution - Types: {dict(types)}")
                self.logger.info(f"Work distribution - Priorities: {dict(priorities)}")
            
            # Scrape all works with enhanced metadata
            scrape_results = await self.scrape_works_enhanced(works)
            
            return {
                'success_count': scrape_results['success_count'],
                'failure_count': scrape_results['failure_count'],
                'total_files': scrape_results['total_files'],
                'categories_processed': 1,
                'source_type': source_type,
                'details': scrape_results['details'][:10]
            }
            
        except Exception as e:
            self.logger.error(f"Error processing works: {e}")
            self.logger.info("Falling back to category-based scraping...")
            return await self.scrape_all_categories_fallback()
    
    async def scrape_all_categories_fallback(self) -> Dict:
        """Fallback to original category-based scraping."""
        # Define comprehensive categories for Latin texts up to 15th century
        categories = [
            # Classical period
            "Categoria:Auctores Romani",
            "Categoria:Saeculum I a.C.n.",
            "Categoria:Saeculum I",
            "Categoria:Saeculum II",
            "Categoria:Saeculum III",
            "Categoria:Saeculum IV",
            "Categoria:Saeculum V",
            
            # Medieval period
            "Categoria:Saeculum VI",
            "Categoria:Saeculum VII", 
            "Categoria:Saeculum VIII",
            "Categoria:Saeculum IX",
            "Categoria:Saeculum X",
            "Categoria:Saeculum XI",
            "Categoria:Saeculum XII",
            "Categoria:Saeculum XIII",
            "Categoria:Saeculum XIV",
            "Categoria:Saeculum XV",
            
            # Literary genres
            "Categoria:Epica",
            "Categoria:Lyrica",
            "Categoria:Orationes",
            "Categoria:Historici",
            "Categoria:Philosophi",
            "Categoria:Theologia",
            "Categoria:Epistulae",
            "Categoria:Carmina",
            "Categoria:Satirae",
            
            # Major works and authors
            "Categoria:Marcus Tullius Cicero",
            "Categoria:Gaius Iulius Caesar",
            "Categoria:Publius Vergilius Maro",
            "Categoria:Quintus Horatius Flaccus",
            "Categoria:Publius Ovidius Naso",
            "Categoria:Marcus Valerius Martialis",
            "Categoria:Decimus Iunius Iuvenalis",
            "Categoria:Gaius Plinius Secundus",
            "Categoria:Publius Cornelius Tacitus",
            "Categoria:Gaius Suetonius Tranquillus",
            "Categoria:Titus Livius",
            "Categoria:Lucius Annaeus Seneca",
            
            # Medieval authors
            "Categoria:Augustinus Hipponensis",
            "Categoria:Hieronymus",
            "Categoria:Gregorius Magnus",
            "Categoria:Isidorus Hispalensis",
            "Categoria:Beda Venerabilis",
            "Categoria:Alcuinus",
            "Categoria:Thomas Aquinas",
            "Categoria:Boethius",
        ]
        
        self.logger.info(f"Starting to scrape {len(categories)} categories")
        
        all_pages = []  # Collect all pages
        results = {
            'success_count': 0,
            'failure_count': 0,
            'total_files': 0,
            'categories_processed': 0,
            'details': []
        }
        
        # Process categories with concurrency control
        async def process_category(category):
            async with self.semaphore:  # Rate limiting
                try:
                    category_page = pywikibot.Category(self.site, category)
                    category_pages = []
                    
                    count = 0
                    for page in category_page.articles():
                        if page.namespace() == 0:  # Main namespace only
                            # Removed arbitrary limit to collect full corpus
                            # if count >= 1000:  # Limit per category to avoid overwhelming
                            #     break
                            
                            page_title = page.title()
                            # Create page dict
                            page_dict = {
                                'title': page_title,
                                'author': self._extract_author_from_title(page_title),
                                'estimated_period': self._estimate_period_from_category(category),
                                'categories': [category]
                            }
                            category_pages.append(page_dict)
                            count += 1
                    
                    # ENHANCEMENT: Also check Scriptor namespace for author categories (EXPANDED LIST)
                    major_authors = [
                        'Caesar', 'Cicero', 'Vergilius', 'Plinius', 'Livius', 'Tacitus', 
                        'Ovidius', 'Horatius', 'Quintilianus', 'Seneca', 'Suetonius',
                        'Martialis', 'Iuvenalis', 'Catullus', 'Propertius', 'Tibullus',
                        'Lucanus', 'Statius', 'Silius', 'Valerius Flaccus', 'Persius',
                        'Apuleius', 'Gellius', 'Aulus Gellius', 'Plautus', 'Terentius',
                        'Lucretius', 'Sallustius', 'Nepos', 'Curtius', 'Ammianus',
                        'Augustinus', 'Hieronymus', 'Ambrosius', 'Boethius', 'Cassiodorus',
                        'Gregorius', 'Isidorus', 'Beda', 'Alcuinus', 'Einhard', 'Notker',
                        'Thomas Aquinas', 'Bernardus', 'Anselmus', 'Abelardus'
                    ]
                    if category.startswith('Categoria:') and any(author in category for author in major_authors):
                        author_name = category.replace('Categoria:', '').strip()
                        scriptor_page_title = f"Scriptor:{author_name}"
                        
                        try:
                            scriptor_page = pywikibot.Page(self.site, scriptor_page_title)
                            if scriptor_page.exists():
                                # Extract works from author page
                                author_text = scriptor_page.text
                                work_links = re.findall(r'\[\[([^|\]]+)\]', author_text)
                                
                                for link in work_links:
                                    if ':' not in link:  # Avoid categories, files, etc.
                                        try:
                                            work_page = pywikibot.Page(self.site, link)
                                            if work_page.exists() and work_page.namespace() == 0:
                                                page_dict = {
                                                    'title': link,
                                                    'author': self._extract_author_from_title(link),
                                                    'estimated_period': self._estimate_period_from_category(category),
                                                    'categories': [category, 'scriptor_found']
                                                }
                                                category_pages.append(page_dict)
                                        except:
                                            continue
                                
                                self.logger.info(f"Found {len(work_links)} works from {scriptor_page_title}")
                        except Exception as e:
                            self.logger.debug(f"No scriptor page for {author_name}: {e}")
                    
                    self.logger.info(f"Category {category}: {len(category_pages)} pages")
                    return category_pages
                    
                except Exception as e:
                    self.logger.error(f"Error processing category {category}: {e}")
                    return []
        
        # Process categories in batches
        batch_size = 10
        for i in range(0, len(categories), batch_size):
            batch = categories[i:i + batch_size]
            tasks = [process_category(cat) for cat in batch]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for category_pages in batch_results:
                if isinstance(category_pages, list):
                    all_pages.extend(category_pages)
                    results['categories_processed'] += 1
            
            # Pause between batches
            await asyncio.sleep(2.0)
        
        # Remove duplicates based on title
        seen_titles = set()
        unique_pages = []
        for page in all_pages:
            if page['title'] not in seen_titles:
                unique_pages.append(page)
                seen_titles.add(page['title'])
        
        self.logger.info(f"Total unique pages found: {len(unique_pages)} (from {len(all_pages)} total)")
        
        # Scrape all collected pages
        if unique_pages:
            scrape_results = await self.scrape_works(unique_pages)
            results.update(scrape_results)
        
        return results
    
    def _extract_author_from_title(self, title: str) -> str:
        """Extract author name from page title."""
        # Simple heuristic for author extraction
        if '/' in title:
            return title.split('/')[0]
        
        # Check for known author patterns
        author_patterns = {
            'cicero': 'Marcus Tullius Cicero',
            'caesar': 'Gaius Iulius Caesar',
            'vergilius': 'Publius Vergilius Maro',
            'virgil': 'Publius Vergilius Maro',
            'horatius': 'Quintus Horatius Flaccus',
            'ovidius': 'Publius Ovidius Naso',
            'martialis': 'Marcus Valerius Martialis',
            'tacitus': 'Publius Cornelius Tacitus',
            'livius': 'Titus Livius',
            'seneca': 'Lucius Annaeus Seneca',
            'augustinus': 'Augustinus Hipponensis',
        }
        
        title_lower = title.lower()
        for pattern, author in author_patterns.items():
            if pattern in title_lower:
                return author
        
        return "Unknown"
    
    def _estimate_period_from_category(self, category: str) -> str:
        """Estimate time period from category name."""
        category_lower = category.lower()
        
        # Classical markers
        if any(marker in category_lower for marker in ['romani', 'saeculum i', 'saeculum ii', 'saeculum iii', 'saeculum iv']):
            return 'classical'
        
        # Medieval markers
        if any(marker in category_lower for marker in ['saeculum v', 'saeculum vi', 'saeculum vii', 
                                                       'saeculum viii', 'saeculum ix', 'saeculum x',
                                                       'saeculum xi', 'saeculum xii', 'saeculum xiii',
                                                       'saeculum xiv', 'saeculum xv', 'theologia']):
            return 'post_classical'
        
        return 'unknown'