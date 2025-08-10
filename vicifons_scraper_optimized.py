#!/usr/bin/env python3 
# YES I REALIZE THIS IS CRAZY SPAGHETTI CODE!!! IF IT WORKS IT WORKS!!!
"""
Vicifons (la.wikisource.org) Latin Text Scraper - Fully Optimized Version

High-performance scraper that properly handles:
- Author-based works (Scriptor: namespace)
- Index pages with individual chapters/books  
- Comprehensive category traversal
- All major classical works (Caesar, Livy, Gellius, etc.)
"""

import pywikibot
import re
import logging
import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from typing import Set, List, Tuple, Dict, Optional
import time
import json
import hashlib
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pickle


class ComprehensiveVicifonsDownloader:
    def __init__(self, output_dir: str = "downloaded_texts", single_folder: bool = False, 
                 max_concurrent: int = 10, use_cache: bool = True, cache_duration_hours: int = 12):
        """Initialize the comprehensive downloader."""
        self.site = pywikibot.Site('la', 'wikisource')
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.single_folder = single_folder
        self.max_concurrent = max_concurrent
        self.use_cache = use_cache
        self.cache_duration_hours = cache_duration_hours
        
        # Create subdirectories
        if not single_folder:
            (self.output_dir / "Latinitas_Romana").mkdir(exist_ok=True)
            (self.output_dir / "Latinitas_Mediaevalis").mkdir(exist_ok=True)
            (self.output_dir / "Saeculum_XV").mkdir(exist_ok=True)
        
        # Cache and state management
        self.cache_dir = self.output_dir / ".cache"
        self.cache_dir.mkdir(exist_ok=True)
        self.state_file = self.cache_dir / "download_state.json"
        
        self.downloaded_pages = set()
        self.failed_pages = set()
        self.all_found_pages = set()
        
        # Initialize logging first
        self.logger = logging.getLogger(__name__)
        
        self.load_state()
        
        # Enhanced exclusion criteria
        self.quick_exclude_patterns = {
            'c.i.l', 'corpus inscriptionum', 'inscriptio', 'ae ', 'i.l.s',
            'fragment', 'fragmenta', 'fragmentum', 'excerpta', 'epitaph'
        }
        
        self.excluded_categories = {
            'categoria:saeculi incogniti opera', 'categoria:inscriptiones',
            'categoria:epitaphia', 'categoria:carmina epigraphica', 
            'categoria:fragmenta', 'categoria:testimonia'
        }
        
        # Enhanced genre classification using categoria:genera
        self.genre_categories = {
            # Poetry genres
            'poetry': {
                'categoria:carmina', 'categoria:poesis', 'categoria:versus',
                'categoria:elegia', 'categoria:elegiae', 'categoria:epigrammata', 
                'categoria:satirae', 'categoria:satira', 'categoria:eclogae', 
                'categoria:georgica', 'categoria:bucolica', 'categoria:hymni', 
                'categoria:odes', 'categoria:epic', 'categoria:epica',
                'categoria:carmen epicum', 'categoria:heroica', 'categoria:lyrica'
            },
            # Prose genres
            'prose': {
                'categoria:historia', 'categoria:historiae', 'categoria:oratio', 
                'categoria:orationes', 'categoria:epistolae', 'categoria:epistola',
                'categoria:commentarii', 'categoria:annales', 'categoria:vitae', 
                'categoria:vita', 'categoria:biographia', 'categoria:philosophia',
                'categoria:rhetorica', 'categoria:de re', 'categoria:tractatus',
                'categoria:dialogus', 'categoria:narratio', 'categoria:prosa'
            },
            # Mixed/Drama genres
            'mixed': {
                'categoria:comoedia', 'categoria:tragoedia', 'categoria:drama',
                'categoria:fabula', 'categoria:dialogus mixtus'
            }
        }
        
        # Legacy categories for backward compatibility
        self.poetry_categories = self.genre_categories['poetry']
        self.prose_categories = self.genre_categories['prose']
        
        # Pre-compiled patterns for performance
        self.fragmentary_patterns = [
            re.compile(r'\[\.{3}\]'),
            re.compile(r'\[lacuna\]'),
            re.compile(r'fragmenta?\s+', re.IGNORECASE),
            re.compile(r'deest|desunt', re.IGNORECASE),
        ]
        
        self.epigraphic_patterns = [
            re.compile(r'hic\s+iacet', re.IGNORECASE),
            re.compile(r'd\.?\s*m\.?\s*s', re.IGNORECASE),
            re.compile(r'dis\s+manibus', re.IGNORECASE),
        ]
        
        # Index page detection patterns - much more comprehensive
        self.index_patterns = [
            re.compile(r'\[\[([^|\]]+/Liber\s+[IVXLCDM]+)\|', re.IGNORECASE),
            re.compile(r'\[\[([^|\]]+/Book\s+[IVXLCDM]+)\|', re.IGNORECASE), 
            re.compile(r'\[\[([^|\]]+/Capitulum\s+[IVXLCDM]+)\|', re.IGNORECASE),
            re.compile(r'\[\[([^|\]]+)\|Liber\s+[IVXLCDM]+', re.IGNORECASE),
            re.compile(r'\[\[([^|\]]+)\|Book\s+[IVXLCDM]+', re.IGNORECASE),
            re.compile(r'\[\[([^|\]]+)\|Chapter\s+[0-9]+', re.IGNORECASE),
            re.compile(r'\[\[([^|\]]+)\|\s*[0-9]+\.', re.IGNORECASE),
        ]
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('vicifons_comprehensive.log'),
                logging.StreamHandler()
            ]
        )
        # Logger already initialized above
        
        # Performance tracking
        self.start_time = None
        self.processed_count = 0
        self.total_estimated = 0
    
    def save_state(self):
        """Save current download state."""
        state = {
            'downloaded_pages': list(self.downloaded_pages),
            'failed_pages': list(self.failed_pages),
            'all_found_pages': list(self.all_found_pages),
            'timestamp': datetime.now().isoformat()
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f)
    
    def load_state(self):
        """Load previous state."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                self.downloaded_pages = set(state.get('downloaded_pages', []))
                self.failed_pages = set(state.get('failed_pages', []))
                self.all_found_pages = set(state.get('all_found_pages', []))
                self.logger.info(f"Resumed: {len(self.downloaded_pages)} downloaded, "
                               f"{len(self.all_found_pages)} total found")
            except Exception as e:
                self.logger.warning(f"Could not load state: {e}")
    
    def quick_exclude_check(self, title: str) -> bool:
        """Fast exclusion check."""
        title_lower = title.lower()
        return any(pattern in title_lower for pattern in self.quick_exclude_patterns)
    
    async def get_page_categories_cached(self, page: pywikibot.Page) -> Set[str]:
        """Get page categories with caching."""
        cache_key = f"page_cats_{page.title().replace('/', '_').replace(':', '_')}"
        cache_file = self.cache_dir / f"{cache_key}.json"
        
        # Check cache
        if self.use_cache and cache_file.exists():
            cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if cache_age < timedelta(hours=self.cache_duration_hours):
                try:
                    with open(cache_file, 'r') as f:
                        return set(json.load(f))
                except:
                    pass
        
        # Fetch categories
        try:
            categories = set()
            for cat in page.categories():
                categories.add(cat.title().lower())
            
            # Cache result
            if self.use_cache:
                with open(cache_file, 'w') as f:
                    json.dump(list(categories), f)
            
            return categories
        except Exception as e:
            self.logger.debug(f"Could not get categories for {page.title()}: {e}")
            return set()
    
    async def get_genre_from_categories(self, page: pywikibot.Page) -> str:
        """Determine genre by checking page categories against categoria:genera."""
        try:
            page_categories = await self.get_page_categories_cached(page)
            
            # Score each genre type based on category matches
            genre_scores = {'poetry': 0, 'prose': 0, 'mixed': 0}
            
            for category in page_categories:
                for genre_type, genre_cats in self.genre_categories.items():
                    if category in genre_cats:
                        genre_scores[genre_type] += 2  # Strong category match
                        self.logger.debug(f"Genre match: {page.title()} -> {category} -> {genre_type}")
                    
                    # Also check for partial matches (subcategories of genera)
                    for genre_cat in genre_cats:
                        if genre_cat.replace('categoria:', '') in category:
                            genre_scores[genre_type] += 1  # Weak match
            
            # Also check if page is in any subcategory of categoria:genera
            if not any(genre_scores.values()):
                genera_categories = await self.get_all_category_pages("Categoria:Genera", max_depth=2)
                genera_titles = {p.title().lower() for p in genera_categories}
                
                if page.title().lower() in genera_titles:
                    # If we find the page in genera but don't know the specific genre,
                    # we'll fall back to other methods
                    self.logger.debug(f"Found {page.title()} in categoria:genera but no specific genre match")
            
            # Return the highest scoring genre
            if genre_scores['poetry'] > genre_scores['prose'] and genre_scores['poetry'] > genre_scores['mixed']:
                return 'poetry'
            elif genre_scores['prose'] > genre_scores['mixed']:
                return 'prose'
            elif genre_scores['mixed'] > 0:
                return 'mixed'
            
            return 'unknown'  # No genre category match found
            
        except Exception as e:
            self.logger.debug(f"Error getting genre from categories for {page.title()}: {e}")
            return 'unknown'
    
    async def get_all_category_pages(self, category_name: str, depth: int = 0, max_depth: int = 3) -> Set[pywikibot.Page]:
        """Comprehensive category traversal with depth control."""
        if depth > max_depth:
            return set()
            
        cache_key = f"{category_name}_depth{depth}"
        cache_file = self.cache_dir / f"{cache_key.replace(':', '_')}.pickle"
        
        # Check cache
        if self.use_cache and cache_file.exists():
            cache_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            if cache_age < timedelta(hours=self.cache_duration_hours):  # Configurable cache duration
                try:
                    with open(cache_file, 'rb') as f:
                        cached_titles = pickle.load(f)
                    pages = set()
                    for title in cached_titles:
                        try:
                            pages.add(pywikibot.Page(self.site, title))
                        except:
                            continue
                    self.logger.info(f"Cached: {len(pages)} pages from {category_name} (depth {depth})")
                    return pages
                except:
                    pass
        
        # Fresh fetch
        self.logger.info(f"Fetching: {category_name} (depth {depth})")
        pages = set()
        
        try:
            category = pywikibot.Category(self.site, category_name)
            
            # Get direct articles
            for page in category.articles():
                if page.namespace() == 0:  # Main namespace
                    pages.add(page)
            
            # Get subcategories recursively
            subcats_processed = 0
            for subcat in category.subcategories():
                subcats_processed += 1
                subcat_pages = await self.get_all_category_pages(subcat.title(), depth + 1, max_depth)
                pages.update(subcat_pages)
                
                # Progress feedback for large categories
                if subcats_processed % 10 == 0:
                    self.logger.info(f"Processed {subcats_processed} subcategories in {category_name}")
            
            # Cache results
            if self.use_cache:
                titles = [p.title() for p in pages]
                with open(cache_file, 'wb') as f:
                    pickle.dump(titles, f)
            
            self.logger.info(f"Found {len(pages)} total pages in {category_name} (depth {depth})")
            return pages
            
        except Exception as e:
            self.logger.error(f"Error fetching {category_name}: {e}")
            return set()
    
    async def get_author_works(self, author_category: str) -> Set[pywikibot.Page]:
        """Get works by specific authors from Scriptor namespace and categories."""
        pages = set()
        
        try:
            # Try author category
            author_pages = await self.get_all_category_pages(author_category)
            pages.update(author_pages)
            
            # Also try to find author page directly in Scriptor namespace
            author_name = author_category.replace('Categoria:', '').replace('categoria:', '')
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
                                    pages.add(work_page)
                            except:
                                continue
                    
                    self.logger.info(f"Found {len(work_links)} works from {scriptor_page_title}")
            except Exception as e:
                self.logger.debug(f"No scriptor page for {author_name}: {e}")
        
        except Exception as e:
            self.logger.error(f"Error getting author works for {author_category}: {e}")
        
        return pages
    
    def is_index_page(self, page_text: str) -> bool:
        """Enhanced index page detection with looser criteria."""
        # Count links that look like chapters/books
        chapter_links = 0
        
        # Enhanced patterns based on debug findings
        enhanced_patterns = [
            r'\[\[([^|\]]+)\|[^|\]]*(?:Liber|Book|Chapter|Capitulum)\s+[IVXLCDM0-9]+',  # [[Work/Book|Liber I]]
            r'\[\[([^|\]]+/(?:Liber|Book|Chapter|Capitulum)\s+[IVXLCDM0-9]+)',  # [[Work/Liber I]]
            r'\[\[([^|\]]+)\|[IVXLCDM]+\]',  # [[Link|I]]
            r'^\*\s*\[\[([^|\]]+)', # List format
        ]
        
        for pattern in enhanced_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE | re.MULTILINE)
            chapter_links += len(matches)
        
        # Check for common index indicators
        index_indicators = [
            r'==\s*Liber\s+[IVXLCDM]+',
            r'==\s*Book\s+[IVXLCDM]+', 
            r'==\s*Chapter\s+[0-9]+',
            r'INDEX.*Liber',  # Noctes Atticae style
            r'thumb.*center',  # Images suggesting presentation page
        ]
        
        indicator_count = sum(1 for pattern in index_indicators 
                            if re.search(pattern, page_text, re.IGNORECASE))
        
        # Calculate text vs links ratio
        clean_text = re.sub(r'\[\[[^\]]+\]\]', '', page_text)
        clean_text = re.sub(r'\{\{[^}]+\}\}', '', clean_text)
        clean_text = re.sub(r'<[^>]+>', '', clean_text)
        
        word_count = len(re.findall(r'\w+', clean_text))
        
        # More lenient decision logic based on debug findings
        is_index = (
            (chapter_links >= 3 and word_count < chapter_links * 20) or  # More lenient ratio
            (chapter_links >= 5) or  # 5+ chapter links = definitely index
            (indicator_count >= 1 and chapter_links >= 3) or  # Any indicator + 3 chapters
            (chapter_links >= 8 and word_count < 500)  # Large chapter count with short text
        )
        
        if is_index:
            self.logger.info(f"📋 Index detected: {chapter_links} chapter links, {indicator_count} indicators, {word_count} words")
        else:
            # Log near misses for debugging
            if chapter_links >= 3:
                self.logger.debug(f"Near-miss index: {chapter_links} chapter links, {word_count} words (ratio: {word_count/(chapter_links or 1):.1f})")
        
        return is_index
    
    def extract_chapter_links(self, page_text: str) -> List[str]:
        """Extract chapter/book links using patterns that actually work."""
        links = []
        
        # Use the EXACT patterns that worked in debug test
        working_patterns = [
            # This is the one that found all the Aeneis/Noctes Atticae chapters
            r'\[\[([^|\]]+)\|[^|\]]*(?:Liber|Book|Chapter|Capitulum)\s+[IVXLCDM0-9]+',
            
            # Additional backup patterns
            r'\[\[([^|\]]+/(?:Liber|Book|Chapter|Capitulum)\s+[IVXLCDM0-9]+[^|\]]*)\]',
            r'\[\[([^|\]]+/(?:Liber|Book|Chapter|Capitulum)\s+[IVXLCDM0-9]+)',
            r'^\*\s*\[\[([^|\]]+)\]',
            r'^\*\s*\[\[([^|\]]+)\|[^\]]+\]',
        ]
        
        # Apply working patterns
        for pattern_str in working_patterns:
            pattern = re.compile(pattern_str, re.IGNORECASE | re.MULTILINE)
            matches = pattern.findall(page_text)
            links.extend(matches)
            
        # Clean and deduplicate links
        unique_links = []
        seen = set()
        
        for link in links:
            if not link or link in seen:
                continue
                
            # Skip non-content links
            if any(prefix in link.lower() for prefix in [
                'category:', 'categoria:', 'file:', 'fasciculus:', 'imago:', 
                'image:', 'template:', 'formula:', 'help:', 'auxilium:',
                'fr:', 'en:', 'de:'  # Language links
            ]):
                continue
            
            # Skip very short or invalid links
            if len(link.strip()) < 3:
                continue
                
            link = link.strip()
            unique_links.append(link)
            seen.add(link)
        
        return unique_links
    
    async def find_chapters_via_category(self, work_title: str) -> List[str]:
        """Find chapters for a work by searching categoria:capita ex operibus."""
        chapters = []
        
        try:
            # Search for chapters in the capita ex operibus category
            capita_category = "Categoria:Capita ex operibus"
            capita_pages = await self.get_all_category_pages(capita_category, max_depth=2)
            
            # Filter for chapters that belong to this work
            work_chapters = []
            work_title_clean = work_title.lower().strip()
            
            for page in capita_pages:
                page_title = page.title().lower()
                
                # Check if this chapter belongs to our work
                # Look for work title in the chapter title
                if any(keyword in page_title for keyword in [work_title_clean, work_title_clean.replace(' ', '')]):
                    work_chapters.append(page.title())
                
                # Also check for common variations
                if work_title_clean == "ab urbe condita" and "livius" in page_title:
                    work_chapters.append(page.title())
                elif work_title_clean == "noctes atticae" and "gellius" in page_title:
                    work_chapters.append(page.title())
                elif work_title_clean.startswith("bellum") and "caesar" in page_title:
                    work_chapters.append(page.title())
            
            if work_chapters:
                self.logger.info(f"Found {len(work_chapters)} chapters for '{work_title}' via categoria:capita ex operibus")
                return work_chapters
                
        except Exception as e:
            self.logger.debug(f"Could not search capita ex operibus for {work_title}: {e}")
        
        return []
    
    async def find_chapters_comprehensive(self, page: pywikibot.Page, page_text: str) -> List[str]:
        """Comprehensive chapter finding using multiple methods."""
        all_chapters = []
        
        # Method 1: Extract from page text
        text_chapters = self.extract_chapter_links(page_text)
        all_chapters.extend(text_chapters)
        
        # Method 2: Search categoria:capita ex operibus
        category_chapters = await self.find_chapters_via_category(page.title())
        all_chapters.extend(category_chapters)
        
        # Method 3: Try common patterns based on work title (using actual page names)
        title_lower = page.title().lower()
        
        if "aeneis" in title_lower:
            # Vergil's Aeneid - confirmed working
            aeneis_patterns = [
                f"Aeneis/Liber {i}" for i in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII']
            ]
            all_chapters.extend(aeneis_patterns)
            
        elif "noctes atticae" in title_lower or ("noctes" in title_lower and "attic" in title_lower):
            # Gellius - confirmed working
            gellius_patterns = [
                f"Noctes Atticae/Liber {i}" for i in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX']
            ]
            all_chapters.extend(gellius_patterns)
            
        elif "commentarii de bello gallico" in title_lower:
            # Caesar's Gallic Wars - confirmed working  
            caesar_bg_patterns = [
                f"Commentarii de bello Gallico/Liber {i}" for i in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII']
            ]
            all_chapters.extend(caesar_bg_patterns)
            
        elif "commentarii de bello civili" in title_lower:
            # Caesar's Civil War
            caesar_bc_patterns = [
                f"Commentarii de bello civili/Liber {i}" for i in ['I', 'II', 'III']
            ]
            all_chapters.extend(caesar_bc_patterns)
            
        # Add more patterns for works we know exist but might not be detected
        elif "metamorphoses" in title_lower and "ovidius" in title_lower:
            # Ovid's Metamorphoses
            ovid_patterns = [
                f"Metamorphoses (Ovidius)/Liber {i}" for i in ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII', 'XIV', 'XV']
            ]
            all_chapters.extend(ovid_patterns)
        
        # Remove duplicates and validate
        unique_chapters = []
        seen = set()
        
        for chapter in all_chapters:
            if chapter and chapter not in seen:
                unique_chapters.append(chapter)
                seen.add(chapter)
        
        return unique_chapters
    
    async def enhanced_content_analysis(self, page: pywikibot.Page, text: str) -> Tuple[bool, bool, str, str]:
        """Enhanced content analysis with genre-based classification."""
        title_lower = page.title().lower()
        text_lower = text.lower()
        
        # Quick fragmentary check
        fragment_count = sum(1 for pattern in self.fragmentary_patterns 
                           if pattern.search(text_lower))
        if fragment_count >= 2:
            return False, True, "unknown", "multiple_fragment_indicators"
        
        # Quick epigraphic check
        if any(pattern.search(text_lower) for pattern in self.epigraphic_patterns):
            return False, False, "unknown", "epigraphic_content"
        
        # Quality check
        word_count = len(re.findall(r'\w+', text))
        if word_count < 50:  # Very short threshold for chapters
            return False, False, "unknown", f"too_short_{word_count}_words"
        
        # Enhanced genre detection using categories first
        category_genre = await self.get_genre_from_categories(page)
        
        if category_genre != 'unknown':
            # High confidence from category classification
            return True, False, category_genre, f"category_classified_{word_count}_words"
        
        # Fallback to title and content analysis
        poetry_score = prose_score = mixed_score = 0
        
        # Enhanced title-based scoring
        poetry_titles = [
            'carmen', 'carmina', 'elegia', 'elegiae', 'versus', 'aeneis', 
            'metamorphoses', 'ecloga', 'eclogae', 'georgica', 'bucolica',
            'satirae', 'satira', 'hymnus', 'hymni', 'odes', 'ode'
        ]
        prose_titles = [
            'historia', 'historiae', 'oratio', 'orationes', 'epistola', 'epistolae',
            'commentarii', 'annales', 'bellum', 'bella', 'de ', 'ad ', 'vita', 'vitae',
            'dialogus', 'tractatus', 'institutio', 'naturalis historia'
        ]
        mixed_titles = ['comoedia', 'tragoedia', 'fabula', 'drama']
        
        for p_title in poetry_titles:
            if p_title in title_lower:
                poetry_score += 3  # Increased weight
                
        for p_title in prose_titles:
            if p_title in title_lower:
                prose_score += 3
                
        for m_title in mixed_titles:
            if m_title in title_lower:
                mixed_score += 3
        
        # Author-based genre hints (some authors are known for specific genres)
        author_genre_hints = {
            'vergilius': 'poetry', 'ovidius': 'poetry', 'horatius': 'poetry',
            'catullus': 'poetry', 'propertius': 'poetry', 'tibullus': 'poetry',
            'cicero': 'prose', 'caesar': 'prose', 'livius': 'prose', 'tacitus': 'prose',
            'plinius': 'prose', 'quintilianus': 'prose', 'seneca': 'mixed'
        }
        
        for author, genre in author_genre_hints.items():
            if author in title_lower:
                if genre == 'poetry':
                    poetry_score += 2
                elif genre == 'prose':
                    prose_score += 2
                else:
                    mixed_score += 2
        
        # Enhanced content analysis (first 100 lines for better accuracy)
        lines = [line.strip() for line in text.split('\n')[:100] if line.strip()]
        if lines:
            # Verse characteristics
            short_lines = sum(1 for line in lines if 20 <= len(line) <= 80)
            very_short_lines = sum(1 for line in lines if 10 <= len(line) < 30)  # Typical verse
            long_lines = sum(1 for line in lines if len(line) > 100)  # Typical prose
            
            # Line ending patterns (poetry often doesn't end with periods)
            non_period_endings = sum(1 for line in lines if line and not line.endswith('.'))
            period_endings = sum(1 for line in lines if line.endswith('.'))
            
            # Poetry indicators
            if very_short_lines > len(lines) * 0.3:  # 30%+ very short lines
                poetry_score += 2
            if non_period_endings > period_endings * 2:  # Many non-period endings
                poetry_score += 1
            if short_lines > long_lines * 2:  # Many medium-length lines
                poetry_score += 1
                
            # Prose indicators  
            if long_lines > len(lines) * 0.2:  # 20%+ long lines
                prose_score += 2
            if period_endings > non_period_endings:  # More sentences
                prose_score += 1
                
            # Check for prose connectors
            prose_connectors = ['itaque', 'igitur', 'ergo', 'autem', 'enim', 'nam', 'sed', 'at']
            connector_count = sum(text_lower.count(conn) for conn in prose_connectors)
            if connector_count > word_count // 100:  # Relative to text length
                prose_score += 1
        
        # Determine final classification
        max_score = max(poetry_score, prose_score, mixed_score)
        
        if max_score == 0:
            text_type = "mixed"  # Default when uncertain
        elif poetry_score == max_score:
            text_type = "poetry"
        elif prose_score == max_score:
            text_type = "prose"
        else:
            text_type = "mixed"
        
        confidence = "high" if max_score >= 4 else "medium" if max_score >= 2 else "low"
        
        return True, False, text_type, f"{confidence}_confidence_{word_count}_words"
    
    def fast_content_analysis(self, title: str, text: str) -> Tuple[bool, bool, str, str]:
        """Legacy fast content analysis for backward compatibility."""
        # Create a dummy page object for the enhanced analysis
        try:
            dummy_page = pywikibot.Page(self.site, title)
            # This is a synchronous wrapper - in practice, we should use enhanced_content_analysis
            return asyncio.run(self.enhanced_content_analysis(dummy_page, text))
        except:
            # Fallback to simple analysis if async fails
            word_count = len(re.findall(r'\w+', text))
            if word_count < 50:
                return False, False, "unknown", f"too_short_{word_count}_words"
            return True, False, "mixed", f"fallback_{word_count}_words"
    
    async def download_text_concurrent(self, session: aiohttp.ClientSession, 
                                     page: pywikibot.Page) -> Optional[str]:
        """Download page text concurrently."""
        try:
            # Try export API first
            export_url = "https://ws-export.wmcloud.org/tool/book.php"
            params = {'lang': 'la', 'page': page.title(), 'format': 'txt'}
            
            async with session.get(export_url, params=params, timeout=30) as response:
                if response.status == 200:
                    text = await response.text()
                    if text.strip() and len(text.strip()) > 50:
                        # Clean export metadata
                        lines = text.split('\n')
                        clean_lines = [line for line in lines 
                                     if not any(marker in line.lower() 
                                              for marker in ['exported by', 'generated by', 'wikisource export'])]
                        cleaned_text = '\n'.join(clean_lines).strip()
                        if len(cleaned_text) > 50:
                            return cleaned_text
            
            # Fallback to pywikibot
            return self.fallback_text_extraction(page)
            
        except Exception as e:
            self.logger.debug(f"Download failed for {page.title()}: {e}")
            return None
    
    def fallback_text_extraction(self, page: pywikibot.Page) -> Optional[str]:
        """Fallback text extraction."""
        try:
            page_text = page.text
            if len(page_text.strip()) < 50:
                return None
            
            # Quick cleanup
            text = re.sub(r'\{\{[^{}]*\}\}', '', page_text)
            text = re.sub(r'<[^>]+>', '', text)
            text = re.sub(r'\[\[Category:[^\]]+\]\]', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\[\[[a-z-]+:[^\]]+\]\]', '', text)
            text = re.sub(r'\[\[([^\]|]+)\|([^\]]+)\]\]', r'\2', text)
            text = re.sub(r'\[\[([^\]]+)\]\]', r'\1', text)
            text = re.sub(r'\n\s*\n+', '\n\n', text)
            text = re.sub(r'[ \t]+', ' ', text)
            
            return text.strip() if len(text.strip()) > 50 else None
            
        except Exception as e:
            self.logger.error(f"Fallback extraction failed for {page.title()}: {e}")
            return None
    
    async def download_single_page(self, page_title: str) -> Optional[Dict]:
        """Download a single page for testing purposes."""
        try:
            page = pywikibot.Page(self.site, page_title)
            if not page.exists():
                return {"success": False, "error": "Page does not exist"}
            
            # Use aiohttp session for download
            async with aiohttp.ClientSession() as session:
                text = await self.download_text_concurrent(session, page)
                
                if text:
                    # Save to output directory
                    safe_title = self.clean_filename(page.title())
                    file_path = self.output_dir / f"{safe_title}.txt"
                    
                    async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                        await f.write(text)
                    
                    return {
                        "success": True, 
                        "title": page.title(),
                        "file_path": str(file_path),
                        "text_length": len(text)
                    }
                else:
                    return {"success": False, "error": "Failed to extract text"}
                    
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    async def save_text_async(self, page: pywikibot.Page, text: str, 
                            category_dir: str, text_type: str) -> bool:
        """Save text file asynchronously."""
        try:
            filename = self.clean_filename(page.title())
            
            if self.single_folder:
                filepath = self.output_dir / filename
            else:
                filepath = self.output_dir / category_dir / filename
            
            content = (f"Title: {page.title()}\n"
                      f"Source: {page.full_url()}\n"
                      f"Category: {category_dir}\n"
                      f"Text Type: {text_type}\n"
                      f"{'-' * 50}\n\n{text}")
            
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(content)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving {page.title()}: {e}")
            return False
    
    def clean_filename(self, title: str) -> str:
        """Clean filename."""
        if ':' in title:
            title = title.split(':', 1)[1]
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            title = title.replace(char, '_')
        return f"{title[:200]}.txt"
    
    async def process_single_page(self, session: aiohttp.ClientSession, 
                                page: pywikibot.Page, category_dir: str) -> Dict[str, int]:
        """Process a single page."""
        try:
            # Download text
            text = await self.download_text_concurrent(session, page)
            if not text:
                return {'failed': 1}
            
            # Check if this is an index page
            if self.is_index_page(text):
                self.logger.info(f"🔍 Processing index page: {page.title()}")
                
                # Use comprehensive chapter finding
                chapter_links = await self.find_chapters_comprehensive(page, text)
                
                if chapter_links:
                    self.logger.info(f"📚 Found {len(chapter_links)} potential chapters for {page.title()}")
                    chapters_processed = 0
                    chapters_found = 0
                    
                    for chapter_title in chapter_links:
                        try:
                            chapter_page = pywikibot.Page(self.site, chapter_title)
                            
                            # Check if page exists
                            if not chapter_page.exists():
                                self.logger.debug(f"Chapter does not exist: {chapter_title}")
                                continue
                            
                            # Skip if already processed
                            if chapter_page.title() in self.downloaded_pages:
                                continue
                            
                            chapters_found += 1
                            self.logger.info(f"📖 Processing chapter: {chapter_title}")
                            
                            chapter_text = await self.download_text_concurrent(session, chapter_page)
                            if not chapter_text:
                                self.logger.debug(f"No text retrieved for: {chapter_title}")
                                continue
                            
                            # Enhanced analysis with genre detection
                            is_valid, is_frag, text_type, reason = await self.enhanced_content_analysis(
                                chapter_page, chapter_text)
                            
                            if is_valid:
                                success = await self.save_text_async(
                                    chapter_page, chapter_text, category_dir, text_type)
                                if success:
                                    self.downloaded_pages.add(chapter_page.title())
                                    chapters_processed += 1
                                    self.processed_count += 1
                                    self.logger.info(f"✅ Saved chapter: {chapter_title} ({text_type}, {reason})")
                                    
                                    if self.processed_count % 10 == 0:
                                        self.log_progress()
                            else:
                                self.logger.debug(f"Chapter filtered out: {chapter_title} - {reason}")
                        
                        except Exception as e:
                            self.logger.debug(f"Error processing chapter {chapter_title}: {e}")
                    
                    self.logger.info(f"🎉 Index processing complete for {page.title()}: {chapters_processed}/{chapters_found} chapters saved")
                    
                    if chapters_processed > 0:
                        return {'processed': chapters_processed, 'mixed': chapters_processed}
                
                # If no chapters found or processed, log it but don't save the index page itself
                self.logger.warning(f"⚠️ No valid chapters found for index page: {page.title()}")
                return {'skipped_quality': 1}  # Don't save empty index pages
            
            # Regular page processing with enhanced analysis
            is_valid, is_fragmentary, text_type, reason = await self.enhanced_content_analysis(page, text)
            
            if not is_valid:
                if is_fragmentary:
                    return {'skipped_fragmentary': 1}
                elif 'epigraphic' in reason:
                    return {'skipped_epigraphic': 1}
                else:
                    return {'skipped_quality': 1}
            
            # Save file
            success = await self.save_text_async(page, text, category_dir, text_type)
            if success:
                self.downloaded_pages.add(page.title())
                self.processed_count += 1
                
                if self.processed_count % 10 == 0:
                    self.log_progress()
                
                return {'processed': 1, text_type: 1}
            else:
                return {'failed': 1}
                
        except Exception as e:
            self.logger.error(f"Error processing {page.title()}: {e}")
            return {'failed': 1}
    
    def log_progress(self):
        """Log progress with ETA."""
        if self.start_time and self.total_estimated > 0:
            elapsed = time.time() - self.start_time
            rate = self.processed_count / (elapsed / 60) if elapsed > 0 else 0
            remaining = max(0, self.total_estimated - self.processed_count)
            eta_minutes = remaining / rate if rate > 0 else 0
            eta = str(timedelta(minutes=int(eta_minutes)))
            
            self.logger.info(f"Progress: {self.processed_count}/{self.total_estimated} "
                           f"({rate:.1f}/min) ETA: {eta}")
    
    async def process_category_comprehensive(self, category_name: str, category_dir: str):
        """Process category with comprehensive search including authors."""
        self.logger.info(f"=== Processing: {category_name} ===")
        
        # Get all pages from main category and subcategories
        all_pages = await self.get_all_category_pages(category_name, max_depth=4)
        
        # Also search for author-specific categories within this period
        if "Romana" in category_name:
            # Major Roman authors
            roman_authors = [
                "Categoria:Titus Livius", "Categoria:Gaius Iulius Caesar", 
                "Categoria:Marcus Tullius Cicero", "Categoria:Publius Vergilius Maro",
                "Categoria:Quintus Horatius Flaccus", "Categoria:Publius Ovidius Naso",
                "Categoria:Gaius Suetonius Tranquillus", "Categoria:Cornelius Tacitus",
                "Categoria:Aulus Gellius", "Categoria:Lucius Annaeus Seneca"
            ]
            
            for author_cat in roman_authors:
                try:
                    author_pages = await self.get_author_works(author_cat)
                    all_pages.update(author_pages)
                    self.logger.info(f"Added {len(author_pages)} pages from {author_cat}")
                except Exception as e:
                    self.logger.debug(f"Could not process {author_cat}: {e}")
        
        elif "Mediaevalis" in category_name:
            # Medieval authors
            medieval_authors = [
                "Categoria:Thomas Aquinas", "Categoria:Aurelius Augustinus",
                "Categoria:Boethius", "Categoria:Isidorus Hispalensis"
            ]
            
            for author_cat in medieval_authors:
                try:
                    author_pages = await self.get_author_works(author_cat)
                    all_pages.update(author_pages)
                    self.logger.info(f"Added {len(author_pages)} pages from {author_cat}")
                except Exception as e:
                    self.logger.debug(f"Could not process {author_cat}: {e}")
        
        # Filter out already processed and excluded pages
        unprocessed_pages = [p for p in all_pages 
                           if p.title() not in self.downloaded_pages 
                           and not self.quick_exclude_check(p.title())]
        
        self.logger.info(f"Found {len(all_pages)} total pages, {len(unprocessed_pages)} to process")
        self.total_estimated += len(unprocessed_pages)
        self.all_found_pages.update(p.title() for p in all_pages)
        
        if not unprocessed_pages:
            return
        
        # Process in concurrent batches
        batch_size = self.max_concurrent
        connector = aiohttp.TCPConnector(limit=self.max_concurrent, limit_per_host=5)
        timeout = aiohttp.ClientTimeout(total=60)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            total_stats = {
                'processed': 0, 'skipped_fragmentary': 0, 'skipped_quality': 0,
                'skipped_epigraphic': 0, 'failed': 0, 'poetry': 0, 'prose': 0, 'mixed': 0
            }
            
            for i in range(0, len(unprocessed_pages), batch_size):
                batch = unprocessed_pages[i:i + batch_size]
                self.logger.info(f"Processing batch {i//batch_size + 1}/{(len(unprocessed_pages)-1)//batch_size + 1} "
                               f"({len(batch)} pages)")
                
                # Process batch
                tasks = [self.process_single_page(session, page, category_dir) for page in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Collect statistics
                for result in results:
                    if isinstance(result, dict):
                        for key, value in result.items():
                            total_stats[key] = total_stats.get(key, 0) + value
                
                # Save state periodically
                if i % (batch_size * 5) == 0:
                    self.save_state()
                
                # Brief pause
                await asyncio.sleep(0.5)
            
            # Log final statistics for this category
            self.logger.info(f"=== {category_dir} Complete ===")
            for key, value in total_stats.items():
                if value > 0:
                    self.logger.info(f"  {key.replace('_', ' ').title()}: {value}")
    
    async def run_comprehensive(self):
        """Main comprehensive execution method."""
        self.logger.info("Starting comprehensive Vicifons download...")
        self.start_time = time.time()
        
        try:
            # Process all main categories comprehensively
            await self.process_category_comprehensive("Categoria:Latinitas Romana", "Latinitas_Romana")
            await self.process_category_comprehensive("Categoria:Latinitas Mediaevalis", "Latinitas_Mediaevalis")
            
            # 15th century - try multiple category names
            century_15_categories = [
                "Categoria:Saeculum quintum decimum",
                "Categoria:Saeculum XV", 
                "Saeculum quintum decimum",
                "Saeculum XV"
            ]
            
            for cat_name in century_15_categories:
                try:
                    pages = await self.get_all_category_pages(cat_name)
                    if pages:
                        self.logger.info(f"Processing 15th century from {cat_name}")
                        # Create temporary category structure for processing
                        unprocessed = [p for p in pages if p.title() not in self.downloaded_pages]
                        
                        if unprocessed:
                            # Process the pages we found
                            connector = aiohttp.TCPConnector(limit=self.max_concurrent, limit_per_host=5)
                            timeout = aiohttp.ClientTimeout(total=60)
                            
                            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                                batch_size = self.max_concurrent
                                for i in range(0, len(unprocessed), batch_size):
                                    batch = unprocessed[i:i + batch_size]
                                    tasks = [self.process_single_page(session, page, "Saeculum_XV") 
                                           for page in batch]
                                    await asyncio.gather(*tasks, return_exceptions=True)
                                    await asyncio.sleep(0.5)
                        break
                except Exception as e:
                    self.logger.debug(f"Could not process {cat_name}: {e}")
            
            # Final statistics
            elapsed = time.time() - self.start_time
            self.logger.info("=" * 60)
            self.logger.info("COMPREHENSIVE DOWNLOAD COMPLETE!")
            self.logger.info(f"Total pages discovered: {len(self.all_found_pages)}")
            self.logger.info(f"Total pages processed: {len(self.downloaded_pages)}")
            self.logger.info(f"Total time: {timedelta(seconds=int(elapsed))}")
            if elapsed > 60:
                self.logger.info(f"Average rate: {len(self.downloaded_pages) / (elapsed/60):.1f} pages/min")
            self.logger.info("=" * 60)
            
            # Final state save
            self.save_state()
            
        except Exception as e:
            self.logger.error(f"Fatal error: {e}")


def main():
    """Main entry point."""
    print("Vicifons Comprehensive Latin Text Scraper")
    print("=" * 60)
    print("Features:")
    print("- Comprehensive category and author traversal")
    print("- Proper index page detection and chapter extraction")
    print("- Finds major classical works (Caesar, Livy, Gellius, etc.)")
    print("- Concurrent downloads with caching and resume")
    print("- Deep subcategory search with author-specific discovery")
    print()
    
    output_dir = input("Enter output directory (default: downloaded_texts): ").strip()
    if not output_dir:
        output_dir = "downloaded_texts"
    
    single_folder = input("Export all files to single folder? (y/N): ").strip().lower() == 'y'
    
    max_concurrent_input = input("Max concurrent downloads (default: 10): ").strip()
    max_concurrent = int(max_concurrent_input) if max_concurrent_input.isdigit() else 10
    
    use_cache = input("Use caching? (Y/n): ").strip().lower() != 'n'
    
    downloader = ComprehensiveVicifonsDownloader(
        output_dir=output_dir,
        single_folder=single_folder, 
        max_concurrent=max_concurrent,
        use_cache=use_cache
    )
    
    try:
        asyncio.run(downloader.run_comprehensive())
    except KeyboardInterrupt:
        print("\nDownload interrupted by user.")
        downloader.save_state()
        print("Progress saved. You can resume later.")
    except Exception as e:
        print(f"Error: {e}")
        downloader.save_state()


if __name__ == "__main__":
    main()