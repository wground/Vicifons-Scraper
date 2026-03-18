#!/usr/bin/env python3
"""
Comprehensive Latin Text Scraper

This module provides a comprehensive approach to scraping ALL Latin texts from 
Vicifons by parsing the XML dump rather than relying only on categories.
This ensures we capture the maximum amount of Latin content for LLM training.
"""

import xml.etree.ElementTree as ET
import re
import logging
from typing import List, Dict, Set
from pathlib import Path


class ComprehensiveLatinExtractor:
    """Extract all Latin content from Vicifons XML dump."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Patterns to identify non-content pages
        self.skip_patterns = [
            # Administrative pages
            'talk:', 'user:', 'disputatio', 'usor:', 'category:', 'categoria:',
            'template:', 'formula:', 'help:', 'auxilium:', 'file:', 'fasciculus:',
            'mediawiki:', 'special:', 'project:', 'vicifons:',
            
            # Technical files
            '.css', '.js', '.json', '/common.css', '/common.js',
            
            # Discussion and meta pages
            'discussion', 'talk page', 'user page', 'project page',
        ]
        
        # Patterns to identify index/navigation pages (to exclude from content scraping)
        self.index_indicators = [
            'index', 'contents', 'table of contents', 'navigation',
            'disambiguation', 'redirect', 'stub', 'category',
            'see also', 'main article', 'list of',
        ]
    
    def extract_all_latin_titles(self, xml_file_path: str) -> List[Dict]:
        """
        Extract all Latin content titles from the XML dump.
        
        Returns list of dicts with title, estimated content type, etc.
        """
        self.logger.info(f"Extracting Latin content from XML dump: {xml_file_path}")
        
        latin_works = []
        processed_count = 0
        
        try:
            # Use iterparse for memory efficiency with large XML files
            context = ET.iterparse(xml_file_path, events=('start', 'end'))
            context = iter(context)
            event, root = next(context)
            
            for event, elem in context:
                if event == 'end' and elem.tag.endswith('}page'):
                    processed_count += 1
                    
                    # Extract page data
                    ns_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.11/}ns')
                    title_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.11/}title')
                    text_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.11/}text')
                    
                    if ns_elem is not None and title_elem is not None and ns_elem.text == '0':
                        title = title_elem.text
                        
                        if self._is_valid_latin_content(title, text_elem):
                            work_data = self._create_work_data(title, text_elem)
                            latin_works.append(work_data)
                    
                    # Clear element to save memory
                    elem.clear()
                    
                    # Progress logging
                    if processed_count % 5000 == 0:
                        self.logger.info(f"Processed {processed_count} pages, found {len(latin_works)} Latin works")
            
            root.clear()
            
        except Exception as e:
            self.logger.error(f"Error parsing XML dump: {e}")
            raise
        
        self.logger.info(f"Extraction complete: {len(latin_works)} Latin works from {processed_count} pages")
        return latin_works
    
    def _is_valid_latin_content(self, title: str, text_elem) -> bool:
        """Check if a page represents valid Latin content."""
        if not title or not text_elem:
            return False
        
        # Skip administrative and technical pages
        title_lower = title.lower()
        if any(pattern in title_lower for pattern in self.skip_patterns):
            return False
        
        # Check text content
        text_content = text_elem.text or ''
        
        # Skip redirects
        if (text_content.strip().startswith('#REDIRECT') or 
            text_content.strip().startswith('#redirect')):
            return False
        
        # Must have substantial content (reduced threshold)
        if len(text_content.strip()) < 50:
            return False
        
        # For now, let's be very permissive to maximize Latin content
        # Only skip obvious index pages with extreme link-to-content ratios
        if self._appears_to_be_obvious_index_page(text_content):
            return False
        
        return True
    
    def _appears_to_be_obvious_index_page(self, text_content: str) -> bool:
        """Check if content appears to be an index/navigation page."""
        text_lower = text_content.lower()
        
        # Count wiki links vs actual text
        link_count = len(re.findall(r'\[\[[^\]]+\]\]', text_content))
        
        # Remove all markup to get clean text
        clean_text = re.sub(r'\[\[[^\]]+\]\]', '', text_content)
        clean_text = re.sub(r'\{\{[^}]+\}\}', '', clean_text)
        clean_text = re.sub(r'<[^>]+>', '', clean_text)
        clean_text = re.sub(r'[#*:]+', '', clean_text)
        
        word_count = len(re.findall(r'\w+', clean_text))
        
        # If too many links relative to content, likely an index (made less strict)
        if word_count > 0 and link_count > 10 and link_count / word_count > 1.0:
            return True
        
        # Check for index-specific phrases
        index_phrases = [
            'see also', 'main article', 'disambiguation', 'redirect',
            'list of', 'category:', 'index', 'contents', 'navigation'
        ]
        
        if any(phrase in text_lower for phrase in index_phrases):
            return True
        
        return False
    
    def _is_low_quality_content(self, text_content: str) -> bool:
        """Filter out low-quality content (made less strict)."""
        # Content that's mostly markup (very high threshold)
        markup_chars = len(re.findall(r'[\[\]{}|<>=#*]', text_content))
        total_chars = len(text_content)
        
        if total_chars > 0 and markup_chars / total_chars > 0.8:
            return True
        
        # Check for excessive English content (should be Latin, but allow some)
        english_words = [
            'the', 'and', 'this', 'that', 'with', 'from', 'they', 'have', 'been',
            'page', 'article', 'category', 'see also', 'main article', 'english'
        ]
        
        text_lower = text_content.lower()
        english_count = sum(1 for word in english_words if word in text_lower)
        
        if english_count >= 5:  # Allow some English metadata but not too much
            return True
        
        return False
    
    def _create_work_data(self, title: str, text_elem) -> Dict:
        """Create work data dictionary from page information."""
        text_content = text_elem.text or ''
        
        return {
            'title': title,
            'author': self._extract_author_from_title(title),
            'work_type': self._classify_work_type(title, text_content),
            'estimated_period': self._estimate_period(title),
            'content_length': len(text_content.strip()),
            'source': 'xml_dump_comprehensive',
            'priority': self._assign_priority(title, text_content)
        }
    
    def _extract_author_from_title(self, title: str) -> str:
        """Extract author name from title using enhanced patterns."""
        # Check for parenthetical author indication
        if '(' in title and ')' in title:
            match = re.search(r'\(([^)]+)\)$', title)
            if match:
                return match.group(1).strip()
        
        # Check for slash-separated format (Work/Author or Author/Work)
        if '/' in title:
            parts = title.split('/')
            # First part is often the main work or author
            return parts[0].strip()
        
        # Known author patterns
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
            'aquinas': 'Thomas Aquinas',
            'boethius': 'Anicius Manlius Severinus Boethius',
            'plautus': 'Titus Maccius Plautus',
            'terentius': 'Publius Terentius Afer',
            'lucretius': 'Titus Lucretius Carus',
            'catullus': 'Gaius Valerius Catullus',
            'propertius': 'Sextus Propertius',
            'tibullus': 'Albius Tibullus',
            'juvenalis': 'Decimus Iunius Iuvenalis',
            'martialis': 'Marcus Valerius Martialis',
            'plinius': 'Gaius Plinius Secundus',
            'suetonius': 'Gaius Suetonius Tranquillus',
            'ammianus': 'Ammianus Marcellinus',
            'hieronymus': 'Sophronius Eusebius Hieronymus',
            'gregorius': 'Gregorius Magnus',
            'isidorus': 'Isidorus Hispalensis',
            'beda': 'Beda Venerabilis',
            'alcuinus': 'Alcuinus',
            'bernardus': 'Bernardus Claraevallensis'
        }
        
        title_lower = title.lower()
        for pattern, author in author_patterns.items():
            if pattern in title_lower:
                return author
        
        return "Unknown"
    
    def _classify_work_type(self, title: str, text_content: str) -> str:
        """Classify the type of Latin work."""
        title_lower = title.lower()
        content_lower = text_content.lower()
        
        # Epic poetry
        if any(term in title_lower for term in ['aeneis', 'metamorphoses', 'thebais']):
            return 'epic_poetry'
        
        # Historical works
        if any(term in title_lower for term in ['commentarii', 'historia', 'annales', 'bellum']):
            return 'historical_prose'
        
        # Philosophical works
        if any(term in title_lower for term in ['de ', 'summa', 'confessiones', 'consolatione']):
            return 'philosophical_prose'
        
        # Religious/theological works
        if any(term in title_lower for term in ['patrologia', 'theologia', 'liber', 'epistula']):
            return 'theological_prose'
        
        # Rhetorical works
        if any(term in title_lower for term in ['orationes', 'brutus', 'de oratore']):
            return 'rhetorical_prose'
        
        # Poetry (general)
        if any(term in title_lower for term in ['carmen', 'versus', 'ode', 'ecloga']):
            return 'poetry'
        
        # Legal/administrative
        if any(term in title_lower for term in ['lex', 'decretum', 'constitutio']):
            return 'legal_text'
        
        return 'prose'
    
    def _estimate_period(self, title: str) -> str:
        """Estimate the time period of the work."""
        # This is a simplified heuristic - could be enhanced with more data
        title_lower = title.lower()
        
        # Classical authors (roughly 1st century BCE to 2nd century CE)
        classical_authors = [
            'cicero', 'caesar', 'vergilius', 'horatius', 'ovidius', 'livius',
            'tacitus', 'suetonius', 'plautus', 'terentius', 'lucretius',
            'catullus', 'propertius', 'tibullus', 'juvenalis', 'martialis'
        ]
        
        if any(author in title_lower for author in classical_authors):
            return 'classical'
        
        # Late antique/early Christian (3rd-6th centuries)
        late_antique_authors = [
            'augustinus', 'hieronymus', 'ambrosius', 'boethius'
        ]
        
        if any(author in title_lower for author in late_antique_authors):
            return 'late_antique'
        
        # Medieval (7th-15th centuries)
        medieval_authors = [
            'gregorius', 'isidorus', 'beda', 'alcuinus', 'aquinas',
            'bernardus', 'anselmus', 'abelardus'
        ]
        
        if any(author in title_lower for author in medieval_authors):
            return 'medieval'
        
        return 'unknown'
    
    def _assign_priority(self, title: str, text_content: str) -> str:
        """Assign priority level for scraping."""
        title_lower = title.lower()
        
        # Critical works - major classical texts
        critical_works = [
            'commentarii de bello gallico', 'aeneis', 'metamorphoses',
            'de re publica', 'de officiis', 'confessiones', 'summa theologiae',
            'de philosophiae consolatione', 'annales', 'historiae'
        ]
        
        if any(work in title_lower for work in critical_works):
            return 'critical'
        
        # High priority - well-known authors
        high_priority_authors = [
            'cicero', 'caesar', 'vergilius', 'horatius', 'ovidius',
            'augustinus', 'aquinas', 'boethius'
        ]
        
        if any(author in title_lower for author in high_priority_authors):
            return 'high'
        
        # Substantial content gets medium priority
        if len(text_content.strip()) > 2000:
            return 'medium'
        
        return 'normal'
    
    def save_titles_list(self, works: List[Dict], output_file: str):
        """Save the list of works to a file for the scraper to use."""
        self.logger.info(f"Saving {len(works)} works to {output_file}")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write header
            f.write("# Comprehensive Latin Works List\n")
            f.write(f"# Generated from XML dump analysis\n")
            f.write(f"# Total works: {len(works)}\n\n")
            
            # Group by priority
            priorities = ['critical', 'high', 'medium', 'normal']
            for priority in priorities:
                priority_works = [w for w in works if w.get('priority') == priority]
                if priority_works:
                    f.write(f"## {priority.upper()} PRIORITY ({len(priority_works)} works)\n")
                    for work in priority_works:
                        f.write(f"{work['title']}\t{work['author']}\t{work['work_type']}\t{work['estimated_period']}\n")
                    f.write("\n")
        
        self.logger.info(f"Works list saved to {output_file}")


def main():
    """Main function to extract comprehensive Latin titles."""
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    extractor = ComprehensiveLatinExtractor()
    
    xml_file = "/Users/willow/Documents/Combined Scraper & Cleaner/LawikiSource Dump Jul 20 2025.xml"
    output_file = "/Users/willow/Documents/Combined Scraper & Cleaner/comprehensive_latin_works.txt"
    
    # Extract all Latin works
    works = extractor.extract_all_latin_titles(xml_file)
    
    # Save the list
    extractor.save_titles_list(works, output_file)
    
    print(f"Extracted {len(works)} Latin works")
    print(f"Saved to: {output_file}")
    
    # Print summary by priority
    from collections import Counter
    priority_counts = Counter(work.get('priority', 'unknown') for work in works)
    work_type_counts = Counter(work.get('work_type', 'unknown') for work in works)
    period_counts = Counter(work.get('estimated_period', 'unknown') for work in works)
    
    print("\nSummary by Priority:")
    for priority, count in priority_counts.most_common():
        print(f"  {priority}: {count}")
    
    print("\nSummary by Work Type:")
    for work_type, count in work_type_counts.most_common():
        print(f"  {work_type}: {count}")
    
    print("\nSummary by Period:")
    for period, count in period_counts.most_common():
        print(f"  {period}: {count}")


if __name__ == "__main__":
    main()