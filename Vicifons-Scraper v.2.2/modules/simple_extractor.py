#!/usr/bin/env python3
"""
Simple Latin Content Extractor

Just extract all main namespace pages from the XML dump with minimal filtering
to maximize Latin content for LLM training.
"""

import xml.etree.ElementTree as ET
import re
import json
from typing import List, Dict


def extract_all_main_namespace_titles(xml_file_path: str) -> List[Dict]:
    """Extract all main namespace titles with minimal filtering."""
    print(f"Extracting all main namespace content from: {xml_file_path}")
    
    works = []
    processed_count = 0
    
    try:
        for event, elem in ET.iterparse(xml_file_path, events=('start', 'end')):
            if event == 'end' and elem.tag.endswith('}page'):
                processed_count += 1
                
                # Extract page data
                ns_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.11/}ns')
                title_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.11/}title')
                text_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.11/}text')
                
                if (ns_elem is not None and 
                    title_elem is not None and 
                    text_elem is not None and
                    ns_elem.text == '0'):  # Main namespace only
                    
                    title = title_elem.text
                    text_content = text_elem.text or ''
                    
                    # Very minimal filtering - just exclude obvious non-content
                    if (title and 
                        len(text_content.strip()) > 50 and
                        not text_content.strip().startswith('#REDIRECT') and
                        not text_content.strip().startswith('#redirect') and
                        not any(skip in title.lower() for skip in [
                            '.css', '.js', '.json', 'mediawiki:', 'special:'
                        ])):
                        
                        work_data = {
                            'title': title,
                            'content_length': len(text_content.strip()),
                            'author': extract_author_from_title(title),
                            'priority': assign_priority(title)
                        }
                        works.append(work_data)
                
                # Clear element to save memory
                elem.clear()
                
                # Progress logging
                if processed_count % 5000 == 0:
                    print(f"Processed {processed_count} pages, found {len(works)} works")
        
    except Exception as e:
        print(f"Error parsing XML dump: {e}")
        raise
    
    print(f"Extraction complete: {len(works)} works from {processed_count} pages")
    return works


def extract_author_from_title(title: str) -> str:
    """Simple author extraction."""
    # Check for parenthetical author indication
    if '(' in title and ')' in title:
        match = re.search(r'\(([^)]+)\)$', title)
        if match:
            return match.group(1).strip()
    
    # Check for slash-separated format
    if '/' in title:
        parts = title.split('/')
        return parts[0].strip()
    
    # Known author patterns
    author_patterns = {
        'cicero': 'Marcus Tullius Cicero',
        'caesar': 'Gaius Iulius Caesar',
        'vergilius': 'Publius Vergilius Maro',
        'horatius': 'Quintus Horatius Flaccus',
        'ovidius': 'Publius Ovidius Naso',
        'tacitus': 'Publius Cornelius Tacitus',
        'augustinus': 'Augustinus Hipponensis',
        'aquinas': 'Thomas Aquinas',
        'boethius': 'Boethius'
    }
    
    title_lower = title.lower()
    for pattern, author in author_patterns.items():
        if pattern in title_lower:
            return author
    
    return "Unknown"


def assign_priority(title: str) -> str:
    """Assign simple priority."""
    title_lower = title.lower()
    
    # Critical works
    critical_works = [
        'commentarii de bello gallico', 'aeneis', 'metamorphoses',
        'de re publica', 'de officiis', 'confessiones', 'summa theologiae'
    ]
    
    if any(work in title_lower for work in critical_works):
        return 'critical'
    
    # High priority authors
    high_priority = [
        'cicero', 'caesar', 'vergilius', 'horatius', 'ovidius', 'augustinus'
    ]
    
    if any(author in title_lower for author in high_priority):
        return 'high'
    
    return 'normal'


def main():
    xml_file = "/Users/willow/Documents/Combined Scraper & Cleaner/LawikiSource Dump Jul 20 2025.xml"
    output_file = "/Users/willow/Documents/Combined Scraper & Cleaner/all_latin_works.json"
    
    # Extract works
    works = extract_all_main_namespace_titles(xml_file)
    
    # Save as JSON for the scraper
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(works, f, ensure_ascii=False, indent=2)
    
    print(f"Saved {len(works)} works to {output_file}")
    
    # Print sample titles
    print("\nSample titles:")
    for work in works[:20]:
        print(f"  {work['title']} ({work['content_length']} chars)")
    
    # Print statistics
    from collections import Counter
    
    priorities = Counter(work['priority'] for work in works)
    print(f"\nPriority breakdown:")
    for priority, count in priorities.items():
        print(f"  {priority}: {count}")
    
    # Content length stats
    lengths = [work['content_length'] for work in works]
    if lengths:
        print(f"\nContent length stats:")
        print(f"  Average: {sum(lengths) / len(lengths):.0f} chars")
        print(f"  Median: {sorted(lengths)[len(lengths)//2]:.0f} chars")
        print(f"  Max: {max(lengths)} chars")


if __name__ == "__main__":
    main()