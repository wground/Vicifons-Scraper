#!/usr/bin/env python3
"""
Test Works Database

This module contains curated lists of problematic and critical Latin works
for testing the combined scraper-cleaner system. These works were selected
based on their historical importance and known issues with scraping/cleaning.
"""

from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

def get_test_works() -> List[Dict]:
    """
    Get a curated list of 50-100 problematic/critical works for testing.
    
    Returns:
        List of dictionaries with work metadata including:
        - title: The Vicifons page title
        - author: Author name
        - category: Classification (classical, medieval, etc.)
        - issues: Known problems with this work
        - priority: Testing priority (high, medium, low)
    """
    
    # High priority works - these MUST work correctly
    high_priority = [
        {
            'title': 'Commentarii de bello Gallico',
            'author': 'Caesar',
            'category': 'classical_prose',
            'issues': ['index_detection_failed', 'chapters_not_downloaded'],
            'priority': 'high',
            'expected_chapters': 8
        },
        {
            'title': 'Commentarii de bello civili',
            'author': 'Caesar', 
            'category': 'classical_prose',
            'issues': ['index_detection_failed'],
            'priority': 'high',
            'expected_chapters': 3
        },
        {
            'title': 'Aeneis',
            'author': 'Vergilius',
            'category': 'classical_poetry',
            'issues': ['some_chapters_missing'],
            'priority': 'high',
            'expected_chapters': 12
        },
        {
            'title': 'Ab Urbe Condita',
            'author': 'Livius',
            'category': 'classical_prose',
            'issues': ['index_incorrectly_tagged', 'periochae_confusion'],
            'priority': 'high',
            'expected_chapters': 142  # Periochae
        },
        {
            'title': 'Noctes Atticae',
            'author': 'Gellius',
            'category': 'classical_prose',
            'issues': ['index_detection_inconsistent'],
            'priority': 'high',
            'expected_chapters': 20
        },
        {
            'title': 'Metamorphoses (Ovidius)',
            'author': 'Ovidius',
            'category': 'classical_poetry',
            'issues': ['book_structure_complex'],
            'priority': 'high',
            'expected_chapters': 15
        },
        {
            'title': 'De civitate Dei',
            'author': 'Augustinus',
            'category': 'late_antique_prose',
            'issues': ['very_long_work', 'book_organization'],
            'priority': 'high',
            'expected_chapters': 22
        },
        {
            'title': 'Confessiones',
            'author': 'Augustinus',
            'category': 'late_antique_prose', 
            'issues': ['text_cleaning_challenges'],
            'priority': 'high',
            'expected_chapters': 13
        }
    ]
    
    # Medium priority works - important classical texts
    medium_priority = [
        # Cicero works
        {
            'title': 'De officiis',
            'author': 'Cicero',
            'category': 'classical_prose',
            'issues': ['book_divisions'],
            'priority': 'medium',
            'expected_chapters': 3
        },
        {
            'title': 'De oratore',
            'author': 'Cicero',
            'category': 'classical_prose',
            'issues': ['dialogue_structure'],
            'priority': 'medium',
            'expected_chapters': 3
        },
        {
            'title': 'Catilinarias',
            'author': 'Cicero',
            'category': 'classical_prose',
            'issues': ['speech_collection'],
            'priority': 'medium',
            'expected_chapters': 4
        },
        
        # Tacitus works
        {
            'title': 'Annales',
            'author': 'Tacitus',
            'category': 'classical_prose',
            'issues': ['incomplete_preservation', 'book_numbering'],
            'priority': 'medium',
            'expected_chapters': 16
        },
        {
            'title': 'Historiae (Tacitus)',
            'author': 'Tacitus',
            'category': 'classical_prose',
            'issues': ['fragmentary_state'],
            'priority': 'medium',
            'expected_chapters': 5
        },
        {
            'title': 'Agricola',
            'author': 'Tacitus',
            'category': 'classical_prose',
            'issues': ['single_book_work'],
            'priority': 'medium',
            'expected_chapters': 1
        },
        
        # Seneca works
        {
            'title': 'Epistulae morales ad Lucilium',
            'author': 'Seneca',
            'category': 'classical_prose',
            'issues': ['letter_collection', 'numbering_scheme'],
            'priority': 'medium',
            'expected_chapters': 20  # Books of letters
        },
        {
            'title': 'De ira',
            'author': 'Seneca',
            'category': 'classical_prose',
            'issues': ['philosophical_treatise'],
            'priority': 'medium',
            'expected_chapters': 3
        },
        
        # Poetry works
        {
            'title': 'Carmina (Horatius)',
            'author': 'Horatius',
            'category': 'classical_poetry',
            'issues': ['ode_collections', 'book_structure'],
            'priority': 'medium',
            'expected_chapters': 4
        },
        {
            'title': 'Satirae (Horatius)',
            'author': 'Horatius', 
            'category': 'classical_poetry',
            'issues': ['satire_numbering'],
            'priority': 'medium',
            'expected_chapters': 2
        },
        {
            'title': 'Carmina (Catullus)',
            'author': 'Catullus',
            'category': 'classical_poetry',
            'issues': ['single_collection', 'poem_numbering'],
            'priority': 'medium',
            'expected_chapters': 1
        },
        
        # Pliny works
        {
            'title': 'Naturalis Historia',
            'author': 'Plinius Maior',
            'category': 'classical_prose',
            'issues': ['massive_work', 'book_organization'],
            'priority': 'medium',
            'expected_chapters': 37
        },
        {
            'title': 'Epistulae (Plinius)',
            'author': 'Plinius Minor',
            'category': 'classical_prose',
            'issues': ['letter_books'],
            'priority': 'medium',
            'expected_chapters': 10
        },
        
        # Suetonius
        {
            'title': 'De vita Caesarum',
            'author': 'Suetonius',
            'category': 'classical_prose',
            'issues': ['biographical_collection'],
            'priority': 'medium',
            'expected_chapters': 12  # Lives of 12 Caesars
        }
    ]
    
    # Lower priority works - diverse selection for comprehensive testing
    lower_priority = [
        # Legal texts
        {
            'title': 'Institutiones (Gaius)',
            'author': 'Gaius',
            'category': 'legal_prose',
            'issues': ['legal_commentary', 'technical_language'],
            'priority': 'low',
            'expected_chapters': 4
        },
        
        # Christian Latin
        {
            'title': 'Vulgata',
            'author': 'Hieronymus',
            'category': 'christian_latin',
            'issues': ['biblical_text', 'verse_structure'],
            'priority': 'low',
            'expected_chapters': 66  # Books of Bible
        },
        {
            'title': 'De doctrina christiana',
            'author': 'Augustinus',
            'category': 'christian_latin',
            'issues': ['theological_treatise'],
            'priority': 'low',
            'expected_chapters': 4
        },
        
        # Medieval works
        {
            'title': 'Summa Theologica',
            'author': 'Thomas Aquinas',
            'category': 'medieval_latin',
            'issues': ['scholastic_structure', 'question_format'],
            'priority': 'low',
            'expected_chapters': 3  # Parts
        },
        {
            'title': 'Consolatio Philosophiae',
            'author': 'Boethius',
            'category': 'medieval_latin',
            'issues': ['prose_poetry_mix'],
            'priority': 'low',
            'expected_chapters': 5
        },
        
        # Scientific/Technical
        {
            'title': 'De re rustica (Columella)',
            'author': 'Columella',
            'category': 'technical_prose',
            'issues': ['agricultural_treatise', 'technical_vocabulary'],
            'priority': 'low',
            'expected_chapters': 12
        },
        {
            'title': 'De architectura',
            'author': 'Vitruvius',
            'category': 'technical_prose',
            'issues': ['architectural_treatise', 'technical_diagrams'],
            'priority': 'low',
            'expected_chapters': 10
        },
        
        # Epic poetry
        {
            'title': 'Pharsalia',
            'author': 'Lucanus',
            'category': 'classical_poetry',
            'issues': ['epic_poem', 'incomplete_work'],
            'priority': 'low',
            'expected_chapters': 10
        },
        {
            'title': 'Thebais',
            'author': 'Statius',
            'category': 'classical_poetry',
            'issues': ['epic_structure'],
            'priority': 'low',
            'expected_chapters': 12
        },
        
        # Later imperial works
        {
            'title': 'Res Gestae Divi Augusti',
            'author': 'Augustus',
            'category': 'imperial_prose',
            'issues': ['inscription_text', 'political_document'],
            'priority': 'low',
            'expected_chapters': 1
        },
        
        # Miscellaneous important works
        {
            'title': 'Satyricon',
            'author': 'Petronius',
            'category': 'classical_prose',
            'issues': ['fragmentary_novel', 'unusual_format'],
            'priority': 'low',
            'expected_chapters': 1  # Fragmentary
        },
        {
            'title': 'Metamorphoses (Apuleius)',
            'author': 'Apuleius',
            'category': 'late_classical_prose',
            'issues': ['novel_structure'],
            'priority': 'low',
            'expected_chapters': 11
        }
    ]
    
    # Combine all works
    all_works = high_priority + medium_priority + lower_priority
    
    logger.info(f"Generated test works list: {len(all_works)} works total")
    logger.info(f"High priority: {len(high_priority)}, Medium: {len(medium_priority)}, Low: {len(lower_priority)}")
    
    return all_works

def get_works_by_priority(priority: str) -> List[Dict]:
    """Get works filtered by priority level."""
    all_works = get_test_works()
    return [work for work in all_works if work['priority'] == priority]

def get_works_by_category(category: str) -> List[Dict]:
    """Get works filtered by category."""
    all_works = get_test_works()
    return [work for work in all_works if work['category'] == category]

def get_problem_works() -> List[Dict]:
    """Get works that have known scraping/cleaning issues."""
    all_works = get_test_works()
    return [work for work in all_works if work['issues']]

def print_test_summary():
    """Print a summary of the test works."""
    all_works = get_test_works()
    
    # Count by priority
    priority_counts = {}
    category_counts = {}
    issue_counts = {}
    
    for work in all_works:
        # Count priorities
        priority = work['priority']
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
        
        # Count categories
        category = work['category']
        category_counts[category] = category_counts.get(category, 0) + 1
        
        # Count issues
        for issue in work['issues']:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1
    
    print("Test Works Summary")
    print("=" * 50)
    print(f"Total works: {len(all_works)}")
    print()
    
    print("By Priority:")
    for priority, count in priority_counts.items():
        print(f"  {priority}: {count}")
    print()
    
    print("By Category:")
    for category, count in sorted(category_counts.items()):
        print(f"  {category}: {count}")
    print()
    
    print("Common Issues:")
    for issue, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {issue}: {count}")

if __name__ == "__main__":
    print_test_summary()