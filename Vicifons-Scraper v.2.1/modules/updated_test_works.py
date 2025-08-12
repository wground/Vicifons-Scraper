#!/usr/bin/env python3
"""
Updated Test Works Database with Enhanced Categorization

Includes both Classical/Post-Classical and Prose/Poetry examples for testing
the enhanced categorization system.
"""

from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

def get_enhanced_test_works() -> List[Dict]:
    """
    Get enhanced test works with proper period and genre categorization.
    """
    
    # Classical Prose (High Priority) - CRITICAL WORKS FIRST
    classical_prose_high = [
        {
            'title': 'Commentarii de bello Gallico',
            'author': 'Caesar',
            'period': 'classical',
            'genre': 'prose', 
            'category': 'historical_prose',
            'issues': ['index_detection_failed', 'chapters_not_downloaded'],
            'priority': 'critical',  # Changed to critical
            'expected_chapters': 8,
            'test_categorization': True,
            'force_scrape': True  # Force scraping for critical works
        },
        {
            'title': 'Commentarii de bello civili',
            'author': 'Caesar',
            'period': 'classical',
            'genre': 'prose',
            'category': 'historical_prose',
            'issues': ['index_detection_failed'],
            'priority': 'high',
            'expected_chapters': 3,
            'test_categorization': True
        },
        {
            'title': 'Ab Urbe Condita',
            'author': 'Livius',
            'period': 'classical',
            'genre': 'prose',
            'category': 'historical_prose',
            'issues': ['index_incorrectly_tagged', 'periochae_confusion'],
            'priority': 'high',
            'expected_chapters': 142
        },
        {
            'title': 'Noctes Atticae',
            'author': 'Gellius',
            'period': 'classical',
            'genre': 'prose',
            'category': 'miscellany_prose',
            'issues': ['index_detection_inconsistent'],
            'priority': 'critical',  # Changed to critical
            'expected_chapters': 20,
            'force_scrape': True  # Force scraping for critical works
        }
    ]
    
    # Classical Poetry (High Priority)
    classical_poetry_high = [
        {
            'title': 'Aeneis',
            'author': 'Vergilius',
            'period': 'classical',
            'genre': 'poetry',
            'category': 'epic_poetry',
            'issues': ['some_chapters_missing'],
            'priority': 'critical',  # Changed to critical
            'expected_chapters': 12,
            'test_categorization': True,
            'force_scrape': True  # Force scraping for critical works
        },
        {
            'title': 'Metamorphoses (Ovidius)',
            'author': 'Ovidius', 
            'period': 'classical',
            'genre': 'poetry',
            'category': 'narrative_poetry',
            'issues': ['book_structure_complex'],
            'priority': 'high',
            'expected_chapters': 15
        },
        {
            'title': 'Carmina (Horatius)',
            'author': 'Horatius',
            'period': 'classical',
            'genre': 'poetry',
            'category': 'lyric_poetry',
            'issues': ['ode_collections', 'book_structure'],
            'priority': 'medium',
            'expected_chapters': 4
        }
    ]
    
    # Post-Classical Prose (High Priority)
    post_classical_prose_high = [
        {
            'title': 'De civitate Dei',
            'author': 'Augustinus',
            'period': 'post_classical', 
            'genre': 'prose',
            'category': 'christian_prose',
            'issues': ['very_long_work', 'book_organization'],
            'priority': 'high',
            'expected_chapters': 22,
            'test_categorization': True
        },
        {
            'title': 'Confessiones',
            'author': 'Augustinus',
            'period': 'post_classical',
            'genre': 'prose', 
            'category': 'autobiographical_prose',
            'issues': ['text_cleaning_challenges'],
            'priority': 'high',
            'expected_chapters': 13,
            'test_categorization': True
        }
    ]
    
    # Post-Classical Poetry (Medium Priority)
    post_classical_poetry = [
        {
            'title': 'Vexilla Regis',
            'author': 'Venantius Fortunatus',
            'period': 'post_classical',
            'genre': 'poetry',
            'category': 'christian_poetry',
            'issues': ['hymn_structure'],
            'priority': 'medium',
            'expected_chapters': 1
        },
        {
            'title': 'Te Deum',
            'author': 'Ambrosius',
            'period': 'post_classical',
            'genre': 'poetry',
            'category': 'liturgical_poetry',
            'issues': ['hymn_attribution'],
            'priority': 'medium', 
            'expected_chapters': 1
        }
    ]
    
    # Additional Classical Prose (Medium Priority)
    classical_prose_medium = [
        {
            'title': 'De officiis',
            'author': 'Cicero',
            'period': 'classical',
            'genre': 'prose',
            'category': 'philosophical_prose',
            'issues': ['book_divisions'],
            'priority': 'medium',
            'expected_chapters': 3
        },
        {
            'title': 'Naturalis Historia',
            'author': 'Plinius Maior',
            'period': 'classical',
            'genre': 'prose',
            'category': 'scientific_prose',
            'issues': ['massive_work', 'book_organization'],
            'priority': 'critical',  # Changed to critical
            'expected_chapters': 37,
            'force_scrape': True  # Force scraping for critical works
        },
        {
            'title': 'Annales',
            'author': 'Tacitus',
            'period': 'classical',
            'genre': 'prose',
            'category': 'historical_prose',
            'issues': ['incomplete_preservation', 'book_numbering'],
            'priority': 'medium',
            'expected_chapters': 16
        }
    ]
    
    # Additional Classical Poetry (Medium Priority) 
    classical_poetry_medium = [
        {
            'title': 'Satirae (Horatius)',
            'author': 'Horatius',
            'period': 'classical',
            'genre': 'poetry',
            'category': 'satirical_poetry',
            'issues': ['satire_numbering'],
            'priority': 'medium',
            'expected_chapters': 2
        },
        {
            'title': 'Carmina (Catullus)',
            'author': 'Catullus',
            'period': 'classical',
            'genre': 'poetry',
            'category': 'lyric_poetry',
            'issues': ['single_collection', 'poem_numbering'],
            'priority': 'medium',
            'expected_chapters': 1
        },
        {
            'title': 'Pharsalia',
            'author': 'Lucanus',
            'period': 'classical',
            'genre': 'poetry',
            'category': 'epic_poetry',
            'issues': ['epic_poem', 'incomplete_work'],
            'priority': 'low',
            'expected_chapters': 10
        }
    ]
    
    # Additional Post-Classical Prose (Medium Priority)
    post_classical_prose_medium = [
        {
            'title': 'Summa Theologica',
            'author': 'Thomas Aquinas',
            'period': 'post_classical',
            'genre': 'prose',
            'category': 'scholastic_prose',
            'issues': ['scholastic_structure', 'question_format'],
            'priority': 'medium',
            'expected_chapters': 3
        },
        {
            'title': 'Consolatio Philosophiae',
            'author': 'Boethius',
            'period': 'post_classical',
            'genre': 'mixed',  # Has both prose and poetry
            'category': 'philosophical_prose',
            'issues': ['prose_poetry_mix'],
            'priority': 'medium',
            'expected_chapters': 5
        },
        {
            'title': 'Etymologiae',
            'author': 'Isidorus Hispalensis',
            'period': 'post_classical',
            'genre': 'prose',
            'category': 'encyclopedic_prose',
            'issues': ['massive_encyclopedia', 'book_structure'],
            'priority': 'low',
            'expected_chapters': 20
        }
    ]
    
    # Lower priority works for comprehensive testing
    lower_priority_works = [
        # Legal texts
        {
            'title': 'Institutiones (Gaius)',
            'author': 'Gaius',
            'period': 'classical',
            'genre': 'prose',
            'category': 'legal_prose',
            'issues': ['legal_commentary', 'technical_language'],
            'priority': 'low',
            'expected_chapters': 4
        },
        # Christian Latin
        {
            'title': 'Vulgata',
            'author': 'Hieronymus',
            'period': 'post_classical',
            'genre': 'mixed',
            'category': 'biblical_text',
            'issues': ['biblical_text', 'verse_structure'],
            'priority': 'low',
            'expected_chapters': 66
        }
    ]
    
    # Combine all works
    all_works = (
        classical_prose_high + 
        classical_poetry_high + 
        post_classical_prose_high + 
        post_classical_poetry +
        classical_prose_medium +
        classical_poetry_medium +
        post_classical_prose_medium +
        lower_priority_works
    )
    
    logger.info(f"Enhanced test works: {len(all_works)} total")
    
    # Count by categories
    period_counts = {}
    genre_counts = {}
    for work in all_works:
        period = work['period']
        genre = work['genre']
        period_counts[period] = period_counts.get(period, 0) + 1
        genre_counts[genre] = genre_counts.get(genre, 0) + 1
    
    logger.info(f"Period distribution: {period_counts}")
    logger.info(f"Genre distribution: {genre_counts}")
    
    return all_works

def get_categorization_test_works() -> List[Dict]:
    """Get works specifically for testing categorization."""
    all_works = get_enhanced_test_works()
    return [work for work in all_works if work.get('test_categorization', False)]

def get_works_by_period_genre(period: str = None, genre: str = None) -> List[Dict]:
    """Get works filtered by period and/or genre."""
    all_works = get_enhanced_test_works()
    
    filtered = all_works
    if period:
        filtered = [w for w in filtered if w.get('period') == period]
    if genre:
        filtered = [w for w in filtered if w.get('genre') == genre]
    
    return filtered

def print_categorization_summary():
    """Print summary of categorization test works."""
    all_works = get_enhanced_test_works()
    
    print("Enhanced Test Works - Categorization Summary")
    print("=" * 60)
    print(f"Total works: {len(all_works)}")
    
    # Count by period
    periods = {}
    genres = {}
    priorities = {}
    
    for work in all_works:
        period = work.get('period', 'unknown')
        genre = work.get('genre', 'unknown')
        priority = work.get('priority', 'unknown')
        
        periods[period] = periods.get(period, 0) + 1
        genres[genre] = genres.get(genre, 0) + 1
        priorities[priority] = priorities.get(priority, 0) + 1
    
    print(f"\nBy Period:")
    for period, count in periods.items():
        print(f"  {period}: {count}")
    
    print(f"\nBy Genre:")
    for genre, count in genres.items():
        print(f"  {genre}: {count}")
    
    print(f"\nBy Priority:")
    for priority, count in priorities.items():
        print(f"  {priority}: {count}")
    
    # Show categorization test works
    cat_test_works = get_categorization_test_works()
    print(f"\nCategorization Test Works ({len(cat_test_works)}):")
    for work in cat_test_works:
        print(f"  {work['title']} ({work['author']}) - {work['period']}/{work['genre']}")

if __name__ == "__main__":
    print_categorization_summary()