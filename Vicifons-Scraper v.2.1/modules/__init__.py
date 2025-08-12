#!/usr/bin/env python3
"""
Combined Latin Processor Modules

Modular components for scraping and cleaning Latin texts from Vicifons.
"""

from .scraper import VicifonsScraper
from .cleaner import TextCleaner
from .orthography import OrthographyStandardizer
from .test_works import get_test_works, get_works_by_priority, get_problem_works
from .utils import setup_logging, create_directories, ProgressTracker

__version__ = "1.0.0"
__author__ = "Combined implementation based on existing scraper/cleaner"

__all__ = [
    'VicifonsScraper',
    'TextCleaner', 
    'OrthographyStandardizer',
    'get_test_works',
    'get_works_by_priority',
    'get_problem_works',
    'setup_logging',
    'create_directories',
    'ProgressTracker'
]