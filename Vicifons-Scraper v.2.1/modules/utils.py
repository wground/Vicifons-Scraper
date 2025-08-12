#!/usr/bin/env python3
"""
Utility functions for the Combined Latin Processor
"""

import logging
import os
from pathlib import Path
from typing import Dict, List
import time
from datetime import datetime

def setup_logging(level: str = 'INFO') -> logging.Logger:
    """Set up logging configuration."""
    log_level = getattr(logging, level.upper())
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Set up root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    log_file = f"latin_processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

def create_directories(base_path: Path):
    """Create necessary directory structure."""
    directories = [
        base_path,
        base_path / "raw_scraped",
        base_path / "cleaned_texts",
        base_path / "logs",
        base_path / "cache",
        base_path / "test_results"
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

def clean_filename(title: str, max_length: int = 200) -> str:
    """Clean a title for use as a filename."""
    # Remove or replace invalid characters
    invalid_chars = '<>:"/\\|?*'
    cleaned = title
    
    for char in invalid_chars:
        cleaned = cleaned.replace(char, '_')
    
    # Handle colons specially (often used in Latin titles)
    if ':' in title:
        cleaned = title.split(':', 1)[1].strip()
    
    # Truncate if too long
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length]
    
    return cleaned

def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"

def count_files_by_extension(directory: Path, extension: str = '.txt') -> int:
    """Count files with given extension in directory and subdirectories."""
    count = 0
    for root, dirs, files in os.walk(directory):
        count += len([f for f in files if f.endswith(extension)])
    return count

def get_file_size(file_path: Path) -> int:
    """Get file size in bytes."""
    try:
        return file_path.stat().st_size
    except (FileNotFoundError, PermissionError):
        return 0

def calculate_text_stats(text: str) -> Dict:
    """Calculate basic statistics about a text."""
    import re
    
    words = re.findall(r'\w+', text)
    lines = text.split('\n')
    
    return {
        'char_count': len(text),
        'word_count': len(words),
        'line_count': len(lines),
        'avg_line_length': len(text) / len(lines) if lines else 0,
        'avg_word_length': sum(len(word) for word in words) / len(words) if words else 0
    }

class ProgressTracker:
    """Simple progress tracking utility."""
    
    def __init__(self, total: int, description: str = "Processing"):
        self.total = total
        self.current = 0
        self.description = description
        self.start_time = time.time()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def update(self, amount: int = 1):
        """Update progress by specified amount."""
        self.current += amount
        
        if self.current % 10 == 0 or self.current == self.total:
            self.log_progress()
    
    def log_progress(self):
        """Log current progress."""
        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        remaining = self.total - self.current
        eta = remaining / rate if rate > 0 else 0
        
        percentage = (self.current / self.total) * 100 if self.total > 0 else 0
        
        self.logger.info(
            f"{self.description}: {self.current}/{self.total} "
            f"({percentage:.1f}%) - {rate:.1f}/s - "
            f"ETA: {format_duration(eta)}"
        )
    
    def finish(self):
        """Mark progress as complete."""
        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        
        self.logger.info(
            f"{self.description} complete: {self.current} items "
            f"in {format_duration(elapsed)} ({rate:.1f}/s)"
        )

def validate_latin_text(text: str) -> Dict:
    """
    Validate that text appears to be legitimate Latin.
    Returns validation results with confidence score.
    """
    import re
    
    # Common Latin words (high frequency)
    common_latin = {
        'et', 'in', 'est', 'non', 'ad', 'cum', 'de', 'ex', 'per', 'pro',
        'ab', 'ut', 'si', 'sed', 'qui', 'quae', 'quod', 'hic', 'haec', 'hoc',
        'ille', 'illa', 'illud', 'is', 'ea', 'id', 'ego', 'tu', 'nos', 'vos',
        'sum', 'esse', 'fuit', 'eram', 'erat', 'sunt', 'erant', 'homo',
        'deus', 'rex', 'populus', 'terra', 'caelum', 'animus', 'corpus'
    }
    
    # Latin-specific patterns
    latin_patterns = [
        r'que\b',  # -que conjunction
        r'\bne\b',  # question particle
        r'us\b',    # common ending
        r'um\b',    # common ending
        r'ae\b',    # common ending
        r'is\b',    # common ending
    ]
    
    words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
    if not words:
        return {'is_latin': False, 'confidence': 0.0, 'reason': 'no_words_found'}
    
    # Calculate scores
    common_word_score = sum(1 for word in words if word in common_latin) / len(words)
    pattern_matches = sum(1 for pattern in latin_patterns 
                         if re.search(pattern, text.lower()))
    pattern_score = min(pattern_matches / 3, 1.0)  # Normalize to 0-1
    
    # Check for non-Latin indicators
    non_latin_indicators = ['the', 'and', 'of', 'to', 'a', 'in', 'is', 'it', 'you', 'that']
    non_latin_score = sum(1 for word in words if word in non_latin_indicators) / len(words)
    
    # Overall confidence calculation
    confidence = (common_word_score * 0.5 + pattern_score * 0.3) - (non_latin_score * 0.2)
    confidence = max(0.0, min(1.0, confidence))
    
    is_latin = confidence > 0.3  # Threshold for Latin detection
    
    reason = "validated"
    if not is_latin:
        if non_latin_score > 0.1:
            reason = "non_latin_words_detected"
        elif common_word_score < 0.1:
            reason = "no_common_latin_words"
        else:
            reason = "insufficient_latin_patterns"
    
    return {
        'is_latin': is_latin,
        'confidence': confidence,
        'reason': reason,
        'common_word_ratio': common_word_score,
        'pattern_matches': pattern_matches
    }

def detect_text_type(text: str) -> str:
    """
    Detect whether text is prose, poetry, or mixed.
    Uses heuristics based on line length and structure.
    """
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    if not lines:
        return 'unknown'
    
    # Calculate line statistics
    line_lengths = [len(line) for line in lines]
    avg_length = sum(line_lengths) / len(line_lengths)
    
    # Poetry indicators
    short_lines = sum(1 for length in line_lengths if length < 60)
    very_short_lines = sum(1 for length in line_lengths if length < 40)
    
    # Prose indicators  
    long_lines = sum(1 for length in line_lengths if length > 100)
    very_long_lines = sum(1 for length in line_lengths if length > 150)
    
    # Calculate ratios
    total_lines = len(lines)
    short_ratio = short_lines / total_lines
    long_ratio = long_lines / total_lines
    
    # Classification logic
    if short_ratio > 0.6 and avg_length < 70:
        return 'poetry'
    elif long_ratio > 0.4 and avg_length > 100:
        return 'prose'
    elif short_ratio > 0.3 and long_ratio > 0.2:
        return 'mixed'
    else:
        return 'prose'  # Default assumption for Latin texts