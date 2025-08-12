# Vicifons Scraper

## Notes

August 12: Improved removal of chapter headings. Still need to look into caching problems if I ever need to re-scrape, or repurpose this code to scrape something else.
August 11: Revised everything and updated to current version, merging scraping and cleaning functionality. Spent a lot of time trying to figure out the best way to scale scraping, though I'm still not really certain that we're good. There is a known issue with caching harming download speed about halfway throught the Vicifons filtered corpus. You may ask me: 'couldn't this be much easier if you just took directly from the XML datadump?' I answer: yes, but.
August 9: I would really like to go to bed soon. The main goal of this project is to scrape texts so that I can use them as training data for something else. This is the first time I've done something like this. I just read about half of a book on Python today and most of this was done with Claude Code. If it weren't the middle of the night I'm sure it'd be better. I am sure there are millions of better ways to build a text scraper, but this is my "learning experience" / poor attempt. Project Status: It's a mess but it works I guess. Performance Notes: Poor, but it's like 4AM rn. Issues Found: There are a lot of issues. I have no idea why we even need to do the post-processing, there's some real problem with how index files are handled. IF I ever have cause to return to this project it'll be to fix that. Also the main scraper is like 1k lines of incomprehensible spaghetti.

## Project Overview

This project scrapes and processes Latin texts from Vicifons (la.wikisource.org) for LLM training data. It extracts ~8,000 historical Latin works (Classical through Early Renaissance), cleans the text, and organizes them by period and type.

## Project Structure

```
Combined Scraper & Cleaner/
├── combined_latin_processor.py        # Main entry point
├── modules/                          # Core functionality
│   ├── scraper.py                   # Vicifons scraper with index detection
│   ├── enhanced_cleaner.py          # Text cleaner with categorization
│   ├── filtered_extractor.py        # XML dump analyzer for work discovery
│   ├── simple_extractor.py          # Basic XML dump extractor
│   ├── orthography.py               # Latin text standardization
│   ├── updated_test_works.py        # Test dataset definitions
│   └── utils.py                     # Shared utilities
├── filtered_latin_works.json         # Pre-categorized work metadata (8k works)
├── all_latin_works.json             # Comprehensive work list (15k works)
└── LawikiSource Dump Jul 20 2025.xml # Vicifons XML dump
```

## Component Functions

### Main Components

**`combined_latin_processor.py`**
- Main orchestrator script
- Handles user configuration and mode selection
- Coordinates scraping and cleaning phases
- Supports test, test-parallel, and full corpus modes

**`modules/scraper.py`**
- Downloads Latin texts from Vicifons
- Detects index pages and extracts individual chapters
- Handles known multi-part works (Caesar, Virgil, etc.)
- Creates enhanced metadata headers
- Supports concurrent downloading

**`modules/enhanced_cleaner.py`**
- Cleans scraped texts for LLM training
- Removes export metadata and non-Latin content
- Categorizes texts by period (classical/post-classical) and type (prose/poetry)
- Standardizes orthography and expands abbreviations
- Creates categorized directory structure

### Analysis Components

**`modules/filtered_extractor.py`**
- Analyzes XML dump to identify historical Latin works
- Filters out fragments, modern texts, and non-content
- Pre-categorizes works by author, period, and type
- Covers Classical through Early Renaissance (up to ~1600)
- Produces high-quality filtered dataset

**`modules/simple_extractor.py`**
- Basic XML dump extractor with minimal filtering
- Extracts all main namespace content
- Used as fallback when filtered extraction unavailable

### Support Components

**`modules/orthography.py`**
- Standardizes Latin orthography (u/v, i/j variations)
- Handles medieval spelling variations
- Optimizes text for consistent LLM training

**`modules/utils.py`**
- Shared utilities (logging, progress tracking, filename cleaning)
- Directory structure creation
- Text validation and statistics

**`modules/updated_test_works.py`**
- Defines test dataset of critical/problematic works
- Used for testing and validation

## Data Flow

1. **Discovery**: `filtered_extractor.py` analyzes XML dump → `filtered_latin_works.json`
2. **Scraping**: `scraper.py` downloads texts → `raw_scraped/` directory
3. **Cleaning**: `enhanced_cleaner.py` processes texts → `cleaned_texts/` with categorization
4. **Organization**: Texts sorted into `classical/prose/`, `classical/poetry/`, etc.

## Output Structure

```
processed_latin_texts/
├── raw_scraped/                     # Original scraped files
├── cleaned_texts/                   # Processed and categorized texts
│   ├── classical/
│   │   ├── prose/                   # Classical Latin prose
│   │   └── poetry/                  # Classical Latin poetry
│   ├── post_classical/
│   │   ├── prose/                   # Medieval/Renaissance prose
│   │   └── poetry/                  # Medieval/Renaissance poetry
│   └── unknown/
│       └── uncategorized/           # Unclassified texts
├── re_cleaned_texts/                # Enhanced cleaning output (v2)
│   └── cleaned_texts/               # Re-processed with improved cleaning
│       ├── classical/
│       │   ├── prose/               # Purer classical prose (no headings/formatting)
│       │   └── poetry/              # Purer classical poetry
│       ├── post_classical/
│       │   ├── prose/               # Cleaner medieval/renaissance prose
│       │   └── poetry/              # Cleaner medieval/renaissance poetry
│       └── unknown/
│           └── uncategorized/       # Re-cleaned unclassified texts
├── logs/                           # Processing logs
├── cache/                          # Caching data
└── test_results/                   # Test outputs
```

## Usage

### Interactive Mode
```bash
python3 combined_latin_processor.py
```

### Command Line Mode
```bash
# Test mode (small dataset)
python3 combined_latin_processor.py --mode test

# Full corpus (8k works)
python3 combined_latin_processor.py --mode full --max-concurrent 25

# With custom settings
python3 combined_latin_processor.py --mode full --max-concurrent 50 --output-dir my_output
```

## Key Features

- **Comprehensive Coverage**: 8,055 filtered Latin works vs ~1,000 from category-based approaches
- **Intelligent Index Detection**: Automatically detects and processes multi-part works
- **Pre-categorization**: Works classified by period and type before scraping
- **Enhanced Metadata**: Rich headers with author, period, work type, completeness
- **Chapter Handling**: Proper processing of individual book chapters
- **Quality Filtering**: Excludes fragments, modern scholarship, non-Latin content
- **Concurrent Processing**: Configurable parallel downloading and cleaning
- **Orthography Standardization**: Consistent Latin text formatting

## Configuration Options

- **Processing Modes**: test, test-parallel, full
- **Concurrency Levels**: conservative (10), balanced (25), aggressive (50), custom
- **Output Control**: Custom output directories, caching, NLP processing
- **Filtering**: Time period limits, content type selection
- **Logging**: Debug, info, warning, error levels

## Data Sources

- **Primary**: Vicifons (la.wikisource.org) - Latin Wikisource
- **Metadata**: XML dump analysis for comprehensive work discovery
- **Coverage**: Classical Latin (1st century B.C. - 5th century A.D.) through Early Renaissance (~1600)
- **Content Types**: Complete works, book chapters, substantial excerpts
