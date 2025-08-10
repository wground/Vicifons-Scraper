# **VICIFONS SCRAPER**
*Comprehensive Latin Text Harvester for la.wikisource.org*

## **COMMENTS**

I would really like to go to bed soon. The main goal of this project is to scrape texts so that I can use them as training data for something else. This is the first time I've done something like this. I just read about half of a book on Python today and most of this was done with Claude Code. If it weren't the middle of the night I'm sure it'd be better. I am sure there are millions of better ways to build a text scraper, but this is my "learning experience" / poor attempt.

**Project Status:** It's a mess but it works I guess. 
**Performance Notes:**  Poor, but it's like 4AM rn.
**Issues Found:** There are *a lot* of issues. I have no idea why we even need to do the post-processing, there's some real problem with how index files are handled. IF I ever have cause to return to this project it'll be to fix that. Also the main scraper is like 1k lines of incomprehensible spaghetti.

---

</div>

## **PURPOSE**

The Vicifons Scraper is a sophisticated tool designed to harvest Latin literary texts from **la.wikisource.org** (Latin Wikisource). It systematically downloads non-fragmentary Latin sources from antiquity through the 15th century, organizing them by historical period and literary genre.

<div class="elegant-section">

**Target Categories:**
- **Latinitas Romana** - Classical Latin works
- **Latinitas Mediaevalis** - Medieval Latin texts  
- **Saeculum XV** - 15th century sources

**Output:** Clean `.txt` files categorized as prose/poetry, suitable for corpus linguistics and LLM training.

</div>

## **SYSTEM ARCHITECTURE**

### **Core Components**

<div class="elegant-list">

**Main Scraper** (`vicifons_scraper_optimized.py`)
- High-performance concurrent downloader with aiohttp
- Comprehensive category traversal and author-based searches
- Index page detection with automatic chapter extraction
- Genre classification using categoria:genera cross-referencing
- Configurable caching system with smart cache duration

</div>

<div class="elegant-list">

** Post-Processor** (`post_process_indices.py`)
- Detects incorrectly saved index files containing only chapter lists
- Cross-validates with categoria:capita ex operibus
- Downloads individual chapters to replace index files
- Creates backup files (.index_backup) for recovery

</div>

<div class="elegant-list">

**Testing Suite**
- `quick_test.py` - Tests specific problematic works
- `micro_scraper.py` - Comprehensive test on 50+ representative works
- `test_fixes.py` - Validates index detection and chapter extraction

</div>

## **OPERATION WORKFLOW**

### **Phase I: Primary Harvest**
```bash
python3 vicifons_scraper_optimized.py
```
- Traverses categories with 4-level depth recursion
- Downloads texts using ws-export.wmcloud.org API for clean extraction
- Filters out fragmentary/epigraphic content automatically
- Saves with standardized headers and metadata

### **Phase II: Index Resolution**  
```bash
python3 post_process_indices.py
```
- Scans all downloaded files for index-like content patterns
- Identifies works like "Ab Urbe Condita - Periochae.txt" containing only chapter lists
- Replaces index files with complete individual chapters
- Cross-validates against Wikisource's categoria:capita ex operibus

<div class="elegant-section">

**Key Innovation:** The post-processor solves the critical problem where major works (Caesar, Livy, Gellius) were being saved as link lists instead of actual content. It automatically detects and corrects these cases.

</div>

## **TECHNICAL FEATURES**

### **Performance Optimizations**
- **Concurrent downloads** with configurable limits (default: 10 concurrent)
- **Smart caching** with adjustable duration (default: 12 hours) 
- **Rate limiting** to respect server resources
- **Resume capability** with state persistence

### **Content Quality Assurance**
- **Fragmentary content detection** using multiple heuristics
- **Epigraphic filtering** to exclude inscriptions
- **Text quality validation** with minimum length requirements
- **Genre classification** using Wikisource's native categorization

### **Error Handling & Recovery**
- **Comprehensive logging** with configurable verbosity
- **Graceful failure handling** with retry mechanisms
- **State preservation** for interrupted sessions
- **Backup creation** for modified files

## **CONFIGURATION**

<blockquote>
**Cache Duration:** Set cache_duration_hours=1 for rapid testing, cache_duration_hours=24 for production runs.

**Concurrency:** Adjust max_concurrent (default: 10) based on your connection and server considerations.

**Output Structure:** Use single_folder=True for unified output, single_folder=False for period-based organization.
</blockquote>

## **REQUIREMENTS**

```
pywikibot>=7.0.0
requests>=2.25.0  
aiohttp>=3.8.0
aiofiles>=0.8.0
```

## **EXPECTED RESULTS**

The complete system successfully processes major classical works including:

- **Aeneis** (12 books) → Individual liber files
- **Ab Urbe Condita** (140+ periochae) → Complete chapter breakdown  
- **Commentarii de bello Gallico** (8 books) → Full campaign narratives
- **Noctes Atticae** (20 books) → Complete miscellanea
    *This stuff is a bit of a mess, I'm really sorry.*
---
