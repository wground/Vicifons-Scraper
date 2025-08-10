# **VICIFONS SCRAPER**
*Comprehensive Latin Text Harvester for la.wikisource.org*

<style>
@font-face {
  font-family: 'Junicode';
  src: url('./Markdown/Junicode.ttf') format('truetype');
  font-weight: normal;
  font-style: normal;
}
@font-face {
  font-family: 'Junicode';
  src: url('./Markdown/Junicode-Bold.ttf') format('truetype');
  font-weight: bold;
  font-style: normal;
}
@font-face {
  font-family: 'Junicode';
  src: url('./Markdown/Junicode-Italic.ttf') format('truetype');
  font-weight: normal;
  font-style: italic;
}
@font-face {
  font-family: 'Junicode';
  src: url('./Markdown/Junicode-BoldItalic.ttf') format('truetype');
  font-weight: bold;
  font-style: italic;
}

body {
  font-family: 'Junicode', serif;
  line-height: 1.6;
  color: #2c2c2c;
  background: #fefefe;
  max-width: 900px;
  margin: 0 auto;
  padding: 2em;
}

h1 {
  font-family: 'Junicode', serif;
  font-weight: bold;
  font-size: 2.5em;
  text-align: center;
  color: #8B0000;
  margin-bottom: 0.2em;
  letter-spacing: 0.02em;
  text-shadow: 1px 1px 2px rgba(139,0,0,0.1);
}

h2 {
  font-family: 'Junicode', serif;
  font-weight: bold;
  font-size: 1.6em;
  color: #4A4A4A;
  border-bottom: 2px solid #DAA520;
  padding-bottom: 0.3em;
  margin-top: 2em;
  margin-bottom: 1em;
}

h3 {
  font-family: 'Junicode', serif;
  font-weight: bold;
  font-size: 1.2em;
  color: #8B0000;
  margin-top: 1.5em;
  margin-bottom: 0.8em;
}

em {
  font-family: 'Junicode', serif;
  font-style: italic;
  color: #666;
  text-align: center;
  display: block;
  margin-bottom: 2em;
  font-size: 1.1em;
}

code {
  font-family: 'Monaco', 'Menlo', monospace;
  background: #f8f8f8;
  border: 1px solid #e0e0e0;
  padding: 2px 6px;
  border-radius: 3px;
  font-size: 0.9em;
}

pre {
  background: #f8f8f8;
  border: 1px solid #e0e0e0;
  border-radius: 6px;
  padding: 1em;
  overflow-x: auto;
  margin: 1em 0;
}

pre code {
  background: transparent;
  border: none;
  padding: 0;
}

.elegant-section {
  background: linear-gradient(135deg, #f9f9f9 0%, #fefefe 100%);
  border: 1px solid #e8e8e8;
  border-radius: 8px;
  padding: 1.5em;
  margin: 1.5em 0;
  box-shadow: 0 2px 4px rgba(0,0,0,0.05);
}

.comment-box {
  background: linear-gradient(135deg, #fff8dc 0%, #ffeaa7 100%);
  border: 2px solid #DAA520;
  border-radius: 8px;
  padding: 1.5em;
  margin: 2em 0;
  box-shadow: 0 3px 6px rgba(218,165,32,0.2);
}

.elegant-list {
  background: #fbfbfb;
  border-left: 4px solid #DAA520;
  padding: 1em;
  margin: 1em 0;
}

.elegant-list ul {
  margin: 0;
  padding-left: 1.2em;
}

blockquote {
  border-left: 4px solid #DAA520;
  margin: 1.5em 0;
  padding: 0.5em 1em;
  background: #fafafa;
  font-style: italic;
  color: #666;
}
</style>

<div class="comment-box">

## **USER COMMENTS**

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

**🔧 Post-Processor** (`post_process_indices.py`)
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

**Performance Target:** 10x+ speed improvement over basic scrapers through concurrent processing and intelligent caching.

---

*Built with Junicode typography for classical text presentation*