# Vicifons Scraper

Extracts clean Latin texts from a [Vicifons](https://la.wikisource.org) (Latin Wikisource) XML dump for use as LLM training data.
Not really a scraper anymore since it just takes stuff from an XML dump but so geht's... Code here can probably be repurposed elsewhere for data cleaning or XML extraction I guess.

## How it works

A single script (`extract_texts.py`) streams through a MediaWiki XML dump and for each page:

1. **Strips wikitext markup** — templates, tables, links, HTML tags, references, categories, etc.
2. **Removes editorial apparatus** — section numbers, brackets, parentheses, quotation marks, HTML entities
3. **Normalizes orthography** — ligatures (æ→ae), diacritics, j→i, v→u, medieval→classical spelling, abbreviation expansion, lowercase
4. **Filters out junk** — redirects, TOC/index pages, short stubs, English-language content
5. **Classifies** texts by period (classical / post-classical) and type (prose / poetry) using author, title, genre, date, and category metadata
6. **Writes** each text as a plain `.txt` file into categorized subdirectories

## Usage

```
python3 extract_texts.py <dump.xml> -o output/
```

Options:
- `--ns 0 104` — namespaces to include (default: 0=Main, 104=Pagina)
- `--no-normalize` — skip orthography normalization
- `--min-length N` — minimum character count after cleaning (default: 200)
- `-v` — verbose/debug logging

## Output structure

```
output/
  classical/prose/
  classical/poetry/
  post_classical/prose/
  post_classical/poetry/
  uncategorized/
```

## Getting a dump

Download the latest XML dump from:
https://dumps.wikimedia.org/lawikisource/

You want the `pages-articles.xml.bz2` file. Decompress it before running.

## Legacy code

The `_legacy/` directory contains the original HTTP-scraping pipeline (~3000 lines across multiple modules). It was replaced by the current single-file XML dump approach.
