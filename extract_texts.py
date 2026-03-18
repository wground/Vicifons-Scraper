#!/usr/bin/env python3
"""
Vicifons Text Extractor

Extracts clean Latin texts directly from a Vicifons (la.wikisource.org) XML dump.
Strips all wikitext markup, applies orthography normalization, and outputs plain
text files organized by period (classical/post-classical) and type (prose/poetry).

Processes:
  - ns=0   (main content): Latin works and their chapters
  - ns=104 (Pagina):       Page-level transcriptions from scanned books

Filters out:
  - Redirects
  - Table-of-contents / index pages
  - Pages with too little text after cleanup
  - Non-Latin content (templates, categories, talk pages, etc.)

Output structure:
  output/
    classical/prose/
    classical/poetry/
    post_classical/prose/
    post_classical/poetry/
    uncategorized/

Author: Willow Groundwater-Schuldt
"""

import argparse
import html
import logging
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from xml.etree.ElementTree import iterparse

# ---------------------------------------------------------------------------
# Wikitext → plain text
# ---------------------------------------------------------------------------

_RE_COMMENT = re.compile(r'<!--.*?-->', re.DOTALL)
_RE_NOINCLUDE = re.compile(r'<noinclude>.*?</noinclude>', re.DOTALL)
_RE_INCLUDEONLY_TAGS = re.compile(r'</?includeonly>', re.DOTALL)
_RE_REF = re.compile(r'<ref[^>]*>.*?</ref>', re.DOTALL)
_RE_REF_SELF = re.compile(r'<ref[^>]*/>')
_RE_MATH = re.compile(r'<math[^>]*>.*?</math>', re.DOTALL)
_RE_POEM_TAGS = re.compile(r'</?poem>')
_RE_HTML_TAG = re.compile(r'<[^>]+>')
_RE_NESTED_TEMPLATE = re.compile(r'\{\{(?:[^{}]|\{[^{]|\}[^}])*\}\}')
_RE_WIKI_TABLE = re.compile(r'\{\|.*?\|\}', re.DOTALL)
_RE_PIPED_LINK = re.compile(r'\[\[[^\]]*?\|([^\]]*?)\]\]')
_RE_PLAIN_LINK = re.compile(r'\[\[([^\]]*?)\]\]')
_RE_EXTERNAL_LINK = re.compile(r'\[https?://[^\s\]]*\s*([^\]]*)\]')
_RE_EXTERNAL_URL = re.compile(r'\[https?://[^\]]*\]')
_RE_BARE_URL = re.compile(r'https?://\S+')
_RE_HEADING = re.compile(r'^={2,6}\s*(.*?)\s*={2,6}\s*$', re.MULTILINE)
_RE_BOLD_ITALIC = re.compile(r"'{2,5}")
_RE_CATEGORY = re.compile(r'\[\[(Categoria|Category):[^\]]*\]\]', re.IGNORECASE)
_RE_INTERWIKI = re.compile(r'\[\[[a-z]{2,3}:[^\]]*\]\]')
_RE_MAGIC_WORD = re.compile(r'__[A-Z]+__')
_RE_PAGEQUALITY = re.compile(r'\{\{(pagequality|PaginaQuality)[^}]*\}\}', re.IGNORECASE)
_RE_MULTI_NEWLINE = re.compile(r'\n{3,}')
_RE_TRAILING_SPACE = re.compile(r'[ \t]+$', re.MULTILINE)
_RE_MULTI_SPACE = re.compile(r'  +')
_RE_WIKI_LIST = re.compile(r'^\s*[*#:;]+\s*', re.MULTILINE)
_RE_IMAGO = re.compile(r'Imago:[^\s|}\]]*', re.IGNORECASE)
_RE_HORIZ_RULE = re.compile(r'^-{4,}\s*$', re.MULTILINE)
_RE_SQUARE_BRACKETS = re.compile(r'\[[^\]]*\]')
_RE_PARENTHESIZED = re.compile(r'\([^)]*\)')
_RE_ROMAN_HEADER = re.compile(r'^[ivxlcdm]+\.\s*$', re.MULTILINE)
_RE_STANDALONE_ROMAN = re.compile(r'^\s*[IVXLCDM]+\.?\s*$', re.MULTILINE)
_RE_STANDALONE_NUM = re.compile(r'^\s*\d{1,4}\.?\s*$', re.MULTILINE)


def strip_wikitext(text: str) -> str:
    """Convert wikitext to plain text, keeping only prose content."""
    if not text:
        return ''

    # Remove HTML comments
    text = _RE_COMMENT.sub('', text)

    # Remove <noinclude> blocks (Pagina namespace metadata)
    text = _RE_NOINCLUDE.sub('', text)
    text = _RE_INCLUDEONLY_TAGS.sub('', text)

    # Remove references
    text = _RE_REF.sub('', text)
    text = _RE_REF_SELF.sub('', text)

    # Remove math
    text = _RE_MATH.sub('', text)

    # Remove ProofreadPage quality markers
    text = _RE_PAGEQUALITY.sub('', text)

    # Remove templates (iteratively to handle nesting)
    for _ in range(10):
        new_text = _RE_NESTED_TEMPLATE.sub('', text)
        if new_text == text:
            break
        text = new_text

    # Remove wiki tables
    text = _RE_WIKI_TABLE.sub('', text)

    # Remove category and interwiki links
    text = _RE_CATEGORY.sub('', text)
    text = _RE_INTERWIKI.sub('', text)

    # Remove image references (Imago:filename.png)
    text = _RE_IMAGO.sub('', text)

    # Convert wiki links to display text
    text = _RE_PIPED_LINK.sub(r'\1', text)
    text = _RE_PLAIN_LINK.sub(r'\1', text)

    # Remove external links (keep label if present)
    text = _RE_EXTERNAL_LINK.sub(r'\1', text)
    text = _RE_EXTERNAL_URL.sub('', text)
    text = _RE_BARE_URL.sub('', text)

    # Remove poem tags (keep content)
    text = _RE_POEM_TAGS.sub('', text)

    # Remove headings markup (keep heading text)
    text = _RE_HEADING.sub(r'\1', text)

    # Remove bold/italic markup
    text = _RE_BOLD_ITALIC.sub('', text)

    # Remove magic words
    text = _RE_MAGIC_WORD.sub('', text)

    # Remove wiki list markers (* # : ;)
    text = _RE_WIKI_LIST.sub('', text)

    # Remove horizontal rules
    text = _RE_HORIZ_RULE.sub('', text)

    # Remove remaining HTML tags (br → newline first)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = _RE_HTML_TAG.sub('', text)

    # Decode HTML entities (&nbsp; → space, &amp; → &, etc.)
    text = html.unescape(text)
    # Non-breaking spaces → regular spaces
    text = text.replace('\u00a0', ' ')

    # Remove ALL square bracket content (editorial annotations: [nec], [...], [ut], etc.)
    text = _RE_SQUARE_BRACKETS.sub('', text)

    # Remove ALL parenthesized content (editorial references, numbers, etc.)
    text = _RE_PARENTHESIZED.sub('', text)

    # Remove quotation marks — all types (editorial additions, not original Latin)
    # Also remove &, #, @, *, ~, ^, `, | — none are part of Latin text
    text = re.sub(r'[«»\u201c\u201d\u201e\u2018\u2019\u201a\u2039\u203a"\'&\#@\*~\^`|]', '', text)

    # Remove inline section numbers at start of lines (e.g. "1. dictis", "42. haec")
    text = re.sub(r'^\d{1,4}\.\s+', '', text, flags=re.MULTILINE)

    # Remove standalone Roman numeral lines (section headers from wiki headings)
    text = _RE_STANDALONE_ROMAN.sub('', text)
    text = _RE_ROMAN_HEADER.sub('', text)

    # Remove standalone number lines
    text = _RE_STANDALONE_NUM.sub('', text)

    # Remove any remaining Arabic numerals (all are editorial — Latin text has none)
    text = re.sub(r'\d+', '', text)

    # Remove stray punctuation left behind by removals (orphaned periods, colons, etc.)
    text = re.sub(r'^\s*[.,;:]\s*$', '', text, flags=re.MULTILINE)

    # Clean up whitespace
    text = _RE_TRAILING_SPACE.sub('', text)
    text = _RE_MULTI_SPACE.sub(' ', text)
    text = _RE_MULTI_NEWLINE.sub('\n\n', text)
    text = text.strip()

    return text


# ---------------------------------------------------------------------------
# Metadata extraction (from raw wikitext, before stripping)
# ---------------------------------------------------------------------------

_RE_TITULUS = re.compile(
    r'\{\{[Tt]itulus\d?\s*\n(.*?)\}\}', re.DOTALL)
_RE_FIELD = re.compile(r'\|(\w+)\s*=\s*([^\n|{}]+)')


def extract_metadata(raw: str) -> dict:
    """Extract metadata from the {{Titulus}} template in raw wikitext."""
    meta = {}
    m = _RE_TITULUS.search(raw)
    if m:
        for field_match in _RE_FIELD.finditer(m.group(1)):
            key = field_match.group(1).strip()
            val = field_match.group(2).strip()
            if val and not val.startswith('<!--'):
                meta[key] = val
    # Also extract categories
    cats = re.findall(r'\[\[Categoria:([^\]]+)\]\]', raw)
    if cats:
        meta['_categories'] = cats
    return meta


# ---------------------------------------------------------------------------
# Classification: period and type
# ---------------------------------------------------------------------------

# Author → (period, default_type)
# "classical" = antiquity through ~500 AD; "post_classical" = after that
# default_type is the author's primary genre; individual works can override
_AUTHOR_MAP = {
    # === CLASSICAL POETRY ===
    'publius vergilius maro': ('classical', 'poetry'),
    'quintus horatius flaccus': ('classical', 'poetry'),
    'publius ovidius naso': ('classical', 'poetry'),
    'gaius valerius catullus': ('classical', 'poetry'),
    'titus lucretius carus': ('classical', 'poetry'),
    'albius tibullus': ('classical', 'poetry'),
    'sextus propertius': ('classical', 'poetry'),
    'decimus iunius iuvenalis': ('classical', 'poetry'),
    'aulus persius flaccus': ('classical', 'poetry'),
    'marcus annaeus lucanus': ('classical', 'poetry'),
    'publius papinius statius': ('classical', 'poetry'),
    'gaius valerius flaccus': ('classical', 'poetry'),
    'tiberius catius silius italicus': ('classical', 'poetry'),
    'silius italicus': ('classical', 'poetry'),
    'marcus valerius martialis': ('classical', 'poetry'),
    'phaedrus': ('classical', 'poetry'),
    'lucius accius': ('classical', 'poetry'),
    'quintus ennius': ('classical', 'poetry'),
    'titus maccius plautus': ('classical', 'poetry'),
    'publius terentius afer': ('classical', 'poetry'),
    'terentius': ('classical', 'poetry'),
    'plautus': ('classical', 'poetry'),
    'lucius annaeus seneca minor': ('classical', 'poetry'),  # tragedies
    'marcus manilius': ('classical', 'poetry'),
    'decimus magnus ausonius': ('classical', 'poetry'),
    'claudianus': ('classical', 'poetry'),
    'claudius claudianus': ('classical', 'poetry'),
    'avianus': ('classical', 'poetry'),
    'flavius avianus': ('classical', 'poetry'),
    'grattius': ('classical', 'poetry'),
    'marcus aurelius nemesianus': ('classical', 'poetry'),
    'titus calpurnius siculus': ('classical', 'poetry'),
    'gaius lucilius': ('classical', 'poetry'),

    # === CLASSICAL PROSE ===
    'marcus tullius cicero': ('classical', 'prose'),
    'gaius iulius caesar': ('classical', 'prose'),
    'gaius sallustius crispus': ('classical', 'prose'),
    'titus livius': ('classical', 'prose'),
    'publius cornelius tacitus': ('classical', 'prose'),
    'gaius plinius secundus': ('classical', 'prose'),     # Pliny the Elder
    'gaius plinius caecilius secundus': ('classical', 'prose'),  # Pliny the Younger
    'lucius annaeus seneca': ('classical', 'prose'),
    'marcus fabius quintilianus': ('classical', 'prose'),
    'gaius suetonius tranquillus': ('classical', 'prose'),
    'aulus gellius': ('classical', 'prose'),
    'lucius apuleius': ('classical', 'prose'),
    'apuleius': ('classical', 'prose'),
    'titus petronius arbiter': ('classical', 'prose'),
    'petronius': ('classical', 'prose'),
    'marcus vitruvius pollio': ('classical', 'prose'),
    'vitruvius': ('classical', 'prose'),
    'marcus porcius cato': ('classical', 'prose'),
    'cato': ('classical', 'prose'),
    'marcus terentius varro': ('classical', 'prose'),
    'lucius iunius moderatus columella': ('classical', 'prose'),
    'aulus cornelius celsus': ('classical', 'prose'),
    'cornelius nepos': ('classical', 'prose'),
    'quintus curtius rufus': ('classical', 'prose'),
    'sextus iulius frontinus': ('classical', 'prose'),
    'valerius maximus': ('classical', 'prose'),
    'aesopus': ('classical', 'prose'),
    'vegetius': ('classical', 'prose'),
    'flavius vegetius renatus': ('classical', 'prose'),
    'sextus pompeius festus': ('classical', 'prose'),
    'macrobius': ('classical', 'prose'),
    'ambrosius theodosius macrobius': ('classical', 'prose'),
    'servius': ('classical', 'prose'),
    'aulus hirtius': ('classical', 'prose'),
    'gaius asinius pollio': ('classical', 'prose'),
    'marcus iunianus iustinus': ('classical', 'prose'),
    'eutropius': ('classical', 'prose'),
    'aurelius victor': ('classical', 'prose'),
    'ammianus marcellinus': ('classical', 'prose'),
    'pomponius mela': ('classical', 'prose'),
    'solinus': ('classical', 'prose'),
    'gaius iulius solinus': ('classical', 'prose'),
    'hyginus': ('classical', 'prose'),
    'hyginus mythographus': ('classical', 'prose'),

    # === LATE ANTIQUE / EARLY CHURCH (classical period, mostly prose) ===
    'quintus septimius florens tertullianus': ('classical', 'prose'),
    'lucius caecilius firmianus lactantius': ('classical', 'prose'),
    'lactantius': ('classical', 'prose'),

    # === POST-CLASSICAL PROSE (church fathers, medieval, renaissance) ===
    'aurelius augustinus': ('post_classical', 'prose'),
    'sophronius eusebius hieronymus': ('post_classical', 'prose'),
    'ambrosius': ('post_classical', 'prose'),
    'gregorius magnus': ('post_classical', 'prose'),
    'thomas aquinas': ('post_classical', 'prose'),
    'beda venerabilis': ('post_classical', 'prose'),
    'isidorus hispalensis': ('post_classical', 'prose'),
    'anicius manlius torquatus severinus boethius': ('post_classical', 'prose'),
    'boethius': ('post_classical', 'prose'),
    'anselmus laudunensis': ('post_classical', 'prose'),
    'bernardus claraevallensis': ('post_classical', 'prose'),
    'hilarius pictaviensis': ('post_classical', 'prose'),
    'rabanus maurus': ('post_classical', 'prose'),
    'zeno veronensis': ('post_classical', 'prose'),
    'leo i papa': ('post_classical', 'prose'),
    'desiderius erasmus roterodamus': ('post_classical', 'prose'),
    'henricus kramer': ('post_classical', 'prose'),
    'athanasius kircher': ('post_classical', 'prose'),
    'ioannes baptista a vico': ('post_classical', 'prose'),
    'maturinus corderius': ('post_classical', 'prose'),
    'iacobus paulus migne': ('post_classical', 'prose'),
    'johannes romberch': ('post_classical', 'prose'),
    'petrus abaelardus': ('post_classical', 'prose'),
    'ioannes duns scotus': ('post_classical', 'prose'),
    'albertus magnus': ('post_classical', 'prose'),
    'nicolaus copernicus': ('post_classical', 'prose'),
    'franciscus bacon': ('post_classical', 'prose'),
    'renatus cartesius': ('post_classical', 'prose'),
    'baruch spinoza': ('post_classical', 'prose'),
    'thomas morus': ('post_classical', 'prose'),
    'ioannes calvinus': ('post_classical', 'prose'),
    'martinus lutherus': ('post_classical', 'prose'),

    # Popes (all post-classical prose)
    'pius x': ('post_classical', 'prose'),
    'pius ix': ('post_classical', 'prose'),
    'pius xi': ('post_classical', 'prose'),
    'pius ii': ('post_classical', 'prose'),
    'leo xiii': ('post_classical', 'prose'),
    'benedictus xv': ('post_classical', 'prose'),
    'ioannes xxiii': ('post_classical', 'prose'),
    'paulus vi': ('post_classical', 'prose'),

    # === CLASSICAL POETRY (additional) ===
    'sextus aurelius propertius': ('classical', 'poetry'),
    'propertius': ('classical', 'poetry'),
    'luxorius': ('classical', 'poetry'),

    # === CLASSICAL PROSE (additional) ===
    'arnobius': ('classical', 'prose'),
    'procopius': ('classical', 'prose'),

    # === POST-CLASSICAL PROSE (additional) ===
    'mahometus': ('post_classical', 'prose'),
    'nicolaus machiavellus': ('post_classical', 'prose'),
    'giovanni boccaccio': ('post_classical', 'prose'),
    'sebastianus castellio': ('post_classical', 'prose'),
    'petrus ravennas': ('post_classical', 'prose'),
    'hermannus quodam judaeus': ('post_classical', 'prose'),
    'aimoinus floriacensis': ('post_classical', 'prose'),
    'gregorius turonensis': ('post_classical', 'prose'),
    'galfridus monemutensis': ('post_classical', 'prose'),
    'nicolaus cusanus': ('post_classical', 'prose'),
    'flaccus albinus alcuinus': ('post_classical', 'prose'),
    'gilbertus foliot': ('post_classical', 'prose'),
    'francesco petrarca': ('post_classical', 'prose'),
    'birgitta de suecia': ('post_classical', 'prose'),
    'guillelmus de ockham': ('post_classical', 'prose'),
    'paulus diaconus': ('post_classical', 'prose'),
    'hugo de sancto victore': ('post_classical', 'prose'),
    'gregorius ix': ('post_classical', 'prose'),
    'theophanes': ('post_classical', 'prose'),
    'polydorus vergilius': ('post_classical', 'prose'),
    'charles françois lhomond': ('post_classical', 'prose'),
    'george l. bennett': ('post_classical', 'prose'),

    # === POST-CLASSICAL POETRY ===
    'aurelius prudentius clemens': ('post_classical', 'poetry'),
    'prudentius': ('post_classical', 'poetry'),
    'venantius fortunatus': ('post_classical', 'poetry'),
    'giovanni pascoli': ('post_classical', 'poetry'),
    'iovianus pontanus': ('post_classical', 'poetry'),
    'ludovico ariosto': ('post_classical', 'poetry'),
    'franciscus petrarca': ('post_classical', 'poetry'),
    'gualterus de castiglione': ('post_classical', 'poetry'),
    'theophilus folengo': ('post_classical', 'poetry'),
    'carolus orff': ('post_classical', 'poetry'),
    'theodorus korsch': ('post_classical', 'poetry'),
    'caecilius statius': ('classical', 'poetry'),
    'pylades brixianus': ('post_classical', 'poetry'),
}

# Title patterns → (period, type) for well-known works
# Matched against the base title (before any /Liber subpage)
_TITLE_OVERRIDES = {
    # Classical poetry
    'aeneis': ('classical', 'poetry'),
    'bucolica': ('classical', 'poetry'),
    'georgica': ('classical', 'poetry'),
    'metamorphoses': ('classical', 'poetry'),
    'metamorphoses (ovidius)': ('classical', 'poetry'),
    'amores (ovidius)': ('classical', 'poetry'),
    'ars amatoria': ('classical', 'poetry'),
    'fasti': ('classical', 'poetry'),
    'tristia': ('classical', 'poetry'),
    'epistulae ex ponto': ('classical', 'poetry'),
    'heroides': ('classical', 'poetry'),
    'de rerum natura': ('classical', 'poetry'),
    'pharsalia': ('classical', 'poetry'),
    'bellum civile (lucanus)': ('classical', 'poetry'),
    'thebais': ('classical', 'poetry'),
    'achilleis': ('classical', 'poetry'),
    'silvae': ('classical', 'poetry'),
    'argonautica (valerius flaccus)': ('classical', 'poetry'),
    'punica': ('classical', 'poetry'),
    'saturae (iuvenalis)': ('classical', 'poetry'),
    'satirae (horatius)': ('classical', 'poetry'),
    'carmina (horatius)': ('classical', 'poetry'),
    'carmina (catullus)': ('classical', 'poetry'),
    'epigrammata': ('classical', 'poetry'),
    'fabulae (phaedrus)': ('classical', 'poetry'),
    'astronomica': ('classical', 'poetry'),

    # Classical prose
    'ab urbe condita': ('classical', 'prose'),
    'commentarii de bello gallico': ('classical', 'prose'),
    'commentarii de bello civili': ('classical', 'prose'),
    'de bello gallico': ('classical', 'prose'),
    'de bello civili': ('classical', 'prose'),
    'noctes atticae': ('classical', 'prose'),
    'naturalis historia': ('classical', 'prose'),
    'annales (tacitus)': ('classical', 'prose'),
    'historiae (tacitus)': ('classical', 'prose'),
    'de origine et situ germanorum (germania)': ('classical', 'prose'),
    'dialogus de oratoribus': ('classical', 'prose'),
    'agricola (tacitus)': ('classical', 'prose'),
    'bellum catilinae': ('classical', 'prose'),
    'bellum iugurthinum': ('classical', 'prose'),
    'de vita caesarum': ('classical', 'prose'),
    'de institutione oratoria': ('classical', 'prose'),
    'epistulae morales ad lucilium': ('classical', 'prose'),
    'de beneficiis': ('classical', 'prose'),
    'de clementia': ('classical', 'prose'),
    'de ira': ('classical', 'prose'),
    'de brevitate vitae': ('classical', 'prose'),
    'de providentia': ('classical', 'prose'),
    'de constantia sapientis': ('classical', 'prose'),
    'de tranquillitate animi': ('classical', 'prose'),
    'de vita beata': ('classical', 'prose'),
    'naturales quaestiones': ('classical', 'prose'),
    'de officiis': ('classical', 'prose'),
    'de re publica': ('classical', 'prose'),
    'de legibus': ('classical', 'prose'),
    'de finibus bonorum et malorum': ('classical', 'prose'),
    'tusculanae disputationes': ('classical', 'prose'),
    'de natura deorum': ('classical', 'prose'),
    'de divinatione': ('classical', 'prose'),
    'de amicitia': ('classical', 'prose'),
    'de senectute': ('classical', 'prose'),
    'brutus': ('classical', 'prose'),
    'orator': ('classical', 'prose'),
    'de oratore': ('classical', 'prose'),
    'in catilinam': ('classical', 'prose'),
    'pro archia poeta': ('classical', 'prose'),
    'pro milone': ('classical', 'prose'),
    'philippicae': ('classical', 'prose'),
    'epistulae ad atticum': ('classical', 'prose'),
    'epistulae ad familiares': ('classical', 'prose'),
    'de architectura': ('classical', 'prose'),
    'de re rustica': ('classical', 'prose'),
    'res gestae divi augusti': ('classical', 'prose'),
    'satyricon': ('classical', 'prose'),
    'asinus aureus': ('classical', 'prose'),
    'metamorphoses (apuleius)': ('classical', 'prose'),
    'epitome rerum romanarum': ('classical', 'prose'),
    'strategemata': ('classical', 'prose'),
    'facta et dicta memorabilia': ('classical', 'prose'),

    # Post-classical prose
    'confessiones': ('post_classical', 'prose'),
    'de civitate dei': ('post_classical', 'prose'),
    'consolatio philosophiae': ('post_classical', 'prose'),
    'de consolatione philosophiae': ('post_classical', 'prose'),
    'summa theologiae': ('post_classical', 'prose'),
    'summa theologica': ('post_classical', 'prose'),
    'summa contra gentiles': ('post_classical', 'prose'),
    'etymologiae': ('post_classical', 'prose'),
    'historia ecclesiastica gentis anglorum': ('post_classical', 'prose'),
    'vulgata': ('post_classical', 'prose'),
    'biblia sacra vulgata': ('post_classical', 'prose'),
    'malleus maleficarum': ('post_classical', 'prose'),
    'regula sancti benedicti': ('post_classical', 'prose'),
    'magna carta': ('post_classical', 'prose'),
    'meditationes de prima philosophia': ('post_classical', 'prose'),
    'utopia': ('post_classical', 'prose'),
    'de revolutionibus orbium coelestium': ('post_classical', 'prose'),
    'institutio christianae religionis': ('post_classical', 'prose'),
    'novum organum': ('post_classical', 'prose'),
    'patrologia latina': ('post_classical', 'prose'),
}

# Poetry genre indicators (case-insensitive matching on Genera field)
_POETRY_GENRES = {
    'carmina', 'carmen', 'elegiae', 'elegia', 'epigrammata', 'epigramma',
    'hendecasyllabi', 'sapphici', 'elegi', 'hymni', 'hymnus',
    'odes', 'odae', 'bucolica', 'georgica', 'eclogae', 'ecloga',
    'idyllia', 'silvae', 'epyllion', 'versus', 'comoediae', 'tragoediae',
}

_PROSE_GENRES = {
    'theologia', 'philosophia', 'historia', 'epistulae', 'orationes',
    'documenta historica', 'ars rhetorica', 'hagiographiae', 'encyclicae',
    'bullae', 'acta conciliorum ecclesiae', 'institutiones', 'annales',
    'annales et chronica', 'colloquia ad discendam linguam latinam',
    'textus ad discendam linguam latinam', 'geographia', 'biographia',
    'encyclopaediae', 'memoria artificialis', 'constitutiones apostolicae',
    'allocutiones', 'motu proprio', 'litterae apostolicae',
    'epistulae paparum', 'res rusticae', 'medicina',
    'precationes christianae',
}

_POETRY_CATEGORIES = {
    'hendecasyllabi', 'sapphici', 'elegi',
}

_CLASSICAL_CATEGORIES = {
    'latinitas romana',
}

_MEDIEVAL_CATEGORIES = {
    'latinitas mediaevalis',
    'patrologiae cursus completus',
}


_ROMAN_MAP = {'i': 1, 'ii': 2, 'iii': 3, 'iv': 4, 'v': 5,
              'vi': 6, 'vii': 7, 'viii': 8, 'ix': 9, 'x': 10,
              'xi': 11, 'xii': 12, 'xiii': 13, 'xiv': 14, 'xv': 15,
              'xvi': 16, 'xvii': 17, 'xviii': 18, 'xix': 19, 'xx': 20,
              'xxi': 21}

# Latin ordinal words → century number
_LATIN_ORDINALS = {
    'primum': 1, 'secundum': 2, 'tertium': 3, 'quartum': 4, 'quintum': 5,
    'sextum': 6, 'septimum': 7, 'octavum': 8, 'nonum': 9, 'decimum': 10,
}


def _parse_year(annus: str):
    """Try to extract a numeric year from the Annus field. Negative = BC."""
    if not annus:
        return None
    s = annus.lower().strip()

    # Skip non-date values
    if s in ('foenix', ''):
        return None

    # "45 a.ch.n" / "44 a.c.n." / "2 a.C.n. - 8 a.C.n." → BC
    m = re.search(r'(\d{1,4})\s*a\.?\s*c(?:h|\.)\s*n?', s)
    if m:
        return -int(m.group(1))

    # "I sec. a.Ch." / "IV s. a.Ch.n" / "Saeculum II a.Ch.n."
    m = re.search(r'([ivxl]+)\s*(?:sec|s)\.\s*a\.?\s*ch', s)
    if m:
        century = _ROMAN_MAP.get(m.group(1))
        if century:
            return -(century * 100)
    m = re.search(r'saeculum\s+([ivxl]+)\s+a\.?\s*ch', s)
    if m:
        century = _ROMAN_MAP.get(m.group(1))
        if century:
            return -(century * 100)
    # "II a.Ch.n." standalone
    m = re.search(r'^([ivxl]+)\s+a\.?\s*ch', s)
    if m:
        century = _ROMAN_MAP.get(m.group(1))
        if century:
            return -(century * 100)

    # "anno domini 98" / "c. 49 AD" / "159-170 AD" / "8 p.C.n."
    m = re.search(r'(\d{1,4})\s*(ad|a\.?\s*d\.?|p\.?\s*c(?:h|\.)\s*n?)', s)
    if m:
        return int(m.group(1))

    # "saeculo I" / "saec. II" / "saeculum XVI" / "saculo XII"
    m = re.search(r'sa[eé]?c(?:ulo|ulum)?\s*\.?\s*([ivxl]+)', s)
    if m:
        century = _ROMAN_MAP.get(m.group(1))
        if century:
            return century * 100

    # "V saec." / "XII s." / "I sec."
    m = re.search(r'([ivxl]+)\s*(?:saec|s\.|sec\.)', s)
    if m:
        century = _ROMAN_MAP.get(m.group(1))
        if century:
            return century * 100

    # "s. VI" / "s. XII"
    m = re.search(r's\.\s*([ivxl]+)', s)
    if m:
        century = _ROMAN_MAP.get(m.group(1))
        if century:
            return century * 100

    # "saeculum sextum" (Latin ordinal words)
    for word, num in _LATIN_ORDINALS.items():
        if word in s:
            return num * 100

    # Bare 4-digit year: "1215", "1620"
    m = re.search(r'\b(\d{4})\b', s)
    if m:
        return int(m.group(1))

    # Bare 2-3 digit year likely AD
    m = re.search(r'\b(\d{2,3})\b', s)
    if m:
        return int(m.group(1))

    return None


# Pagina title keywords → (period, type) for classifying ns=104 pages
_PAGINA_KEYWORDS = [
    # Classical poetry
    (re.compile(r'horace|horatius|horatii', re.I), ('classical', 'poetry')),
    (re.compile(r'virgil|vergil|aeneid|bucolica|georgica', re.I), ('classical', 'poetry')),
    (re.compile(r'ovid|metamorphos', re.I), ('classical', 'poetry')),
    (re.compile(r'catull', re.I), ('classical', 'poetry')),
    (re.compile(r'lucret', re.I), ('classical', 'poetry')),
    (re.compile(r'tibull', re.I), ('classical', 'poetry')),
    (re.compile(r'properti', re.I), ('classical', 'poetry')),
    (re.compile(r'juvenal|iuvenal', re.I), ('classical', 'poetry')),
    (re.compile(r'martial|epigramma', re.I), ('classical', 'poetry')),
    (re.compile(r'lucan|pharsal', re.I), ('classical', 'poetry')),
    (re.compile(r'stati|thebai|achillei|silvae', re.I), ('classical', 'poetry')),
    (re.compile(r'plaut|terent', re.I), ('classical', 'poetry')),
    (re.compile(r'phaedrus|phaedri', re.I), ('classical', 'poetry')),
    # Classical prose
    (re.compile(r'cicero|cicéron|tulli', re.I), ('classical', 'prose')),
    (re.compile(r'caesar|caesaris|bello gallico|bello civili', re.I), ('classical', 'prose')),
    (re.compile(r'livius|tite-live|ab urbe condita', re.I), ('classical', 'prose')),
    (re.compile(r'tacit|annale|histori.*tacit|germani.*tacit', re.I), ('classical', 'prose')),
    (re.compile(r'plin|naturalis historia', re.I), ('classical', 'prose')),
    (re.compile(r'senec', re.I), ('classical', 'prose')),
    (re.compile(r'quintilian|institutione oratoria', re.I), ('classical', 'prose')),
    (re.compile(r'sueton|suetonius', re.I), ('classical', 'prose')),
    (re.compile(r'gellius|noctes atticae', re.I), ('classical', 'prose')),
    (re.compile(r'apulei|asinus aureus', re.I), ('classical', 'prose')),
    (re.compile(r'sallust', re.I), ('classical', 'prose')),
    (re.compile(r'nepos|cornelii nepotis', re.I), ('classical', 'prose')),
    (re.compile(r'curtius rufus', re.I), ('classical', 'prose')),
    (re.compile(r'vitruvius|architectura', re.I), ('classical', 'prose')),
    (re.compile(r'varro|varron', re.I), ('classical', 'prose')),
    (re.compile(r'aesop', re.I), ('classical', 'prose')),
    # Post-classical poetry (before prose catch-alls so "Hymni Ecclesiae" → poetry)
    (re.compile(r'hymni|hymnus|hymn', re.I), ('post_classical', 'poetry')),
    (re.compile(r'eclogae latinae', re.I), ('post_classical', 'poetry')),
    (re.compile(r'prudenti', re.I), ('post_classical', 'poetry')),
    # Post-classical prose
    (re.compile(r'augustin', re.I), ('post_classical', 'prose')),
    (re.compile(r'hieronym|jerome', re.I), ('post_classical', 'prose')),
    (re.compile(r'ambros', re.I), ('post_classical', 'prose')),
    (re.compile(r'thomas aquin|thomae aquin|summa', re.I), ('post_classical', 'prose')),
    (re.compile(r'boethi|consolatione', re.I), ('post_classical', 'prose')),
    (re.compile(r'isidor', re.I), ('post_classical', 'prose')),
    (re.compile(r'cartesius|descartes|meditatione', re.I), ('post_classical', 'prose')),
    (re.compile(r'erasmus|erasmi', re.I), ('post_classical', 'prose')),
    (re.compile(r'luther', re.I), ('post_classical', 'prose')),
    (re.compile(r'calvin', re.I), ('post_classical', 'prose')),
    (re.compile(r'copernicus|copernici', re.I), ('post_classical', 'prose')),
    (re.compile(r'galileo|galilei', re.I), ('post_classical', 'prose')),
    (re.compile(r'spinoza', re.I), ('post_classical', 'prose')),
    (re.compile(r'newton', re.I), ('post_classical', 'prose')),
    (re.compile(r'patrolog', re.I), ('post_classical', 'prose')),
    (re.compile(r'ecclesi|concili|pontif|encyclica|allocution', re.I), ('post_classical', 'prose')),
    (re.compile(r'biblia|vulgat|testamentum', re.I), ('post_classical', 'prose')),
    (re.compile(r'gallia christiana', re.I), ('post_classical', 'prose')),
    (re.compile(r'corpus iuris|digest|pandect|institutiones', re.I), ('post_classical', 'prose')),
]


def _get_base_title(title: str) -> str:
    """Get the base title before any /Liber or /Caput subpage."""
    return title.split('/')[0].strip()


def classify(meta: dict, title: str = ''):
    """
    Classify a text by period and type based on metadata, author, and title.

    Priority: title override > author map > categories > Genera field > Annus date.

    Returns:
        (period, text_type) where period is 'classical'/'post_classical'/'unknown'
        and text_type is 'prose'/'poetry'/'unknown'.
    """
    period = 'unknown'
    text_type = 'unknown'

    # --- 1. Title-based override (highest priority, handles subpages) ---
    base_title = _get_base_title(title).lower()
    if base_title in _TITLE_OVERRIDES:
        return _TITLE_OVERRIDES[base_title]

    # --- 2. Author-based classification ---
    author = meta.get('Scriptor', '').strip().lower()
    if author in _AUTHOR_MAP:
        period, text_type = _AUTHOR_MAP[author]
        # Still check Genera for type override (author might write both)
        genera_raw = meta.get('Genera', '').lower()
        genera_parts = [g.strip() for g in genera_raw.split(',')]
        for g in genera_parts:
            if g in _POETRY_GENRES or any(pg in g for pg in _POETRY_GENRES):
                text_type = 'poetry'
                break
            if g in _PROSE_GENRES or any(pg in g for pg in _PROSE_GENRES):
                text_type = 'prose'
                break
        return period, text_type

    # --- 3. Categories ---
    cats = [c.lower() for c in meta.get('_categories', [])]

    if any(c in _CLASSICAL_CATEGORIES for c in cats):
        period = 'classical'
    elif any(c in _MEDIEVAL_CATEGORIES for c in cats):
        period = 'post_classical'

    # --- 4. Annus field for period ---
    if period == 'unknown':
        year = _parse_year(meta.get('Annus', ''))
        if year is not None:
            if year < 500:
                period = 'classical'
            else:
                period = 'post_classical'

    # --- Type ---
    text_type = 'unknown'
    genera_raw = meta.get('Genera', '').lower()
    genera_parts = [g.strip() for g in genera_raw.split(',')]

    # Check genre field
    for g in genera_parts:
        if g in _POETRY_GENRES or any(pg in g for pg in _POETRY_GENRES):
            text_type = 'poetry'
            break
        if g in _PROSE_GENRES or any(pg in g for pg in _PROSE_GENRES):
            text_type = 'prose'
            break

    # Check categories if genre didn't resolve it
    if text_type == 'unknown':
        if any(c in _POETRY_CATEGORIES for c in cats):
            text_type = 'poetry'

    # Drama defaults to poetry for training purposes
    if any('comoedia' in g or 'tragoedia' in g for g in genera_parts):
        text_type = 'poetry'

    # --- 5. Pagina title keyword matching (for ns=104 pages with no metadata) ---
    if period == 'unknown':
        for pattern, (p, t) in _PAGINA_KEYWORDS:
            if pattern.search(title):
                period = p
                if text_type == 'unknown':
                    text_type = t
                break

    return period, text_type


# ---------------------------------------------------------------------------
# Orthography normalization
# ---------------------------------------------------------------------------

_LIGATURE_MAP = str.maketrans({
    'æ': 'ae', 'Æ': 'Ae',
    'œ': 'oe', 'Œ': 'Oe',
    '\u0153': 'oe', '\u0152': 'Oe',
})

_MEDIEVAL_VARIANTS = [
    (re.compile(r'\bmichi\b', re.I), 'mihi'),
    (re.compile(r'\btichi\b', re.I), 'tibi'),
    (re.compile(r'\bsichi\b', re.I), 'sibi'),
    (re.compile(r'\bnichil\b', re.I), 'nihil'),
    (re.compile(r'\bnichilo\b', re.I), 'nihilo'),
    (re.compile(r'\bpulcer\b', re.I), 'pulcher'),
    (re.compile(r'\btercius\b', re.I), 'tertius'),
    (re.compile(r'\bnegocium\b', re.I), 'negotium'),
    (re.compile(r'\bprecium\b', re.I), 'pretium'),
    (re.compile(r'\bspacium\b', re.I), 'spatium'),
    (re.compile(r'\bjusticia\b', re.I), 'iustitia'),
    (re.compile(r'\bdampnum\b', re.I), 'damnum'),
    (re.compile(r'\bcolumpna\b', re.I), 'columna'),
    (re.compile(r'\bsolempnis\b', re.I), 'sollemnis'),
    (re.compile(r'\bsompnus\b', re.I), 'somnus'),
    (re.compile(r'\babere\b', re.I), 'habere'),
    (re.compile(r'\bomines\b', re.I), 'homines'),
    (re.compile(r'\bonor\b', re.I), 'honor'),
    (re.compile(r'\bumanus\b', re.I), 'humanus'),
]

_J_PATTERNS = [
    (re.compile(r'\bjam\b', re.I), 'iam'),
    (re.compile(r'\bmajor\b', re.I), 'maior'),
    (re.compile(r'\bmajores\b', re.I), 'maiores'),
    (re.compile(r'\bmajorem\b', re.I), 'maiorem'),
    (re.compile(r'\bmajus\b', re.I), 'maius'),
    (re.compile(r'\bpejor\b', re.I), 'peior'),
    (re.compile(r'\bjulius\b', re.I), 'Iulius'),
    (re.compile(r'\bjulia\b', re.I), 'Iulia'),
    (re.compile(r'\bjustus\b', re.I), 'iustus'),
    (re.compile(r'\bjustitia\b', re.I), 'iustitia'),
    (re.compile(r'\bj([aeiou])', re.I), r'i\1'),
    (re.compile(r'([aeiou])j([aeiou])', re.I), r'\1i\2'),
    (re.compile(r'([aeiou])j\b', re.I), r'\1i'),
]

# Abbreviation expansion patterns (applied before lowercasing)
# Order matters: longer/more specific patterns first to avoid partial matches
_ABBREVIATIONS = [
    # Multi-letter abbreviations first (most specific)
    (re.compile(r'\bet\s+c\.', re.I), 'et cetera'),
    (re.compile(r'\bi\.\s*e\.', re.I), 'id est'),
    (re.compile(r'\be\.\s*g\.', re.I), 'exempli gratia'),
    (re.compile(r'\bviz\.'), 'uidelicet'),
    (re.compile(r'\bscil\.'), 'scilicet'),
    (re.compile(r'\bcf\.'), 'confer'),
    (re.compile(r'\bib\.'), 'ibidem'),
    (re.compile(r'\bid\.'), 'idem'),
    # Religious / titles (case-sensitive, before lowercasing)
    (re.compile(r'\bD\.\s*N\.'), 'Dominus Noster'),
    (re.compile(r'\bI\.\s*H\.\s*S\.'), 'Iesus Hominum Saluator'),
    (re.compile(r'\bD\.\s*M\.'), 'Dis Manibus'),
    (re.compile(r'\bR\.\s*I\.\s*P\.'), 'Requiescat In Pace'),
    (re.compile(r'\bA\.\s*D\.'), 'Anno Domini'),
    (re.compile(r'\bImp\.'), 'Imperator'),
    (re.compile(r'\bCaes\.'), 'Caesar'),
    (re.compile(r'\bCons\.'), 'Consul'),
    (re.compile(r'\bPont\.'), 'Pontifex'),
    # Praenomina (case-sensitive)
    (re.compile(r'\bSer\.\s*'), 'Seruius '),
    (re.compile(r'\bSex\.\s*'), 'Sextus '),
    (re.compile(r'\bTib?\.\s*'), 'Tiberius '),
    (re.compile(r'\bAp\.\s*'), 'Appius '),
    (re.compile(r'\bCn\.\s*'), 'Gnaeus '),
    (re.compile(r'\bM\.\s*'), 'Marcus '),
    (re.compile(r'\bL\.\s*'), 'Lucius '),
    (re.compile(r'\bC\.\s*'), 'Gaius '),
    (re.compile(r'\bP\.\s*'), 'Publius '),
    (re.compile(r'\bQ\.\s*'), 'Quintus '),
    (re.compile(r'\bA\.\s*'), 'Aulus '),
    (re.compile(r'\bD\.\s*'), 'Decimus '),
    (re.compile(r'\bT\.\s*'), 'Titus '),
    # Common contractions (case-insensitive)
    (re.compile(r'\bxpts\b', re.I), 'Christus'),
    (re.compile(r'\bihs\b', re.I), 'Iesus'),
    (re.compile(r'\bdns\b', re.I), 'dominus'),
    (re.compile(r'\bsps\b', re.I), 'spiritus'),
    (re.compile(r'\bscs\b', re.I), 'sanctus'),
    # Simple abbreviations last (most ambiguous — single letter + period)
    (re.compile(r'\bq\.\s*'), 'que '),
]


def normalize_orthography(text: str) -> str:
    """Normalize Latin orthography for consistent LLM training data.

    Handles: ligatures, diacritics, abbreviation expansion, medieval variants,
    j→i, v→u, lowercase.
    """
    text = text.translate(_LIGATURE_MAP)

    # Remove diacritics
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if not unicodedata.combining(c))

    # Expand abbreviations (before lowercasing — some patterns are case-sensitive)
    for pattern, repl in _ABBREVIATIONS:
        text = pattern.sub(repl, text)

    # Lowercase everything
    text = text.lower()

    # Medieval → classical spelling
    for pattern, repl in _MEDIEVAL_VARIANTS:
        text = pattern.sub(repl, text)

    # J → I
    for pattern, repl in _J_PATTERNS:
        text = pattern.sub(repl, text)

    # V → U (all instances — classical Latin had one letter for both)
    text = text.replace('v', 'u')

    return text


# ---------------------------------------------------------------------------
# Content quality filters
# ---------------------------------------------------------------------------

MIN_CONTENT_LENGTH = 200

_SKIP_TITLE_PATTERNS = [
    re.compile(r'^Pagina prima$'),
    re.compile(r'^Categoriae$'),
    re.compile(r'^Vicifons:'),
    re.compile(r'^Formula:'),
    re.compile(r'^Scriptor:'),
]


_ENGLISH_WORDS = frozenset({
    'the', 'of', 'and', 'is', 'was', 'are', 'were', 'this', 'that', 'with',
    'from', 'have', 'has', 'for', 'but', 'not', 'which', 'their', 'they',
    'been', 'would', 'could', 'should', 'will', 'shall', 'may', 'might',
    'than', 'then', 'when', 'where', 'there', 'here', 'these', 'those',
    'into', 'upon', 'about', 'between', 'through', 'after', 'before',
    'also', 'only', 'other', 'such', 'very', 'some', 'any', 'each',
    'being', 'having', 'does', 'did', 'do', 'an', 'or', 'by', 'to',
    'he', 'she', 'it', 'we', 'you', 'his', 'her', 'its', 'our', 'your',
    'who', 'what', 'how', 'why', 'can', 'if', 'so', 'no', 'yes',
    'page', 'pages', 'contents', 'preface', 'chapter', 'section',
    'edition', 'edited', 'translated', 'see', 'vol', 'note', 'notes',
})

# Words that exist in both English and Latin — don't count these
_AMBIGUOUS_WORDS = frozenset({
    'in', 'et', 'at', 'a', 'as', 'me', 'be', 'us',
})


def is_english(text: str, threshold: float = 0.05) -> bool:
    """Detect English text by checking for high-frequency English function words."""
    words = re.findall(r'[a-z]+', text.lower())
    if len(words) < 20:
        return False
    english_count = sum(1 for w in words if w in _ENGLISH_WORDS and w not in _AMBIGUOUS_WORDS)
    return english_count / len(words) > threshold


def is_toc_page(text: str) -> bool:
    """Detect table-of-contents / index pages with no real prose."""
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    words = text.split()

    # Very short pages with no real sentences
    if len(words) < 30:
        # Check if it looks like a list rather than prose
        if len(lines) >= 3:
            avg_line_len = sum(len(l) for l in lines) / len(lines)
            if avg_line_len < 60:
                return True
        return True

    # Pages with lots of hyphens/dashes (OCR artifacts, visual separators)
    hyphen_lines = sum(1 for l in lines if re.search(r'-{3,}|\.{4,}', l))
    if len(lines) > 0 and hyphen_lines / len(lines) > 0.2:
        return True

    # Explicit INDEX heading + mostly "Liber/Caput" lines (works post-normalization too)
    liber_lines = sum(1 for l in lines if re.match(
        r'^(index|liber\s|caput\s|pars\s|uide\s|appendix|INDEX|Liber\s|Caput\s|Pars\s|Vide\s|Appendix)',
        l, re.I))
    if liber_lines > len(lines) * 0.4:
        return True

    # High density of structural words (case-insensitive for post-normalization)
    structural = sum(1 for w in words if re.match(
        r'^[IVXLCDMivxlcdm]+\.?$|^[A-Za-z]\.?$|^[Ll]iber$|^[Cc]aput$|^INDEX$|^index$'
        r'|^[Pp]ars$|^[Aa]ppendix$|^[Pp]raefatio$|^[Vv]ide$|^[Uu]ide$|^etiam$',
        w))
    if structural > len(words) * 0.35:
        return True

    return False


def should_skip_title(title: str) -> bool:
    """Check if a page title indicates non-text content."""
    for pattern in _SKIP_TITLE_PATTERNS:
        if pattern.match(title):
            return True
    return False


# ---------------------------------------------------------------------------
# Safe filename generation
# ---------------------------------------------------------------------------

_RE_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def safe_filename(title: str) -> str:
    """Convert a page title to a safe filename."""
    name = title.replace('/', '_')
    name = _RE_UNSAFE_CHARS.sub('_', name)
    if len(name) > 200:
        name = name[:200]
    return name + '.txt'


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------

def run_extraction(xml_path: str, output_dir: str, namespaces: set,
                   normalize: bool = True, min_length: int = 200) -> dict:
    """
    Extract and clean Latin texts from a Vicifons XML dump.

    Texts are categorized into subdirectories by period and type when
    metadata is available. Pagina (ns=104) pages go into uncategorized/
    since they typically lack metadata.
    """
    logger = logging.getLogger('extract')
    out = Path(output_dir)

    # Create directory structure
    for period in ('classical', 'post_classical'):
        for text_type in ('prose', 'poetry'):
            (out / period / text_type).mkdir(parents=True, exist_ok=True)
    (out / 'uncategorized').mkdir(parents=True, exist_ok=True)

    ns_uri = '{http://www.mediawiki.org/xml/export-0.11/}'
    stats = {
        'pages_seen': 0,
        'redirects_skipped': 0,
        'short_skipped': 0,
        'toc_skipped': 0,
        'english_skipped': 0,
        'title_skipped': 0,
        'extracted': 0,
        'by_category': {
            'classical/prose': 0, 'classical/poetry': 0,
            'post_classical/prose': 0, 'post_classical/poetry': 0,
            'uncategorized': 0,
        },
        'namespaces': {ns: 0 for ns in namespaces},
    }

    # Count total pages for progress (quick pre-scan by file size estimate)
    xml_size = os.path.getsize(xml_path)
    # Rough estimate: ~55k pages per 711MB dump
    est_total_pages = max(1, int(xml_size / 711_000_000 * 55_000))

    is_tty = sys.stderr.isatty()
    start_time = time.monotonic()

    logger.info(f'Parsing {xml_path} ({xml_size / 1_000_000:.0f} MB) ...')
    logger.info(f'Including namespaces: {namespaces}')
    logger.info(f'Estimated ~{est_total_pages} total pages')

    all_pages_processed = 0  # includes pages in other namespaces

    for _, elem in iterparse(xml_path, events=('end',)):
        if elem.tag != ns_uri + 'page':
            continue

        all_pages_processed += 1

        page_ns = elem.findtext(ns_uri + 'ns') or ''
        title = elem.findtext(ns_uri + 'title') or ''
        text_elem = elem.find(f'.//{ns_uri}text')
        raw = text_elem.text if text_elem is not None and text_elem.text else ''

        elem.clear()

        if page_ns not in namespaces:
            continue

        stats['pages_seen'] += 1

        # Progress display
        if all_pages_processed % 1000 == 0:
            elapsed = time.monotonic() - start_time
            pct = min(99.9, all_pages_processed / est_total_pages * 100)
            rate = all_pages_processed / elapsed if elapsed > 0 else 0
            remaining = (est_total_pages - all_pages_processed) / rate if rate > 0 else 0
            eta_min, eta_sec = divmod(int(remaining), 60)

            progress = (
                f'\r  [{pct:5.1f}%] '
                f'{all_pages_processed}/{est_total_pages} pages | '
                f'{stats["extracted"]} extracted | '
                f'ETA {eta_min}m{eta_sec:02d}s'
            )
            if is_tty:
                sys.stderr.write(progress)
                sys.stderr.flush()
            elif all_pages_processed % 5000 == 0:
                logger.info(progress.strip())

        # Skip redirects
        if raw.lstrip().upper().startswith('#REDIRECT'):
            stats['redirects_skipped'] += 1
            continue

        # Skip certain titles
        if should_skip_title(title):
            stats['title_skipped'] += 1
            continue

        # Extract metadata BEFORE stripping (templates get removed)
        meta = extract_metadata(raw)

        # Strip wikitext markup
        cleaned = strip_wikitext(raw)

        # Apply orthography normalization
        if normalize:
            cleaned = normalize_orthography(cleaned)

        # Skip if too short after cleanup
        if len(cleaned) < min_length:
            stats['short_skipped'] += 1
            continue

        # Skip TOC / index pages
        if is_toc_page(cleaned):
            stats['toc_skipped'] += 1
            continue

        # Skip non-Latin (English) content
        if is_english(cleaned):
            stats['english_skipped'] += 1
            continue

        # Classify by period and type
        period, text_type = classify(meta, title)

        # Determine output subdirectory
        if period != 'unknown' and text_type != 'unknown':
            subdir = f'{period}/{text_type}'
        elif period != 'unknown':
            # Known period but unknown type → default to prose
            subdir = f'{period}/prose'
        else:
            subdir = 'uncategorized'

        cat_key = subdir if subdir in stats['by_category'] else 'uncategorized'
        stats['by_category'][cat_key] = stats['by_category'].get(cat_key, 0) + 1

        # Write output file
        target_dir = out / subdir
        target_dir.mkdir(parents=True, exist_ok=True)
        filename = safe_filename(title)
        filepath = target_dir / filename

        if filepath.exists():
            stem = filepath.stem
            for i in range(2, 100):
                filepath = target_dir / f'{stem}_{i}.txt'
                if not filepath.exists():
                    break

        filepath.write_text(cleaned, encoding='utf-8')
        stats['extracted'] += 1
        stats['namespaces'][page_ns] = stats['namespaces'].get(page_ns, 0) + 1

    if is_tty:
        sys.stderr.write('\r' + ' ' * 80 + '\r')  # clear progress line
        sys.stderr.flush()

    elapsed = time.monotonic() - start_time
    mins, secs = divmod(int(elapsed), 60)
    logger.info(f'Done. Extracted {stats["extracted"]} texts in {mins}m{secs:02d}s.')
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Extract clean Latin texts from a Vicifons XML dump.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s dump.xml -o texts/
  %(prog)s dump.xml -o texts/ --no-normalize
  %(prog)s dump.xml -o texts/ --ns 0        # Only main namespace
  %(prog)s dump.xml -o texts/ --ns 0 104    # Main + Pagina (default)
""")

    parser.add_argument('xml', help='Path to the Vicifons XML dump file')
    parser.add_argument('-o', '--output', default='output',
                        help='Output directory (default: output)')
    parser.add_argument('--ns', nargs='+', default=['0', '104'],
                        help='Namespace numbers to include (default: 0 104)')
    parser.add_argument('--no-normalize', action='store_true',
                        help='Skip orthography normalization')
    parser.add_argument('--min-length', type=int, default=MIN_CONTENT_LENGTH,
                        help=f'Minimum content length after cleanup (default: {MIN_CONTENT_LENGTH})')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable debug logging')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    xml_path = Path(args.xml)
    if not xml_path.exists():
        print(f'Error: XML file not found: {xml_path}', file=sys.stderr)
        sys.exit(1)

    stats = run_extraction(
        str(xml_path),
        args.output,
        set(args.ns),
        normalize=not args.no_normalize,
        min_length=args.min_length,
    )

    print()
    print('=' * 50)
    print('EXTRACTION COMPLETE')
    print('=' * 50)
    print(f'Pages scanned:      {stats["pages_seen"]}')
    print(f'Texts extracted:     {stats["extracted"]}')
    print(f'Redirects skipped:   {stats["redirects_skipped"]}')
    print(f'Short pages skipped: {stats["short_skipped"]}')
    print(f'TOC pages skipped:   {stats["toc_skipped"]}')
    print(f'English skipped:     {stats["english_skipped"]}')
    print(f'Title-filtered:      {stats["title_skipped"]}')
    print()
    print('By category:')
    for cat, count in sorted(stats['by_category'].items()):
        if count > 0:
            print(f'  {cat}: {count}')
    print()
    print('By namespace:')
    for ns_id, count in sorted(stats['namespaces'].items()):
        label = {'0': 'Main', '104': 'Pagina'}.get(ns_id, f'ns={ns_id}')
        print(f'  {label}: {count} texts')
    print('=' * 50)


if __name__ == '__main__':
    main()
