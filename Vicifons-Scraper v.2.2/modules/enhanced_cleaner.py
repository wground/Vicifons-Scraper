#!/usr/bin/env python3
"""
Enhanced Text Cleaner Module

Improved text cleaner with:
- Better export metadata removal
- Classical/Post-Classical categorization
- Prose/Poetry categorization using Vicifons categories
- Comprehensive abbreviation expansion
- LLM training data optimization
"""

import asyncio
import aiofiles
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
import unicodedata
from datetime import datetime

from .utils import clean_filename, ProgressTracker, validate_latin_text, detect_text_type, calculate_text_stats
from .orthography import OrthographyStandardizer

class EnhancedTextCleaner:
    """Enhanced text cleaner with categorization and better cleaning."""
    
    def __init__(self, config: Dict):
        """Initialize enhanced text cleaner."""
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Output directories with categorization
        self.output_dir = Path(config['output_dir']) / "cleaned_texts"
        self.setup_categorized_directories()
        
        # Initialize orthography standardizer
        self.orthography = OrthographyStandardizer()
        
        # Enhanced cleaning patterns
        self.export_metadata_patterns = self._compile_export_patterns()
        self.content_cleaning_patterns = self._compile_content_patterns()
        
        # Period classification (up to 15th century as requested)
        self.period_keywords = self._setup_period_classification()
        
        # Genre classification from Vicifons categories
        self.genre_categories = self._setup_genre_classification()
        
        # Enhanced abbreviation expansion
        self.abbreviation_patterns = self._setup_abbreviation_expansion()
        
        self.logger.info(f"Initialized EnhancedTextCleaner with categorized output")
    
    def setup_categorized_directories(self):
        """Create categorized directory structure."""
        categories = [
            "classical/prose",
            "classical/poetry", 
            "post_classical/prose",
            "post_classical/poetry",
            "unknown/uncategorized"
        ]
        
        for category in categories:
            (self.output_dir / category).mkdir(parents=True, exist_ok=True)
    
    def _compile_export_patterns(self) -> List[re.Pattern]:
        """Compile patterns to remove export metadata and non-Latin content."""
        patterns = [
            # Wikisource export metadata (more comprehensive)
            r'about this digital edition.*?$',
            r'this e-book comes from.*?$',
            r'exported from wikisource.*?$', 
            r'generated.*?wikisource.*?$',
            r'this multilingual digital library.*?$',
            r'we distribute our books for free.*?$',
            r'creative commons.*?license.*?$',
            r'gnu fdl.*?$',
            r'wikisource is constantly looking.*?$',
            r'during the realization of this book.*?$',
            r'you can report them at this page.*?$',
            r'the following users contributed.*?$',
            
            # User contribution lists  
            r'accurimbono.*?shakespearefan00.*?$',
            r'nicknack009.*?pmaopus.*?$',
            r'\*\s*\*\s*\*.*?↑\s*$',
            
            # URLs and links
            r'https?://[^\s]+',
            r'www\.[^\s]+',
            r'\b[a-zA-Z0-9]+\.[a-zA-Z]{2,}\b',
            
            # Technical metadata
            r'ws-export.*?$',
            r'source:.*?$',
            r'category:.*?$',
            r'categoria:.*?$',
            
            # HTML remnants and formatting
            r'<[^>]+>',
            r'\[\[Category:[^\]]+\]\]',
            r'\[\[Categoria:[^\]]+\]\]',
            r'\{\{[^}]*\}\}',
            
            # Navigation and structural elements
            r'← .*? →',
            r'◄ .*? ►',
            r'previous.*?next',
            r'precedens.*?sequens',
            
            # Page numbers and references
            r'p\.\s*\d+',
            r'page\s*\d+',
            r'pagina\s*\d+',
            r'\b\d+\s*\|\s*\d+\b',  # Page ranges like "123 | 456"
            
            # Edition information
            r'ed\.\s*\w+',
            r'editio\s*\w+',
            r'editor\s*\w+',
            
            # Copyright and licensing
            r'copyright.*?$',
            r'©.*?$',
            r'all rights reserved.*?$',
            
            # Modern language references and translations (ENHANCED)
            r'\b[a-z]{2,3}:\s*[а-яё\u0100-\u017F\u1E00-\u1EFF]+.*?$',  # Language codes followed by non-Latin text
            r'\b(en|fr|de|it|es|pl|ru|be|ua|cz|sk|hu|ro|bg|hr|sl|mk|sr|bs|lv|lt|et|fi|sv|no|da|nl|pt|ca|eu|gl|mt|cy|ga|gd|br|is|fo|kl):\s*.*?$',
            r'\bbagarodziśa\b.*?$',  # Specific Belarusian text
            r'\bbogurodzica\b.*?$',   # Specific Polish text
            r'\b[а-яёіуэ]+\b.*?$',    # Cyrillic script
            r'\b[αβγδεζηθικλμνξοπρστυφχψω]+\b.*?$',  # Greek script
            
            # Editorial and academic references (ENHANCED)
            r'i-vi,?\s*comm\.?\s*f\.?\s*r\.?\s*d\.?\s*goodyear,?\s*cambridge.*?$',
            r'comm\.?\s*[a-z]\.\s*[a-z]\.\s*[a-z]\.\s*[a-z]+,?\s*cambridge.*?$',
            r'cf\.?\s*[a-z]\.\s*[a-z]\.\s*[a-z]+.*?cambridge.*?$',
            r'vide\s+[a-z]+.*?cambridge.*?$',
            r'see\s+[a-z]+.*?cambridge.*?$',
            r'cambridge\s*,\s*\d*\s*,?\s*\.?$',
            r'oxford\s+classical\s+texts.*?$',
            r'teubner.*?edition.*?$',
            r'loeb\s+classical\s+library.*?$',
        ]
        
        return [re.compile(p, re.IGNORECASE | re.MULTILINE) for p in patterns]
    
    def _compile_content_patterns(self) -> List[Tuple[re.Pattern, str]]:
        """Compile patterns for content cleaning."""
        patterns = [
            # Remove wiki table syntax and templates (ENHANCED)
            (re.compile(r'\{\|[^}]*\|\}', re.DOTALL), ''),  # Wiki tables {| ... |}
            (re.compile(r'\{\|.*?\|\}', re.DOTALL), ''),     # Alternative pattern
            (re.compile(r'notoc', re.IGNORECASE), ''),       # TOC suppression
            (re.compile(r'__NOTOC__', re.IGNORECASE), ''),   # Magic word
            (re.compile(r'style="[^"]*"'), ''),              # Inline styles
            (re.compile(r'align\s*=\s*\w+'), ''),            # Alignment attributes
            (re.compile(r'width\s*=\s*\d+%?'), ''),          # Width attributes
            (re.compile(r'id\s*=\s*\w+'), ''),               # ID attributes
            (re.compile(r'class\s*=\s*"[^"]*"'), ''),        # CSS classes
            (re.compile(r'margin:\s*[^;]+;'), ''),           # CSS margins
            (re.compile(r'margin-top:\s*[^;]+;'), ''),       # CSS margin-top
            
            # Wiki table row/cell separators
            (re.compile(r'\|-+'), ''),                       # Table row separators
            (re.compile(r'\|+'), ''),                        # Table cell separators
            (re.compile(r'!\s*'), ''),                       # Table headers
            
            # Remove remaining HTML and markup
            (re.compile(r'<[^>]+>'), ''),
            (re.compile(r'\{\{[^}]*\}\}'), ''),
            (re.compile(r'\[\[[^\]]*\]\]'), ''),
            
            # Remove editorial annotations
            (re.compile(r'\[(?:Note|Nota):[^\]]+\]'), ''),
            (re.compile(r'\[(?:Editor|Ed\.):[^\]]+\]'), ''),
            (re.compile(r'\{(?:ref|nota)[^}]*\}'), ''),
            
            # Clean up formatting artifacts
            (re.compile(r"'''([^']+)'''"), r'\1'),  # Bold
            (re.compile(r"''([^']+)''"), r'\1'),    # Italic
            (re.compile(r'__([^_]+)__'), r'\1'),    # Underline
            
            # Remove reference markers
            (re.compile(r'\[\d+\]'), ''),  # [1], [2], etc.
            (re.compile(r'\(\d+\)'), ''),  # (1), (2), etc.
            (re.compile(r'\*+'), ''),      # Asterisks
            
            # Clean whitespace and formatting
            (re.compile(r'\s+'), ' '),     # Multiple spaces
            (re.compile(r'\n\s*\n\s*\n+'), '\n\n'),  # Multiple newlines
            (re.compile(r'^\s+|\s+$', re.MULTILINE), ''),  # Leading/trailing spaces
            
            # Remove remaining technical elements
            (re.compile(r'thumb\|[^|]*\|'), ''),
            (re.compile(r'frame\|[^|]*\|'), ''),
            (re.compile(r'border\|[^|]*\|'), ''),
        ]
        
        return patterns
    
    def _setup_period_classification(self) -> Dict:
        """Set up period classification keywords (up to 15th century)."""
        return {
            'classical': {
                # Time periods
                'dates': range(-700, 600),  # 8th century BC to 6th century AD
                'keywords': {
                    'republic', 'respublica', 'consul', 'imperator', 'caesar',
                    'augustus', 'tiberius', 'nero', 'vespasianus', 'traianus',
                    'hadrianus', 'antoninus', 'marcus aurelius', 'severus',
                    'diocletianus', 'constantinus', 'roma', 'senatus',
                    'populus romanus', 'spqr', 'legatus', 'centurio'
                },
                'authors': {
                    'cicero', 'caesar', 'livius', 'tacitus', 'suetonius',
                    'vergilius', 'horatius', 'ovidius', 'catullus', 'propertius',
                    'tibullus', 'martialis', 'juvenalis', 'persius', 'lucanus',
                    'statius', 'silius italicus', 'valerius flaccus',
                    'quintilianus', 'plinius', 'gellius', 'apuleius',
                    'ammianus marcellinus'
                }
            },
            'post_classical': {
                # Late Antiquity and Medieval (up to 15th century)
                'dates': range(400, 1500),  # 5th to 15th century
                'keywords': {
                    'christus', 'deus', 'ecclesia', 'episcopus', 'presbyter',
                    'monachus', 'abbas', 'sanctus', 'beatus', 'martyria',
                    'evangelium', 'apostolus', 'papa', 'pontifex maximus',
                    'imperium', 'byzantium', 'constantinopolis', 'francia',
                    'karolus', 'otho', 'henricus', 'fredericus', 'scholasticus',
                    'universitas', 'magister', 'summa', 'quaestio', 'articulus'
                },
                'authors': {
                    'augustinus', 'hieronymus', 'ambrosius', 'gregorius',
                    'isidorus', 'beda', 'alcuinus', 'hrabanus maurus',
                    'thomas aquinas', 'boethius', 'cassiodorus', 'jordanes',
                    'paul the deacon', 'einhard', 'notker', 'hincmar',
                    'abelard', 'bernard', 'peter lombard', 'anselm',
                    'duns scotus', 'william of ockham'
                }
            }
        }
    
    def _setup_genre_classification(self) -> Dict:
        """Set up genre classification from Vicifons categories."""
        return {
            'poetry': {
                'categories': {
                    'categoria:carmina', 'categoria:poesis', 'categoria:versus',
                    'categoria:elegia', 'categoria:elegiae', 'categoria:epigrammata',
                    'categoria:satirae', 'categoria:satira', 'categoria:eclogae',
                    'categoria:georgica', 'categoria:bucolica', 'categoria:hymni',
                    'categoria:odes', 'categoria:epic', 'categoria:epica',
                    'categoria:lyrica', 'categoria:heroica'
                },
                'keywords': {
                    'carmen', 'cantus', 'versus', 'metrum', 'rhythmus',
                    'hexameter', 'pentameter', 'elegia', 'epigramma',
                    'hymnus', 'ode', 'ecloga', 'bucolicum', 'georgicum',
                    'epicus', 'heroicus', 'lyricus'
                },
                'titles': {
                    'aeneis', 'metamorphoses', 'georgica', 'eclogae',
                    'carmen', 'carmina', 'satirae', 'epodi', 'odes',
                    'elegia', 'elegiae', 'epigrammata', 'silvae'
                }
            },
            'prose': {
                'categories': {
                    'categoria:historia', 'categoria:historiae', 'categoria:oratio',
                    'categoria:orationes', 'categoria:epistolae', 'categoria:epistola',
                    'categoria:commentarii', 'categoria:annales', 'categoria:vitae',
                    'categoria:vita', 'categoria:biographia', 'categoria:philosophia',
                    'categoria:rhetorica', 'categoria:tractatus', 'categoria:dialogus',
                    'categoria:narratio', 'categoria:prosa'
                },
                'keywords': {
                    'historia', 'annales', 'oratio', 'epistola', 'commentarius',
                    'vita', 'biographia', 'tractatus', 'dialogus', 'sermo',
                    'narratio', 'descriptio', 'expositio', 'disputatio'
                },
                'titles': {
                    'historia', 'historiae', 'annales', 'commentarii',
                    'de ', 'ad ', 'oratio', 'orationes', 'epistolae',
                    'vita', 'vitae', 'dialogus', 'institutio'
                }
            }
        }
    
    def _setup_abbreviation_expansion(self) -> Dict[str, str]:
        """Set up comprehensive abbreviation expansion."""
        return {
            # Roman praenomina (most important for Latin texts)
            r'\bA\.\s*': 'Aulus ',
            r'\bAp\.\s*': 'Appius ',
            r'\bC\.\s*': 'Gaius ',
            r'\bCn\.\s*': 'Gnaeus ',
            r'\bD\.\s*': 'Decimus ',
            r'\bK\.\s*': 'Kaeso ',
            r'\bL\.\s*': 'Lucius ',
            r'\bM\.\s*': 'Marcus ',
            r'\bM\'\.\s*': 'Manius ',
            r'\bN\.\s*': 'Numerius ',
            r'\bP\.\s*': 'Publius ',
            r'\bQ\.\s*': 'Quintus ',
            r'\bS\.\s*': 'Spurius ',
            r'\bSer\.\s*': 'Servius ',
            r'\bSex\.\s*': 'Sextus ',
            r'\bSp\.\s*': 'Spurius ',
            r'\bT\.\s*': 'Titus ',
            r'\bTi\.\s*': 'Tiberius ',
            r'\bTib\.\s*': 'Tiberius ',
            
            # Common Latin abbreviations
            r'\bq\.\s*': 'que ',
            r'\bc\.\s*(?!aesar)': 'cum ',  # Avoid expanding C. before Caesar
            r'\bet\s+c\.': 'et cetera',
            r'\bi\.?\s*e\.': 'id est',
            r'\be\.?\s*g\.': 'exempli gratia',
            r'\bviz\.': 'videlicet',
            r'\bscil\.': 'scilicet',
            r'\bv\.(?!\s*[0-9])': 'vide ',  # Avoid Roman numerals
            r'\bcf\.': 'confer',
            r'\bib\.': 'ibidem',
            r'\bid\.': 'idem',
            r'\bloc\.\s*cit\.': 'loco citato',
            r'\bop\.\s*cit\.': 'opere citato',
            
            # Religious abbreviations  
            r'\bD\.?\s*M\.?\s*S?\.?': 'Dis Manibus Sacrum',
            r'\bD\.?\s*N\.?': 'Dominus Noster',
            r'\bI\.?\s*H\.?\s*S\.?': 'Iesus Hominum Salvator',
            r'\bR\.?\s*I\.?\s*P\.?': 'Requiescat In Pace',
            # r'\bA\.?\s*D\.?': 'Anno Domini',  # Disabled - not appropriate for classical Latin
            r'\bA\.?\s*M\.?': 'Ave Maria',
            
            # Titles and offices
            r'\bImp\.': 'Imperator',
            r'\bCaes\.': 'Caesar',
            r'\bAug\.': 'Augustus',
            r'\bCons\.': 'Consul',
            r'\bPont\.?\s*Max\.?': 'Pontifex Maximus',
            r'\bPont\.': 'Pontifex',
            r'\bTrib\.?\s*Pl\.?': 'Tribunus Plebis',
            r'\bLeg\.': 'Legatus',
            r'\bPraef\.': 'Praefectus',
            
            # Medieval abbreviations
            r'\bscs\.?\s*': 'sanctus ',
            r'\bsca\.?\s*': 'sancta ',
            r'\beps\.?\s*': 'episcopus ',
            r'\babb\.?\s*': 'abbas ',
            r'\bmgr\.?\s*': 'magister ',
            
            # Common contractions
            r'\bxps\.?': 'Christus',
            r'\bihs\.?': 'Iesus',
            r'\bdns\.?': 'dominus',
            r'\bsps\.?': 'spiritus',
        }
    
    def extract_header_metadata(self, content: str) -> Dict:
        """Extract pre-categorized metadata from file header."""
        lines = content.split('\n')
        metadata = {}
        
        # Look for our enhanced header format
        for line in lines:
            if line.startswith('Title: '):
                metadata['title'] = line.replace('Title: ', '').strip()
            elif line.startswith('Author: '):
                metadata['author'] = line.replace('Author: ', '').strip()
            elif line.startswith('Parent Work: '):
                metadata['parent_work'] = line.replace('Parent Work: ', '').strip()
            elif line.startswith('Period: '):
                metadata['period'] = line.replace('Period: ', '').strip()
            elif line.startswith('Work Type: '):
                metadata['work_type'] = line.replace('Work Type: ', '').strip()
            elif line.startswith('Completeness: '):
                metadata['completeness'] = line.replace('Completeness: ', '').strip()
            elif line.startswith('Priority: '):
                metadata['priority'] = line.replace('Priority: ', '').strip()
            elif line.startswith('Pre-categorized: '):
                metadata['pre_categorized'] = line.replace('Pre-categorized: ', '').strip()
            elif line.startswith('Content Type: '):
                metadata['content_type'] = line.replace('Content Type: ', '').strip()
            elif line.startswith('-' * 20):  # End of header
                break
        
        # Enhanced chapter handling: if it's a chapter, try to infer metadata from parent work
        if metadata.get('content_type') == 'chapter' or '/' in metadata.get('title', ''):
            metadata = self._enhance_chapter_metadata(metadata)
        
        return metadata
    
    def _enhance_chapter_metadata(self, metadata: Dict) -> Dict:
        """Enhance chapter metadata by inferring from parent work."""
        title = metadata.get('title', '')
        parent_work = metadata.get('parent_work', '')
        
        # Use parent work for classification if available
        classify_title = parent_work if parent_work else title
        
        # Enhanced work patterns for proper classification
        classical_work_patterns = {
            'aeneis': {'author': 'Publius Vergilius Maro', 'period': 'classical', 'type': 'poetry'},
            'georgica': {'author': 'Publius Vergilius Maro', 'period': 'classical', 'type': 'poetry'},
            'eclogae': {'author': 'Publius Vergilius Maro', 'period': 'classical', 'type': 'poetry'},
            'metamorphoses': {'author': 'Publius Ovidius Naso', 'period': 'classical', 'type': 'poetry'},
            'ars amatoria': {'author': 'Publius Ovidius Naso', 'period': 'classical', 'type': 'poetry'},
            'commentarii de bello gallico': {'author': 'Gaius Iulius Caesar', 'period': 'classical', 'type': 'prose'},
            'commentarii de bello civili': {'author': 'Gaius Iulius Caesar', 'period': 'classical', 'type': 'prose'},
            'noctes atticae': {'author': 'Aulus Gellius', 'period': 'classical', 'type': 'prose'},
            'naturalis historia': {'author': 'Gaius Plinius Secundus', 'period': 'classical', 'type': 'prose'},
            'institutio oratoria': {'author': 'Marcus Fabius Quintilianus', 'period': 'classical', 'type': 'prose'},
            'ab urbe condita': {'author': 'Titus Livius', 'period': 'classical', 'type': 'prose'},
            'annales': {'author': 'Publius Cornelius Tacitus', 'period': 'classical', 'type': 'prose'},
            'historiae': {'author': 'Publius Cornelius Tacitus', 'period': 'classical', 'type': 'prose'},
            'carmina': {'author': 'Quintus Horatius Flaccus', 'period': 'classical', 'type': 'poetry'},
            'de civitate dei': {'author': 'Augustinus Hipponensis', 'period': 'post_classical', 'type': 'prose'},
            'confessiones': {'author': 'Augustinus Hipponensis', 'period': 'post_classical', 'type': 'prose'},
            'summa theologiae': {'author': 'Thomas Aquinas', 'period': 'post_classical', 'type': 'prose'}
        }
        
        classify_lower = classify_title.lower()
        
        # Check for known work patterns
        for pattern, work_info in classical_work_patterns.items():
            if pattern in classify_lower:
                # Only override if not already set or if current value seems wrong
                if not metadata.get('author') or metadata.get('author') == title.split('/')[0]:
                    metadata['author'] = work_info['author']
                if not metadata.get('period') or metadata.get('period') == 'unknown':
                    metadata['period'] = work_info['period']
                if not metadata.get('work_type') or metadata.get('work_type') == 'unknown':
                    metadata['work_type'] = work_info['type']
                break
        
        return metadata
    
    def classify_period(self, content: str, metadata: Dict = None) -> str:
        """Classify text as classical or post-classical, using pre-categorization when available."""
        # First, try to extract header metadata
        header_metadata = self.extract_header_metadata(content)
        
        # Use pre-categorized period if available and valid
        pre_period = header_metadata.get('period', '').lower()
        if pre_period in ['classical', 'post_classical']:
            self.logger.debug(f"Using pre-categorized period: {pre_period}")
            return pre_period
        content_lower = content.lower()
        
        # Check metadata first if available
        if metadata:
            title = metadata.get('title', '').lower()
            author = metadata.get('author', '').lower()
            
            # Check for known classical authors
            for classical_author in self.period_keywords['classical']['authors']:
                if classical_author in author or classical_author in title:
                    return 'classical'
            
            # Check for known post-classical authors
            for post_classical_author in self.period_keywords['post_classical']['authors']:
                if post_classical_author in author or post_classical_author in title:
                    return 'post_classical'
        
        # Score based on keywords in content
        classical_score = 0
        post_classical_score = 0
        
        for keyword in self.period_keywords['classical']['keywords']:
            classical_score += content_lower.count(keyword.lower())
        
        for keyword in self.period_keywords['post_classical']['keywords']:
            post_classical_score += content_lower.count(keyword.lower())
        
        # Decide based on scores
        if classical_score > post_classical_score * 1.5:  # Bias toward classical
            return 'classical'
        elif post_classical_score > 0:
            return 'post_classical'
        else:
            return 'classical'  # Default for unknown texts
    
    def classify_genre(self, content: str, title: str = "", categories: Set[str] = None) -> str:
        """Classify text as prose or poetry, using pre-categorization when available."""
        # First, try to extract header metadata
        header_metadata = self.extract_header_metadata(content)
        
        # Use pre-categorized work type if available and valid
        pre_work_type = header_metadata.get('work_type', '').lower()
        if pre_work_type in ['prose', 'poetry']:
            self.logger.debug(f"Using pre-categorized work type: {pre_work_type}")
            return pre_work_type
        
        # Fallback to original classification logic
        # Check Vicifons categories first
        if categories:
            for category in categories:
                if category.lower() in self.genre_categories['poetry']['categories']:
                    return 'poetry'
                elif category.lower() in self.genre_categories['prose']['categories']:
                    return 'prose'
        
        # Use title from header metadata if available, otherwise provided title
        classify_title = header_metadata.get('title', title)
        title_lower = classify_title.lower()
        
        # Check title patterns
        for poetry_title in self.genre_categories['poetry']['titles']:
            if poetry_title in title_lower:
                return 'poetry'
        
        for prose_title in self.genre_categories['prose']['titles']:
            if prose_title in title_lower:
                return 'prose'
        
        # Analyze content structure (fallback)
        return detect_text_type(content)
    
    def remove_trailing_non_latin_content(self, content: str) -> str:
        """Remove trailing non-Latin content, language references, and editorial notes."""
        
        # First handle content that might be all on one line
        # Look for language codes followed by non-Latin text at the end
        language_code_patterns = [
            r'\s+[a-z]{2,3}:\s*[а-яё\u0100-\u017F\u1E00-\u1EFF]+.*?$',  # Language codes followed by non-Latin text
            r'\s+be:\s*багародзіца.*?$',  # Specific Belarusian
            r'\s+en:\s*bogurodzica.*?$',   # Specific English
            r'\s+pl:\s*bogurodzica.*?$',   # Specific Polish
            r'\s+comm\.?\s*[a-z]\.\s*[a-z]\.\s*[a-z].*?$',  # Editorial abbreviations
            r'\s+cambridge\s*,\s*\d*\s*,?\s*\.?$',
            r'\s+oxford\s+classical.*?$',
            r'\s+teubner.*edition.*?$',
            r'\s+loeb\s+classical.*?$',
        ]
        
        # Remove single-line trailing non-Latin content
        for pattern in language_code_patterns:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE)
        
        # Now handle line-by-line approach for multi-line content
        lines = content.split('\n')
        
        # Work backwards from the end to find where Latin content ends
        latin_end = len(lines)
        
        # Patterns that indicate non-Latin or metadata content
        non_latin_patterns = [
            r'^[a-z]{2,3}:\s*',  # Language codes (en:, fr:, pl:, etc.)
            r'[а-яё]',           # Cyrillic characters
            r'[αβγδεζηθικλμνξοπρστυφχψω]',  # Greek characters
            r'\bbagarodziśa\b',  # Specific non-Latin words
            r'\bbogurodzica\b',
            r'comm\.?\s*[a-z]\.\s*[a-z]\.\s*[a-z]',  # Editorial abbreviations
            r'cambridge\s*,\s*\d*\s*,?\s*\.?$',
            r'oxford\s+classical',
            r'teubner.*edition',
            r'loeb\s+classical',
            r'^[^\w]*$',  # Lines with only punctuation/whitespace
        ]
        
        compiled_patterns = [re.compile(p, re.IGNORECASE) for p in non_latin_patterns]
        
        # Scan backwards to find last legitimate Latin content
        for i in range(len(lines) - 1, -1, -1):
            line = lines[i].strip()
            
            # Skip empty lines
            if not line:
                continue
                
            # Check if this line contains non-Latin patterns
            is_non_latin = False
            for pattern in compiled_patterns:
                if pattern.search(line):
                    is_non_latin = True
                    self.logger.debug(f"Detected non-Latin trailing content: '{line}'")
                    break
            
            if is_non_latin:
                # This line and everything after should be removed
                latin_end = i
                continue
            
            # If we have substantial Latin content, this is probably the end
            if len(line) > 10 and re.search(r'[a-zA-Z]', line):
                # Check if it looks like Latin (has common Latin words/patterns)
                latin_indicators = ['et', 'in', 'ad', 'cum', 'de', 'est', 'sunt', 'qui', 'quae', 'sed', 'ut']
                if any(word in line.lower() for word in latin_indicators) or len(line) > 30:
                    break
        
        # Keep content only up to the Latin end
        cleaned_lines = lines[:latin_end]
        result = '\n'.join(cleaned_lines)
        
        # Final cleanup - remove any remaining language references
        result = re.sub(r'\s+[a-z]{2,3}:\s*\S+', '', result, flags=re.IGNORECASE)
        
        return result

    def aggressive_metadata_removal(self, content: str) -> str:
        """Aggressively remove all non-Latin metadata and export information."""
        lines = content.split('\n')
        
        # Find where the actual content starts (after the metadata header)
        content_start = 0
        for i, line in enumerate(lines):
            # Look for the separator line (dashes)
            if '---' in line or line.strip() == '':
                # Content likely starts after this
                if i + 1 < len(lines) and not lines[i + 1].strip():
                    content_start = i + 2  # Skip empty line too
                else:
                    content_start = i + 1
                break
            # If we see obvious metadata patterns, skip them
            if any(pattern in line.lower() for pattern in ['title:', 'source:', 'scraped:', 'type:', 'parent work:']):
                continue
        
        # Take content from where it actually starts
        if content_start < len(lines):
            content = '\n'.join(lines[content_start:])
        else:
            content = '\n'.join(lines)
        
        # Remove trailing non-Latin content (ENHANCED)
        content = self.remove_trailing_non_latin_content(content)
        
        # Pre-remove wiki table syntax that might be mixed with metadata
        wiki_table_patterns = [
            r'notoc\s*\{[^}]*\}[^}]*\}',  # notoc {| style="..." align=... width=... id=...
            r'\{\|\s*style="[^"]*"\s*align=[^}]*\}',  # {| style="..." align=... |}
            r'\{\|[^}]*margin[^}]*\}',     # {| ... margin ... |}
            r'__NOTOC__',                  # Magic word
        ]
        
        for pattern in wiki_table_patterns:
            content = re.sub(pattern, '', content, flags=re.IGNORECASE | re.DOTALL)
        
        # Apply export metadata patterns to clean up any remaining metadata
        for pattern in self.export_metadata_patterns:
            content = pattern.sub('', content)
        
        # Remove everything after export/metadata markers at the end
        end_markers = [
            r'about this digital edition.*$',
            r'exported from.*$',
            r'this e-book comes.*$',
            r'the following users.*$',
            r'accurimbono.*$',
            r'we distribute our books.*$',
            r'↑\s*$',
            r'\*\s*\*\s*\*.*$'
        ]
        
        for marker in end_markers:
            # Use DOTALL to match across lines
            content = re.sub(marker, '', content, flags=re.IGNORECASE | re.DOTALL)
        
        # Clean up remaining separators
        separators = [
            'exported from wikisource',
            'about this digital edition', 
            'accurimbono',
            '* * *'
        ]
        
        for separator in separators:
            if separator.lower() in content.lower():
                parts = re.split(re.escape(separator), content, flags=re.IGNORECASE)
                content = parts[0]  # Take part before the separator
                break
        
        return content.strip()
    
    def expand_abbreviations(self, content: str) -> str:
        """Expand Latin abbreviations selectively for LLM training purity."""
        # Only expand the most essential classical abbreviations
        # Avoid expanding Christian/Medieval abbreviations for classical texts
        essential_patterns = {
            # Roman praenomina (most important)
            r'\bA\.\s+': 'Aulus ',
            r'\bC\.\s+': 'Gaius ',
            r'\bL\.\s+': 'Lucius ',
            r'\bM\.\s+': 'Marcus ',
            r'\bP\.\s+': 'Publius ',
            r'\bQ\.\s+': 'Quintus ',
            r'\bT\.\s+': 'Titus ',
            r'\bTi\.\s+': 'Tiberius ',
            
            # Common Latin words
            r'\bq\.\s*': 'que ',
        }
        
        # Apply only essential expansions
        for pattern, replacement in essential_patterns.items():
            content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)
        
        return content
    
    def remove_chapter_headings(self, content: str) -> str:
        """Remove chapter/book headings and formatting that interfere with LLM training."""
        # First, handle wiki-style section headings (=== I ===, etc.)
        content = re.sub(r'^\s*=+\s*[IVXLCDM]+\s*=+\s*$', '', content, flags=re.MULTILINE)
        content = re.sub(r'^\s*=+\s*\d+\s*=+\s*$', '', content, flags=re.MULTILINE)
        content = re.sub(r'^\s*=+\s*[A-Z\s]{1,20}\s*=+\s*$', '', content, flags=re.MULTILINE)
        
        # Remove wiki table of contents and navigation elements
        content = re.sub(r'\{\|\s*align=center[^}]*\}[^{]*\|\}', '', content, flags=re.DOTALL)
        content = re.sub(r'Hic est indiculum.*?(?=\n[A-Z]|\n\n)', '', content, flags=re.DOTALL)
        
        lines = content.split('\n')
        cleaned_lines = []
        
        heading_patterns = [
            # Wiki-style headings that might remain
            r'^\s*=+.*?=+\s*$',
            
            # Roman numeral headings (standalone lines)
            r'^\s*[IVXLCDM]+\s*\.?\s*$',
            r'^\s*Liber\s+[IVXLCDM]+\s*\.?\s*$',
            r'^\s*Capitulum\s+[IVXLCDM]+\s*\.?\s*$',
            r'^\s*Chapter\s+[IVXLCDM]+\s*\.?\s*$',
            r'^\s*Book\s+[IVXLCDM]+\s*\.?\s*$',
            
            # Numbered headings
            r'^\s*\d+\s*\.?\s*$',
            r'^\s*Liber\s+\d+\s*\.?\s*$',
            r'^\s*Capitulum\s+\d+\s*\.?\s*$',
            
            # Common chapter markers that appear standalone
            r'^\s*INCIPIT\s*\.?\s*$',
            r'^\s*EXPLICIT\s*\.?\s*$',
            r'^\s*FINIS\s*\.?\s*$',
            r'^\s*ARGUMENTUM\s*\.?\s*$',
            
            # Annotation sections
            r'^\s*==\s*annotationes\s*==.*$',
            r'^\s*==\s*notes?\s*==.*$',
            
            # Language reference lines  
            r'^\s*[Ee][Nn]:\s*.*$',  # EN: English references
            r'^\s*[Ff][Rr]:\s*.*$',  # FR: French references
            r'^\s*[Ii][Tt]:\s*.*$',  # IT: Italian references
            r'^\s*[Nn][Ll]:\s*.*$',  # NL: Dutch references
            
            # Editorial references and citations
            r'.*tacitus,? annales.*comm.*goodyear.*',
            r'.*vide tacitus.*cambridge.*p\.?\s*-?\s*\d+.*',
            r'.*marcus tulli ciceronis.*',
            r'.*pro aut\..*vel\..*',
            
            # Very short lines that are likely headings (but preserve poetry)
            r'^\s*[A-Z\s]{1,8}\s*\.\s*$',  # Only remove very short all-caps lines ending with period
        ]
        
        compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in heading_patterns]
        
        for line in lines:
            stripped_line = line.strip()
            
            # Skip empty lines
            if not stripped_line:
                cleaned_lines.append(line)
                continue
            
            # Check if line matches any heading pattern
            is_heading = False
            for pattern in compiled_patterns:
                if pattern.match(stripped_line):
                    # POETRY-SAFE: Additional checks to preserve poetic content
                    if 'A-Z' in pattern.pattern:
                        # Check if it contains common Latin words - if so, keep it
                        latin_indicators = ['et', 'ad', 'in', 'de', 'cum', 'ex', 'ab', 'pro', 'per', 'est', 'sunt', 'non', 
                                          'sed', 'ut', 'que', 'vel', 'aut', 'nec', 'si', 'te', 'me', 'nos', 'vos']
                        if any(word in stripped_line.lower() for word in latin_indicators):
                            continue
                        
                        # Check if it looks like a poetic line (has vowels in Latin pattern)
                        if re.search(r'[aeiou].*[aeiou]', stripped_line.lower()) and len(stripped_line) > 2:
                            continue
                        
                        # Only remove if it's clearly structural (very short with period or obvious heading pattern)
                        if not (len(stripped_line) <= 3 or stripped_line.endswith('.') or 'liber' in stripped_line.lower() or 'capitulum' in stripped_line.lower()):
                            continue
                    
                    is_heading = True
                    self.logger.debug(f"Removed heading: '{stripped_line}'")
                    break
            
            if not is_heading:
                cleaned_lines.append(line)
        
        result = '\n'.join(cleaned_lines)
        
        # Clean up multiple consecutive blank lines that might result from heading removal
        result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
        
        return result
    
    def remove_modern_formatting(self, content: str) -> str:
        """Remove parentheses, brackets, and Arabic numerals that interfere with pure Latin text."""
        # POETRY-SAFE: Only remove obvious editorial content in parentheses/brackets
        # Remove parenthetical content that's clearly editorial (not short poetic content)
        content = re.sub(r'\([^)]{20,}\)', '', content)  # Only remove long parenthetical content
        content = re.sub(r'\[[^\]]{20,}\]', '', content)  # Only remove long bracketed content
        
        # Remove specific editorial patterns in parentheses/brackets
        content = re.sub(r'\([Pp]age? \d+\)', '', content)  # (page 123)
        content = re.sub(r'\([Ll]ine? \d+\)', '', content)  # (line 45)
        content = re.sub(r'\([Ee]d\.?\)', '', content)  # (ed.)
        content = re.sub(r'\([Cc]f\.? [^)]+\)', '', content)  # (cf. reference)
        content = re.sub(r'\[[Nn]ote:? [^\]]+\]', '', content)  # [note: ...]
        content = re.sub(r'\[[Ee]d\.?:? [^\]]+\]', '', content)  # [ed: ...]
        
        # POETRY-SAFE: Only remove numbered list items, not all standalone numbers
        content = re.sub(r'^\d+\.\s+', '', content, flags=re.MULTILINE)  # Remove "1. " list prefixes
        content = re.sub(r'^\d+\)\s+', '', content, flags=re.MULTILINE)  # Remove "1) " list prefixes
        
        # Remove specific formatting artifacts (but preserve poetic structure)
        content = re.sub(r'^\*+\s*', '', content, flags=re.MULTILINE)  # Remove asterisk bullets
        content = re.sub(r'Pag\.\s*\d+', '', content)  # Remove page references
        content = re.sub(r'lin\.\s*\d+', '', content)  # Remove line references
        content = re.sub(r'\d+\.\s*\d+\.\s*\d+\.', '', content)  # Remove version-like numbers
        
        # Remove years in parentheses that might have been missed (only 4-digit years)
        content = re.sub(r'\(\d{4}\)', '', content)  # Remove years like (1245)
        
        # Clean up any double spaces left behind
        content = re.sub(r'\s+', ' ', content)
        content = re.sub(r'^\s+|\s+$', '', content, flags=re.MULTILINE)
        
        # POETRY-SAFE: Keep even very short lines that contain letters
        lines = content.split('\n')
        cleaned_lines = []
        
        for line in lines:
            stripped = line.strip()
            # Keep line if it has any letters at all (including single-letter lines for poetry)
            if re.search(r'[A-Za-z]', stripped):
                cleaned_lines.append(line)
            elif not stripped:  # Keep empty lines for formatting
                cleaned_lines.append(line)
        
        result = '\n'.join(cleaned_lines)
        
        # Clean up excessive blank lines
        result = re.sub(r'\n\s*\n\s*\n+', '\n\n', result)
        
        return result
    
    def final_latin_validation(self, content: str) -> Tuple[str, bool]:
        """Final validation and cleanup to ensure Latin-only content.""" 
        # POETRY-SAFE: Preserve line breaks and spacing for poetry
        # Only clean up excessive whitespace, not all spacing
        clean_content = re.sub(r'[ \t]+', ' ', content)  # Only collapse horizontal whitespace
        clean_content = re.sub(r'^\s+|\s+$', '', clean_content, flags=re.MULTILINE)  # Trim line edges
        clean_content = clean_content.strip()
        
        # Very lenient validation - accept almost all content that has reasonable length
        if len(clean_content) < 10:  # Even more reduced threshold for poetry
            return clean_content, False
        
        # Check for basic Latin indicators or classical text patterns
        latin_words = ['et', 'in', 'ad', 'cum', 'de', 'per', 'pro', 'est', 'sunt', 'qui', 'quae', 'sed', 'ut', 'ex', 'ab', 
                      'que', 'ne', 'enim', 'non', 'si', 'sic', 'hic', 'ille', 'ipse', 'rex', 'deus', 'homo', 'res', 'via',
                      'maria', 'domini', 'dei', 'iesu', 'christe', 'mater', 'caritas', 'pietas', 'nos', 'audi', 'domine',
                      'amor', 'vita', 'mors', 'pax', 'lux', 'nox', 'dies', 'annus', 'tempus', 'carmen', 'versus', 'terra']
        
        # Classical text patterns (common in poetry/prose)
        classical_patterns = [
            r'que\b',  # -que enclitic
            r'[aeiou]m\b',  # accusative endings
            r'[aeiou]s\b',  # various case endings
            r'[aeiou]rum\b',  # genitive plural
            r'tio[nms]?\b',  # -tion words common in Latin
            r'us\b',  # common Latin endings
            r'um\b',  # common Latin endings
            r'is\b',  # common Latin endings
            r'ae\b',  # common Latin endings
            r'or\b',  # common Latin endings
            r'ur\b',  # passive endings
        ]
        
        word_count = len(re.findall(r'\w+', clean_content.lower()))
        latin_word_count = sum(1 for word in latin_words if word in clean_content.lower())
        
        # Check for classical patterns
        pattern_matches = 0
        for pattern in classical_patterns:
            matches = len(re.findall(pattern, clean_content.lower()))
            pattern_matches += matches
        
        # POETRY-SAFE: Even more permissive for short poetic fragments
        has_latin_words = latin_word_count >= 1
        has_classical_patterns = pattern_matches >= 2  # Reduced threshold for poetry
        reasonable_length = word_count >= 5  # Much reduced threshold for poetry fragments
        
        # Very permissive: accept if ANY of these conditions are met
        is_valid = (has_latin_words and reasonable_length) or has_classical_patterns or word_count >= 20
        
        if not is_valid:
            self.logger.debug(f"Text rejected: words={word_count}, latin_words={latin_word_count}, patterns={pattern_matches}")
            self.logger.debug(f"Content preview: {clean_content[:100]}...")
        
        return clean_content, is_valid
    
    async def clean_single_file_enhanced(self, input_path: Path, metadata: Dict = None) -> Dict:
        """Clean a single file with enhanced processing."""
        self.logger.debug(f"Enhanced cleaning: {input_path.name}")
        
        try:
            # Read original file
            async with aiofiles.open(input_path, 'r', encoding='utf-8') as f:
                original_content = await f.read()
            
            if not original_content.strip():
                return {
                    'filename': input_path.name,
                    'success': False,
                    'error': 'empty_file',
                    'action': 'skipped'
                }
            
            # Extract metadata from content if not provided
            if not metadata:
                metadata = self.extract_metadata_from_content(original_content)
            
            # Step 1: Aggressive metadata removal
            cleaned = self.aggressive_metadata_removal(original_content)
            
            # Step 2: Remove chapter/book headings (ENHANCEMENT)
            cleaned = self.remove_chapter_headings(cleaned)
            
            # Step 3: Remove modern formatting (parentheses, brackets, Arabic numerals)
            cleaned = self.remove_modern_formatting(cleaned)
            
            # Step 4: Expand abbreviations
            cleaned = self.expand_abbreviations(cleaned)
            
            # Step 5: Apply content cleaning patterns
            for pattern, replacement in self.content_cleaning_patterns:
                cleaned = pattern.sub(replacement, cleaned)
            
            # Step 6: Standardize orthography
            cleaned = self.orthography.standardize(cleaned)
            
            # Step 7: Final Latin validation and cleanup
            cleaned, is_valid_latin = self.final_latin_validation(cleaned)
            
            if not is_valid_latin:
                return {
                    'filename': input_path.name,
                    'success': False,
                    'error': 'not_valid_latin',
                    'action': 'rejected'
                }
            
            # Step 7: Classification
            period = self.classify_period(cleaned, metadata)
            genre = self.classify_genre(cleaned, metadata.get('title', ''))
            
            # Step 8: Calculate statistics
            stats = calculate_text_stats(cleaned)
            
            # Step 9: Save to appropriate category directory
            category_path = f"{period}/{genre}"
            output_dir = self.output_dir / category_path
            
            clean_filename = input_path.stem
            if clean_filename.endswith('_cleaned'):
                clean_filename = clean_filename[:-8]
            if clean_filename.endswith('.txt'):
                clean_filename = clean_filename[:-4]
            
            output_filename = f"{clean_filename}.txt"
            output_path = output_dir / output_filename
            
            # Save cleaned file (no processing header for LLM training)
            async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
                await f.write(cleaned)
            
            return {
                'filename': input_path.name,
                'output_filename': output_filename,
                'output_path': str(output_path),
                'success': True,
                'action': 'cleaned_enhanced',
                'period': period,
                'genre': genre,
                'category_path': category_path,
                'stats': stats,
                'metadata': metadata
            }
            
        except Exception as e:
            self.logger.error(f"Enhanced cleaning failed for {input_path.name}: {e}")
            return {
                'filename': input_path.name,
                'success': False,
                'error': str(e),
                'action': 'failed'
            }
    
    def extract_metadata_from_content(self, content: str) -> Dict:
        """Extract metadata from content headers."""
        metadata = {}
        
        # Look for header information
        lines = content.split('\n')[:10]  # Check first 10 lines
        
        for line in lines:
            if line.startswith('Title:'):
                metadata['title'] = line.replace('Title:', '').strip()
            elif line.startswith('Parent Work:'):
                metadata['parent_work'] = line.replace('Parent Work:', '').strip()
            elif 'caesar' in line.lower():
                metadata['author'] = 'Caesar'
            elif 'vergilius' in line.lower() or 'virgil' in line.lower():
                metadata['author'] = 'Vergilius'
        
        return metadata
    
    async def clean_directory_enhanced(self, input_dir: Path) -> Dict:
        """Clean directory with enhanced processing and categorization."""
        if not input_dir.exists():
            self.logger.error(f"Input directory does not exist: {input_dir}")
            return {'success_count': 0, 'failure_count': 0, 'details': []}
        
        text_files = list(input_dir.rglob('*.txt'))
        
        if not text_files:
            self.logger.warning(f"No .txt files found in {input_dir}")
            return {'success_count': 0, 'failure_count': 0, 'details': []}
        
        self.logger.info(f"Enhanced cleaning {len(text_files)} files from {input_dir}")
        
        progress = ProgressTracker(len(text_files), "Enhanced cleaning")
        results = {
            'success_count': 0,
            'failure_count': 0,
            'categories': {'classical/prose': 0, 'classical/poetry': 0, 
                          'post_classical/prose': 0, 'post_classical/poetry': 0},
            'total_files': len(text_files),
            'details': []
        }
        
        # Process files in batches
        batch_size = 5  # Smaller batches for intensive processing
        
        for i in range(0, len(text_files), batch_size):
            batch = text_files[i:i + batch_size]
            tasks = [self.clean_single_file_enhanced(file_path) for file_path in batch]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    results['failure_count'] += 1
                    results['details'].append({
                        'success': False,
                        'error': str(result)
                    })
                else:
                    if result.get('success') and result.get('action') == 'cleaned_enhanced':
                        results['success_count'] += 1
                        category = result.get('category_path', 'unknown/uncategorized')
                        if category in results['categories']:
                            results['categories'][category] += 1
                    else:
                        results['failure_count'] += 1
                    
                    results['details'].append(result)
                
                progress.update()
        
        progress.finish()
        
        self.logger.info(
            f"Enhanced cleaning complete: {results['success_count']} cleaned, "
            f"{results['failure_count']} failed"
        )
        
        # Log categorization results
        for category, count in results['categories'].items():
            if count > 0:
                self.logger.info(f"  {category}: {count} files")
        
        return results