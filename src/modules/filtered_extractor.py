#!/usr/bin/env python3
"""
Filtered Latin Content Extractor

Extracts Latin texts from XML dump with intelligent filtering and pre-categorization:
- Classical through Early Renaissance (up to ~1600)
- Excludes fragments and very short texts
- Pre-categorizes by period (classical/post-classical) and type (prose/poetry)
- Filters out modern scholarship and administrative content
"""

import xml.etree.ElementTree as ET
import re
import json
import logging
from typing import List, Dict, Set, Optional
from pathlib import Path


class FilteredLatinExtractor:
    """Extract and categorize historical Latin content (Classical - Early Renaissance)."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Skip patterns - expanded to filter out modern content
        self.skip_patterns = [
            # Administrative pages
            'talk:', 'user:', 'disputatio', 'usor:', 'category:', 'categoria:',
            'template:', 'formula:', 'help:', 'auxilium:', 'file:', 'fasciculus:',
            'mediawiki:', 'special:', 'project:', 'vicifons:',
            
            # Technical files
            '.css', '.js', '.json', '/common.css', '/common.js',
            
            # Modern scholarship and editions
            'patrologia latina', 'corpus christianorum', 'monumenta germaniae',
            'acta sanctorum', 'migne', 'critical edition', 'modern edition',
            
            # Fragments and incomplete texts
            'fragment', 'fragmenta', 'fragmentum', 'excerpta', 'excerpt',
            
            # Very modern Latin (post-Renaissance)
            'saeculum xvii', 'saeculum xviii', 'saeculum xix', 'saeculum xx',
            '17th century', '18th century', '19th century', '20th century'
        ]
        
        # Classical authors (1st century BCE - 5th century CE)
        self.classical_authors = {
            # Golden Age Prose
            'cicero': {'name': 'Marcus Tullius Cicero', 'period': 'classical', 'primary_type': 'prose'},
            'caesar': {'name': 'Gaius Iulius Caesar', 'period': 'classical', 'primary_type': 'prose'},
            'sallustius': {'name': 'Gaius Sallustius Crispus', 'period': 'classical', 'primary_type': 'prose'},
            'livius': {'name': 'Titus Livius', 'period': 'classical', 'primary_type': 'prose'},
            'tacitus': {'name': 'Publius Cornelius Tacitus', 'period': 'classical', 'primary_type': 'prose'},
            'suetonius': {'name': 'Gaius Suetonius Tranquillus', 'period': 'classical', 'primary_type': 'prose'},
            'plinius': {'name': 'Gaius Plinius Secundus', 'period': 'classical', 'primary_type': 'prose'},
            'quintilianus': {'name': 'Marcus Fabius Quintilianus', 'period': 'classical', 'primary_type': 'prose'},
            'nepos': {'name': 'Cornelius Nepos', 'period': 'classical', 'primary_type': 'prose'},
            
            # Golden Age Poetry
            'vergilius': {'name': 'Publius Vergilius Maro', 'period': 'classical', 'primary_type': 'poetry'},
            'virgil': {'name': 'Publius Vergilius Maro', 'period': 'classical', 'primary_type': 'poetry'},
            'horatius': {'name': 'Quintus Horatius Flaccus', 'period': 'classical', 'primary_type': 'poetry'},
            'ovidius': {'name': 'Publius Ovidius Naso', 'period': 'classical', 'primary_type': 'poetry'},
            'catullus': {'name': 'Gaius Valerius Catullus', 'period': 'classical', 'primary_type': 'poetry'},
            'propertius': {'name': 'Sextus Propertius', 'period': 'classical', 'primary_type': 'poetry'},
            'tibullus': {'name': 'Albius Tibullus', 'period': 'classical', 'primary_type': 'poetry'},
            'lucretius': {'name': 'Titus Lucretius Carus', 'period': 'classical', 'primary_type': 'poetry'},
            
            # Silver Age
            'seneca': {'name': 'Lucius Annaeus Seneca', 'period': 'classical', 'primary_type': 'prose'},
            'petronius': {'name': 'Gaius Petronius', 'period': 'classical', 'primary_type': 'prose'},
            'juvenalis': {'name': 'Decimus Iunius Iuvenalis', 'period': 'classical', 'primary_type': 'poetry'},
            'martialis': {'name': 'Marcus Valerius Martialis', 'period': 'classical', 'primary_type': 'poetry'},
            'lucanus': {'name': 'Marcus Annaeus Lucanus', 'period': 'classical', 'primary_type': 'poetry'},
            'statius': {'name': 'Publius Papinius Statius', 'period': 'classical', 'primary_type': 'poetry'},
            'silius': {'name': 'Silius Italicus', 'period': 'classical', 'primary_type': 'poetry'},
            'valerius flaccus': {'name': 'Gaius Valerius Flaccus', 'period': 'classical', 'primary_type': 'poetry'},
            'persius': {'name': 'Aulus Persius Flaccus', 'period': 'classical', 'primary_type': 'poetry'},
            
            # Late Classical
            'apuleius': {'name': 'Lucius Apuleius', 'period': 'classical', 'primary_type': 'prose'},
            'gellius': {'name': 'Aulus Gellius', 'period': 'classical', 'primary_type': 'prose'},
            'ammianus': {'name': 'Ammianus Marcellinus', 'period': 'classical', 'primary_type': 'prose'},
            
            # Early Christian (still Classical period)
            'lactantius': {'name': 'Lucius Caecilius Firmianus Lactantius', 'period': 'classical', 'primary_type': 'prose'},
            'tertullianus': {'name': 'Quintus Septimius Florens Tertullianus', 'period': 'classical', 'primary_type': 'prose'},
        }
        
        # Post-Classical authors (Late Antique through Early Renaissance)
        self.post_classical_authors = {
            # Patristic Period (4th-8th centuries)
            'augustinus': {'name': 'Augustinus Hipponensis', 'period': 'post_classical', 'primary_type': 'prose'},
            'hieronymus': {'name': 'Sophronius Eusebius Hieronymus', 'period': 'post_classical', 'primary_type': 'prose'},
            'ambrosius': {'name': 'Ambrosius Mediolanensis', 'period': 'post_classical', 'primary_type': 'prose'},
            'gregorius': {'name': 'Gregorius Magnus', 'period': 'post_classical', 'primary_type': 'prose'},
            'isidorus': {'name': 'Isidorus Hispalensis', 'period': 'post_classical', 'primary_type': 'prose'},
            'beda': {'name': 'Beda Venerabilis', 'period': 'post_classical', 'primary_type': 'prose'},
            'boethius': {'name': 'Anicius Manlius Severinus Boethius', 'period': 'post_classical', 'primary_type': 'prose'},
            'cassiodorus': {'name': 'Magnus Aurelius Cassiodorus', 'period': 'post_classical', 'primary_type': 'prose'},
            
            # Carolingian Renaissance (8th-9th centuries)
            'alcuinus': {'name': 'Alcuinus', 'period': 'post_classical', 'primary_type': 'prose'},
            'einhard': {'name': 'Einhard', 'period': 'post_classical', 'primary_type': 'prose'},
            'hrabanus maurus': {'name': 'Hrabanus Maurus', 'period': 'post_classical', 'primary_type': 'prose'},
            
            # Scholastic Period (11th-15th centuries)
            'anselmus': {'name': 'Anselmus Cantuariensis', 'period': 'post_classical', 'primary_type': 'prose'},
            'abelardus': {'name': 'Petrus Abelardus', 'period': 'post_classical', 'primary_type': 'prose'},
            'aquinas': {'name': 'Thomas Aquinas', 'period': 'post_classical', 'primary_type': 'prose'},
            'bernardus': {'name': 'Bernardus Claraevallensis', 'period': 'post_classical', 'primary_type': 'prose'},
            'scotus': {'name': 'Iohannes Duns Scotus', 'period': 'post_classical', 'primary_type': 'prose'},
            'ockham': {'name': 'Gulielmus de Ockham', 'period': 'post_classical', 'primary_type': 'prose'},
            
            # Medieval Historians and Chroniclers
            'paulus diaconus': {'name': 'Paulus Diaconus', 'period': 'post_classical', 'primary_type': 'prose'},
            'jordanes': {'name': 'Jordanes', 'period': 'post_classical', 'primary_type': 'prose'},
            'gregory of tours': {'name': 'Gregorius Turonensis', 'period': 'post_classical', 'primary_type': 'prose'},
            
            # Medieval Poetry
            'prudentius': {'name': 'Aurelius Prudentius Clemens', 'period': 'post_classical', 'primary_type': 'poetry'},
            'fortunatus': {'name': 'Venantius Honorius Clementianus Fortunatus', 'period': 'post_classical', 'primary_type': 'poetry'},
            'sedulius': {'name': 'Coelius Sedulius', 'period': 'post_classical', 'primary_type': 'poetry'},
            
            # Early Renaissance Humanists (14th-16th centuries)
            'petrarca': {'name': 'Francesco Petrarca', 'period': 'post_classical', 'primary_type': 'prose'},  # Also poetry
            'boccaccio': {'name': 'Giovanni Boccaccio', 'period': 'post_classical', 'primary_type': 'prose'},
            'salutati': {'name': 'Coluccio Salutati', 'period': 'post_classical', 'primary_type': 'prose'},
            'bruni': {'name': 'Leonardo Bruni', 'period': 'post_classical', 'primary_type': 'prose'},
            'valla': {'name': 'Lorenzo Valla', 'period': 'post_classical', 'primary_type': 'prose'},
            'ficino': {'name': 'Marsilio Ficino', 'period': 'post_classical', 'primary_type': 'prose'},
            'pico': {'name': 'Giovanni Pico della Mirandola', 'period': 'post_classical', 'primary_type': 'prose'},
            'erasmus': {'name': 'Desiderius Erasmus', 'period': 'post_classical', 'primary_type': 'prose'},
            'more': {'name': 'Thomas More', 'period': 'post_classical', 'primary_type': 'prose'},
            'machiavelli': {'name': 'Niccolò Machiavelli', 'period': 'post_classical', 'primary_type': 'prose'},
        }
        
        # Work-specific type classifications
        self.work_type_patterns = {
            # Poetry indicators
            'poetry': [
                'carmen', 'carmina', 'versus', 'ode', 'odae', 'ecloga', 'eclogae',
                'epigramma', 'epigrammata', 'elegia', 'elegiae', 'epopeia',
                'aeneis', 'metamorphoses', 'georgica', 'bucolica', 'satira', 'satirae',
                'hymnus', 'hymni', 'canticum', 'cantica'
            ],
            # Prose indicators  
            'prose': [
                'de ', 'commentarii', 'historia', 'historiae', 'annales', 'epistula',
                'epistulae', 'oratio', 'orationes', 'dialog', 'sermo', 'tractatus',
                'summa', 'quaestio', 'sententia', 'confessiones', 'institutio',
                'chronicon', 'vita', 'vitae', 'bellum'
            ]
        }
        
        # Period indicators in titles
        self.period_indicators = {
            'classical': [
                # Roman period names
                'romana', 'romanus', 'imperialis', 'caesar', 'augustus',
                # Classical genres
                'rhetorica', 'philosophia', 'naturalis historia'
            ],
            'post_classical': [
                # Christian/Medieval indicators
                'christianus', 'ecclesia', 'sanctus', 'beatus', 'martyr',
                'theologia', 'theologicus', 'monasticus', 'clerical',
                # Medieval genres
                'scholastica', 'medieval', 'medius', 'saeculum'
            ]
        }
    
    def extract_filtered_latin_works(self, xml_file_path: str) -> List[Dict]:
        """Extract filtered and pre-categorized Latin works."""
        self.logger.info(f"Extracting filtered Latin content from XML dump: {xml_file_path}")
        
        latin_works = []
        processed_count = 0
        
        try:
            context = ET.iterparse(xml_file_path, events=('start', 'end'))
            context = iter(context)
            event, root = next(context)
            
            for event, elem in context:
                if event == 'end' and elem.tag.endswith('}page'):
                    processed_count += 1
                    
                    # Extract page data
                    ns_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.11/}ns')
                    title_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.11/}title')
                    text_elem = elem.find('.//{http://www.mediawiki.org/xml/export-0.11/}text')
                    
                    if (ns_elem is not None and 
                        title_elem is not None and 
                        text_elem is not None and
                        ns_elem.text == '0'):
                        
                        title = title_elem.text
                        text_content = text_elem.text or ''
                        
                        if self._is_valid_historical_latin(title, text_content):
                            work_data = self._create_categorized_work_data(title, text_content)
                            if work_data:  # Only add if categorization succeeded
                                latin_works.append(work_data)
                    
                    # Clear element to save memory
                    elem.clear()
                    
                    # Progress logging
                    if processed_count % 5000 == 0:
                        self.logger.info(f"Processed {processed_count} pages, found {len(latin_works)} historical Latin works")
            
            root.clear()
            
        except Exception as e:
            self.logger.error(f"Error parsing XML dump: {e}")
            raise
        
        self.logger.info(f"Extraction complete: {len(latin_works)} historical Latin works from {processed_count} pages")
        return latin_works
    
    def _is_valid_historical_latin(self, title: str, text_content: str) -> bool:
        """Check if content represents valid historical Latin (Classical - Early Renaissance)."""
        if not title or not text_content:
            return False
        
        title_lower = title.lower()
        content_lower = text_content.lower()
        
        # Skip administrative and modern content
        if any(pattern in title_lower for pattern in self.skip_patterns):
            return False
        
        # Skip redirects
        if (text_content.strip().startswith('#REDIRECT') or 
            text_content.strip().startswith('#redirect')):
            return False
        
        # Must have substantial content (raised threshold for quality)
        if len(text_content.strip()) < 500:
            return False
        
        # Skip if it's obviously modern Latin (17th century onwards)
        modern_indicators = [
            '16[0-9][0-9]', '17[0-9][0-9]', '18[0-9][0-9]', '19[0-9][0-9]', '20[0-9][0-9]',
            'saeculum xvii', 'saeculum xviii', 'saeculum xix', 'saeculum xx',
            'post-tridentine', 'counter-reformation', 'baroque', 'enlightenment'
        ]
        
        if any(re.search(pattern, content_lower) for pattern in modern_indicators):
            return False
        
        # Skip obvious fragments (unless they're substantial)
        if ('fragment' in title_lower and len(text_content.strip()) < 2000):
            return False
        
        # Skip modern critical apparatus
        critical_apparatus = [
            'critical edition', 'textual criticism', 'apparatus criticus',
            'manuscript tradition', 'stemma codicum', 'editorial notes'
        ]
        
        if any(phrase in content_lower for phrase in critical_apparatus):
            return False
        
        return True
    
    def _create_categorized_work_data(self, title: str, text_content: str) -> Optional[Dict]:
        """Create work data with pre-categorization."""
        # Extract basic information
        author_info = self._determine_author_and_period(title)
        work_type = self._classify_work_type(title, text_content, author_info)
        priority = self._assign_priority(title, author_info, work_type)
        
        # Additional metadata
        content_length = len(text_content.strip())
        
        # Estimate completeness
        completeness = self._estimate_completeness(title, text_content)
        
        work_data = {
            'title': title,
            'author': author_info['name'],
            'author_key': author_info['key'],
            'period': author_info['period'],
            'work_type': work_type,
            'priority': priority,
            'content_length': content_length,
            'completeness': completeness,
            'source': 'xml_dump_filtered',
            'is_index_likely': self._is_likely_index_page(text_content)
        }
        
        return work_data
    
    def _determine_author_and_period(self, title: str) -> Dict:
        """Determine author and period from title."""
        title_lower = title.lower()
        
        # Enhanced author detection with better pattern matching
        # Check classical authors first
        for author_key, author_data in self.classical_authors.items():
            if author_key in title_lower:
                return {
                    'name': author_data['name'],
                    'key': author_key,
                    'period': 'classical',
                    'primary_type': author_data['primary_type']
                }
        
        # Check post-classical authors
        for author_key, author_data in self.post_classical_authors.items():
            if author_key in title_lower:
                return {
                    'name': author_data['name'],
                    'key': author_key,
                    'period': 'post_classical',
                    'primary_type': author_data['primary_type']
                }
        
        # Enhanced work-specific patterns for known classical works
        known_classical_works = {
            'noctes atticae': {'name': 'Aulus Gellius', 'key': 'gellius'},
            'aeneis': {'name': 'Publius Vergilius Maro', 'key': 'vergilius'},
            'georgica': {'name': 'Publius Vergilius Maro', 'key': 'vergilius'},
            'eclogae': {'name': 'Publius Vergilius Maro', 'key': 'vergilius'},
            'bucolica': {'name': 'Publius Vergilius Maro', 'key': 'vergilius'},
            'metamorphoses': {'name': 'Publius Ovidius Naso', 'key': 'ovidius'},
            'ars amatoria': {'name': 'Publius Ovidius Naso', 'key': 'ovidius'},
            'fasti': {'name': 'Publius Ovidius Naso', 'key': 'ovidius'},
            'tristia': {'name': 'Publius Ovidius Naso', 'key': 'ovidius'},
            'institutio oratoria': {'name': 'Marcus Fabius Quintilianus', 'key': 'quintilianus'},
            'satyricon': {'name': 'Gaius Petronius', 'key': 'petronius'},
            'bellum iugurthinum': {'name': 'Gaius Sallustius Crispus', 'key': 'sallustius'},
            'bellum catilinae': {'name': 'Gaius Sallustius Crispus', 'key': 'sallustius'},
            'naturalis historia': {'name': 'Gaius Plinius Secundus', 'key': 'plinius'},
            'epistulae morales': {'name': 'Lucius Annaeus Seneca', 'key': 'seneca'},
            'de rerum natura': {'name': 'Titus Lucretius Carus', 'key': 'lucretius'},
            'carmina': {'name': 'Quintus Horatius Flaccus', 'key': 'horatius'},
            'satirae': {'name': 'Quintus Horatius Flaccus', 'key': 'horatius'},
            'epistulae': {'name': 'Quintus Horatius Flaccus', 'key': 'horatius'},
            'thebais': {'name': 'Publius Papinius Statius', 'key': 'statius'},
            'punica': {'name': 'Silius Italicus', 'key': 'silius'},
            'argonautica': {'name': 'Gaius Valerius Flaccus', 'key': 'valerius flaccus'}
        }
        
        for work_pattern, author_info in known_classical_works.items():
            if work_pattern in title_lower:
                return {
                    'name': author_info['name'],
                    'key': author_info['key'],
                    'period': 'classical',
                    'primary_type': self.classical_authors[author_info['key']]['primary_type']
                }
        
        # Try to extract author from parenthetical indication
        if '(' in title and ')' in title:
            match = re.search(r'\(([^)]+)\)$', title)
            if match:
                author_name = match.group(1).strip()
                # Check if this matches any known author
                author_lower = author_name.lower()
                for author_key, author_data in {**self.classical_authors, **self.post_classical_authors}.items():
                    if author_key in author_lower:
                        return {
                            'name': author_data['name'],
                            'key': author_key,
                            'period': author_data['period'],
                            'primary_type': author_data['primary_type']
                        }
                
                # Unknown author but has indication
                return {
                    'name': author_name,
                    'key': 'unknown',
                    'period': self._estimate_period_from_title(title),
                    'primary_type': 'prose'  # default
                }
        
        # Extract from slash format
        if '/' in title:
            parts = title.split('/')
            potential_author = parts[0].strip()
            return {
                'name': potential_author,
                'key': 'unknown',
                'period': self._estimate_period_from_title(title),
                'primary_type': 'prose'
            }
        
        # Default unknown author
        return {
            'name': 'Unknown',
            'key': 'unknown', 
            'period': self._estimate_period_from_title(title),
            'primary_type': 'prose'
        }
    
    def _classify_work_type(self, title: str, text_content: str, author_info: Dict) -> str:
        """Classify work as prose or poetry with enhanced logic."""
        title_lower = title.lower()
        content_lower = text_content.lower()
        
        # Use author's primary type as baseline
        baseline_type = author_info.get('primary_type', 'prose')
        
        # Check for explicit poetry indicators in title
        poetry_score = 0
        prose_score = 0
        
        for pattern in self.work_type_patterns['poetry']:
            if pattern in title_lower:
                poetry_score += 2
        
        for pattern in self.work_type_patterns['prose']:
            if pattern in title_lower:
                prose_score += 2
        
        # Check content for verse patterns (basic heuristic)
        if self._has_verse_structure(text_content):
            poetry_score += 1
        
        # Specific work classifications
        if any(work in title_lower for work in ['aeneis', 'metamorphoses', 'georgica', 'carmen']):
            return 'poetry'
        
        if any(work in title_lower for work in ['commentarii', 'historia', 'de ', 'epistula', 'oratio']):
            return 'prose'
        
        # Use scores with author baseline
        if baseline_type == 'poetry':
            return 'poetry' if poetry_score >= prose_score else 'prose'
        else:
            return 'prose' if prose_score >= poetry_score else 'poetry'
    
    def _has_verse_structure(self, text_content: str) -> bool:
        """Basic check for verse structure in content."""
        lines = text_content.split('\n')
        short_lines = sum(1 for line in lines if 20 <= len(line.strip()) <= 60)
        total_lines = len([line for line in lines if len(line.strip()) > 10])
        
        # If more than 30% of lines are "verse-like" length, likely poetry
        if total_lines > 0 and (short_lines / total_lines) > 0.3:
            return True
        
        return False
    
    def _estimate_period_from_title(self, title: str) -> str:
        """Estimate period when author is unknown."""
        title_lower = title.lower()
        
        # Check for explicit period indicators
        for period, indicators in self.period_indicators.items():
            if any(indicator in title_lower for indicator in indicators):
                return period
        
        # Specific classical work patterns (to catch works like "De X", "Liber Y", etc.)
        classical_work_patterns = [
            r'\bde\s+\w+',  # "De rerum natura", "De officiis", etc.
            r'\bcommentarii',  # "Commentarii de bello gallico"
            r'\bannales\b',  # "Annales"
            r'\bhistoriae?\b',  # "Historiae"
            r'\bepistulae?\b',  # "Epistulae"
            r'\borationes?\b',  # "Orationes"
            r'\bcarmina?\b',  # "Carmina"
            r'\bgeorgica\b',  # "Georgica"
            r'\bmetamorphoses\b',  # "Metamorphoses"
            r'\baeneis\b',  # "Aeneis"
            r'\bsatirae?\b',  # "Satirae"
        ]
        
        if any(re.search(pattern, title_lower) for pattern in classical_work_patterns):
            return 'classical'
        
        # Medieval/Christian content indicators (more specific now)
        christian_indicators = [
            'sanctus', 'beatus', 'martyr', 'ecclesia', 'christianus',
            'theologia', 'theologicus', 'tractatus', 'summa',
            'confessiones', 'vita sancti', 'passio', 'martyrium'
        ]
        
        # Only consider "liber" post-classical if it's in a clearly Christian context
        has_liber = 'liber' in title_lower
        has_christian_context = any(indicator in title_lower for indicator in christian_indicators)
        
        if has_christian_context or (has_liber and any(indicator in title_lower for indicator in ['sanctus', 'theologia', 'tractatus'])):
            return 'post_classical'
        
        # Additional classical indicators
        classical_indicators = [
            'oratio', 'rhetorica', 'philosophia', 'natura', 'republica',
            'dialogus', 'naturalis', 'bellum', 'institutio'
        ]
        
        if any(indicator in title_lower for indicator in classical_indicators):
            return 'classical'
        
        # For "Liber X" without clear context, default to classical
        # (since many classical works are organized in books)
        if has_liber and not has_christian_context:
            return 'classical'
        
        # Default to post-classical only for truly unknown works
        return 'post_classical'
    
    def _assign_priority(self, title: str, author_info: Dict, work_type: str) -> str:
        """Assign priority for scraping."""
        title_lower = title.lower()
        
        # Critical works - major texts for Latin learning
        critical_works = [
            'commentarii de bello gallico', 'aeneis', 'metamorphoses',
            'de re publica', 'de officiis', 'confessiones', 'summa theologiae',
            'de philosophiae consolatione', 'annales', 'historiae',
            'institutio oratoria', 'de rerum natura', 'georgica'
        ]
        
        if any(work in title_lower for work in critical_works):
            return 'critical'
        
        # High priority - major authors
        if author_info['key'] in ['cicero', 'caesar', 'vergilius', 'virgil', 'horatius', 
                                 'ovidius', 'augustinus', 'aquinas', 'boethius']:
            return 'high'
        
        # Medium priority - substantial classical works
        if author_info['period'] == 'classical' and author_info['key'] != 'unknown':
            return 'medium'
        
        return 'normal'
    
    def _estimate_completeness(self, title: str, text_content: str) -> str:
        """Estimate if work is complete, partial, or fragment."""
        title_lower = title.lower()
        content_length = len(text_content.strip())
        
        # Explicit indicators
        if any(indicator in title_lower for indicator in ['fragment', 'fragmenta', 'excerpta']):
            return 'fragment'
        
        if any(indicator in title_lower for indicator in ['liber', 'book', 'capitulum', 'chapter']):
            return 'partial'  # Likely a section of a larger work
        
        # Length-based estimation
        if content_length < 1000:
            return 'fragment'
        elif content_length < 10000:
            return 'partial'
        else:
            return 'complete'  # Assumed complete if substantial
    
    def _is_likely_index_page(self, text_content: str) -> bool:
        """Check if content appears to be an index/table of contents."""
        # Count wiki links vs actual text
        link_count = len(re.findall(r'\[\[[^\]]+\]\]', text_content))
        
        # Remove all markup to get clean text
        clean_text = re.sub(r'\[\[[^\]]+\]\]', '', text_content)
        clean_text = re.sub(r'\{\{[^}]+\}\}', '', clean_text)  
        clean_text = re.sub(r'<[^>]+>', '', clean_text)
        clean_text = re.sub(r'[#*:]+', '', clean_text)
        
        word_count = len(re.findall(r'\w+', clean_text))
        
        # If high ratio of links to content, likely an index
        if word_count > 0 and link_count > 5 and (link_count / word_count) > 0.3:
            return True
        
        return False
    
    def save_categorized_works(self, works: List[Dict], output_file: str):
        """Save categorized works to JSON with statistics."""
        self.logger.info(f"Saving {len(works)} categorized works to {output_file}")
        
        # Calculate statistics
        from collections import Counter
        
        stats = {
            'total_works': len(works),
            'by_period': dict(Counter(work['period'] for work in works)),
            'by_type': dict(Counter(work['work_type'] for work in works)), 
            'by_priority': dict(Counter(work['priority'] for work in works)),
            'by_completeness': dict(Counter(work['completeness'] for work in works)),
            'by_author_period': {},
            'generation_date': str(Path(__file__).stat().st_mtime)
        }
        
        # Top authors by period
        classical_authors = Counter(work['author'] for work in works if work['period'] == 'classical')
        post_classical_authors = Counter(work['author'] for work in works if work['period'] == 'post_classical')
        
        stats['by_author_period'] = {
            'classical': dict(classical_authors.most_common(20)),
            'post_classical': dict(post_classical_authors.most_common(20))
        }
        
        # Save data
        output_data = {
            'metadata': stats,
            'works': works
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Saved {len(works)} works with categorization")
        
        # Print statistics
        print(f"\nCategorized Latin Works Statistics:")
        print(f"Total works: {stats['total_works']}")
        print(f"\nBy Period:")
        for period, count in stats['by_period'].items():
            print(f"  {period}: {count}")
        print(f"\nBy Type:")
        for work_type, count in stats['by_type'].items():
            print(f"  {work_type}: {count}")
        print(f"\nBy Priority:")
        for priority, count in stats['by_priority'].items():
            print(f"  {priority}: {count}")


def main():
    """Main function to extract filtered Latin works."""
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    extractor = FilteredLatinExtractor()
    
    xml_file = "/Users/willow/Documents/Combined Scraper & Cleaner/LawikiSource Dump Jul 20 2025.xml"
    output_file = "/Users/willow/Documents/Combined Scraper & Cleaner/filtered_latin_works.json"
    
    # Extract filtered works
    works = extractor.extract_filtered_latin_works(xml_file)
    
    # Save the categorized list
    extractor.save_categorized_works(works, output_file)
    
    print(f"\nFiltered extraction complete!")
    print(f"Output: {output_file}")


if __name__ == "__main__":
    main()