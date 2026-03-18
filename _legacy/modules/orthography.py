#!/usr/bin/env python3
"""
Orthography Standardization Module

Standardizes Latin orthography for consistent LLM training data:
- Classical spelling normalization (j->i, v->u)
- Medieval variant normalization
- Diacritic removal
- Case normalization
- Abbreviation expansion
"""

import re
import unicodedata
import logging
from typing import Dict, List, Tuple

class OrthographyStandardizer:
    """Standardizes Latin orthography for consistent processing."""
    
    def __init__(self):
        """Initialize the orthography standardizer."""
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Medieval variants normalization
        self.medieval_variants = self._load_medieval_variants()
        
        # Common Latin abbreviations
        self.abbreviations = self._load_abbreviations()
        
        # Praenomina (Roman names)
        self.praenomina = self._load_praenomina()
        
        self.logger.info("OrthographyStandardizer initialized")
    
    def _load_medieval_variants(self) -> Dict[str, str]:
        """Load medieval spelling variants to normalize."""
        return {
            # H/CH variants
            r'\bmichi\b': 'mihi',
            r'\btichi\b': 'tibi', 
            r'\bsichi\b': 'sibi',
            r'\bnichil\b': 'nihil',
            r'\bnichilo\b': 'nihilo',
            r'\bmacina\b': 'machina',
            r'\bpulcer\b': 'pulcher',
            r'\bsepulcrum\b': 'sepulchrum',
            
            # TI/CI variants
            r'\bdiviciae\b': 'divitiae',
            r'\btercius\b': 'tertius',
            r'\bvicium\b': 'vitium',
            r'\bnegocium\b': 'negotium',
            r'\bprecium\b': 'pretium',
            r'\bspacium\b': 'spatium',
            r'\bgracie\b': 'gratiae',
            r'\bjusticia\b': 'justitia',
            
            # MN/MPN variants
            r'\bdampnum\b': 'damnum',
            r'\bcolumpna\b': 'columna',
            r'\bsolempnis\b': 'sollemnis',
            r'\bsompnus\b': 'somnus',
            
            # AE/E simplification (restore diphthongs)
            r'\bcese\b': 'caese',
            r'\bquedam\b': 'quaedam',
            r'\bpretor\b': 'praetor',
            r'\bequs\b': 'aequus',
            r'\bequalitas\b': 'aequalitas',
            
            # OE/E simplification (restore diphthongs)
            r'\bpena\b': 'poena',
            r'\bfenum\b': 'foenum',
            r'\bfedus\b': 'foedus',
            
            # Double consonant normalization
            r'\btranquilitas\b': 'tranquillitas',
            r'\bAffrica\b': 'Africa',
            r'\boccasio\b': 'occasio',
            
            # H-loss restoration
            r'\babere\b': 'habere',
            r'\bomines\b': 'homines',
            r'\bonor\b': 'honor',
            r'\bumanus\b': 'humanus',
            
            # Remove incorrect h-addition
            r'\bchorona\b': 'corona',
            r'\brhethor\b': 'rhetor',
        }
    
    def _load_abbreviations(self) -> Dict[str, str]:
        """Load common Latin abbreviations for expansion."""
        return {
            # Common abbreviations
            r'\bq\.\s*': 'que ',
            r'\bc\.\s*': 'cum ',
            r'\bet\s+c\.': 'et cetera',
            r'\bi\.\s*e\.': 'id est',
            r'\be\.\s*g\.': 'exempli gratia',
            r'\bviz\.': 'videlicet',
            r'\bscil\.': 'scilicet',
            r'\bv\.\s*': 'vide ',
            r'\bcf\.': 'confer',
            r'\bib\.': 'ibidem',
            r'\bid\.': 'idem',
            
            # Religious abbreviations
            r'\bD\.\s*N\.': 'Dominus Noster',
            r'\bI\.\s*H\.\s*S\.': 'Iesus Hominum Salvator', 
            r'\bD\.\s*M\.': 'Dis Manibus',
            r'\bR\.\s*I\.\s*P\.': 'Requiescat In Pace',
            r'\bA\.\s*D\.': 'Anno Domini',
            r'\bA\.\s*M\.': 'Ave Maria',
            
            # Common contractions
            r'\bxpts\b': 'Christus',
            r'\bihs\b': 'Iesus',
            r'\bdns\b': 'dominus',
            r'\bsps\b': 'spiritus',
            r'\bscs\b': 'sanctus',
            
            # Titles
            r'\bImp\.': 'Imperator',
            r'\bCaes\.': 'Caesar',
            r'\bCons\.': 'Consul',
            r'\bPont\.': 'Pontifex',
        }
    
    def _load_praenomina(self) -> Dict[str, str]:
        """Load Roman praenomina abbreviations."""
        return {
            # Male praenomina (most common)
            r'\bM\.\s*': 'Marcus ',
            r'\bL\.\s*': 'Lucius ',
            r'\bC\.\s*': 'Gaius ',
            r'\bP\.\s*': 'Publius ',
            r'\bQ\.\s*': 'Quintus ',
            r'\bA\.\s*': 'Aulus ',
            r'\bAp\.\s*': 'Appius ',
            r'\bCn\.\s*': 'Gnaeus ',
            r'\bD\.\s*': 'Decimus ',
            r'\bSer\.\s*': 'Servius ',
            r'\bSex\.\s*': 'Sextus ',
            r'\bT\.\s*': 'Titus ',
            r'\bTi\.\s*': 'Tiberius ',
            r'\bTib\.\s*': 'Tiberius ',
        }
    
    def normalize_medieval_variants(self, text: str) -> str:
        """Normalize medieval spelling variants to classical forms."""
        for pattern, replacement in self.medieval_variants.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
        return text
    
    def expand_abbreviations(self, text: str, expand_names: bool = False) -> str:
        """Expand common Latin abbreviations."""
        # Expand common abbreviations
        for pattern, replacement in self.abbreviations.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
        # Expand praenomina if requested
        if expand_names:
            for pattern, replacement in self.praenomina.items():
                text = re.sub(pattern, replacement, text)
        
        return text
    
    def normalize_classical_spelling(self, text: str) -> str:
        """Normalize to classical Latin spelling conventions with enhanced v/u and j/i handling."""
        
        # First, handle specific common classical words with precise patterns
        classical_word_fixes = {
            # V/U normalization for very common words
            r'\bvnvs\b': 'unus',
            r'\bvti\b': 'uti', 
            r'\bvt\b': 'ut',
            r'\bsva\b': 'sua',
            r'\bsvvs\b': 'suus', 
            r'\bsvae\b': 'suae',
            r'\bsvam\b': 'suam',
            r'\bqvod\b': 'quod',
            r'\bqvae\b': 'quae',
            r'\bqvi\b': 'qui',
            r'\bqvem\b': 'quem',
            r'\bqvam\b': 'quam',
            r'\bqvibus\b': 'quibus',
            r'\bqvorvm\b': 'quorum',
            r'\bqvarvm\b': 'quarum',
            r'\bvbi\b': 'ubi',
            r'\bvnde\b': 'unde',
            r'\bvnvm\b': 'unum',
            r'\bvnam\b': 'unam',
            r'\bvllo\b': 'ullo',
            r'\bvlla\b': 'ulla',
            r'\bvnqvam\b': 'unquam',
            r'\bvtiqve\b': 'utique',
            r'\bplvrimvs\b': 'plurimus',
            r'\bplvrima\b': 'plurima',
            r'\bplvrimvm\b': 'plurimum',
            r'\bpopvlvs\b': 'populus',
            r'\bpopvli\b': 'populi',
            r'\bpopvlo\b': 'populo',
            r'\bpopvlvm\b': 'populum',
            r'\bconsulibus\b': 'consulibus',
            r'\bconsvl\b': 'consul',
            r'\bconsvles\b': 'consules',
            r'\bconsvlvm\b': 'consulem',
            r'\bconsvlibus\b': 'consulibus',
            
            # J/I normalization for common words  
            r'\bidem\b': 'idem',
            r'\bjam\b': 'iam',
            r'\bjdemque\b': 'idemque',
            r'\bmajor\b': 'maior',
            r'\bmajores\b': 'maiores',
            r'\bmajorem\b': 'maiorem',
            r'\bmajus\b': 'maius',
            r'\bpeior\b': 'peior',  # Keep as is - correct
            r'\bpejor\b': 'peior',   # Fix alternative spelling
            r'\bmejor\b': 'melior',  # Fix common error
            r'\bjulius\b': 'Iulius',
            r'\bjulia\b': 'Iulia',
            r'\bjustus\b': 'iustus',
            r'\bjustum\b': 'iustum',
            r'\bjusta\b': 'iusta',
            r'\bjustitia\b': 'iustitia',
        }
        
        # Apply word-specific fixes first (most precise)
        for pattern, replacement in classical_word_fixes.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
        # J -> I conversion (enhanced with better context awareness)
        # Handle word-initial j -> i (but be careful with proper names)
        text = re.sub(r'\bj([aeiou])', r'i\1', text, flags=re.IGNORECASE)  
        # Handle j between vowels or after vowels
        text = re.sub(r'([aeiou])j([aeiou])', r'\1i\2', text, flags=re.IGNORECASE)
        text = re.sub(r'([aeiou])j\b', r'\1i', text, flags=re.IGNORECASE)
        text = re.sub(r'([aeiou])j([bcdfghlmnpqrstvwxyz])', r'\1i\2', text, flags=re.IGNORECASE)
        
        # V -> U conversion in vowel positions (enhanced)
        # Handle vowel-v-vowel sequences
        text = re.sub(r'([aeiou])v([aeiou])', r'\1u\2', text, flags=re.IGNORECASE)
        # Handle consonant-v-vowel where v is clearly vocalic
        text = re.sub(r'([bcdfghlmnpqrst])v([aeiou])', r'\1u\2', text, flags=re.IGNORECASE)
        # Handle v after consonants when followed by consonants (likely vocalic)
        text = re.sub(r'([bcdfghlmnpqrst])v([mnlr])', r'\1u\2', text, flags=re.IGNORECASE)
        # Handle final -vs endings (common genitive/nominative)
        text = re.sub(r'([aeiou])vs\b', r'\1us', text, flags=re.IGNORECASE)
        # Handle -vm endings (accusative)
        text = re.sub(r'([aeiou])vm\b', r'\1um', text, flags=re.IGNORECASE)
        
        # Keep initial V before vowels (consonantal V)
        # This is already handled by the patterns above
        
        # Additional common suffixes and patterns
        suffix_patterns = {
            r'([aeiou])vnt\b': r'\1unt',      # 3rd person plural -unt
            r'([aeiou])vntur\b': r'\1untur',  # 3rd person plural passive
            r'([aeiou])vndvs\b': r'\1undus',  # gerundive -undus
            r'([aeiou])vnda\b': r'\1unda',    # gerundive -unda
            r'([aeiou])vndvm\b': r'\1undum',  # gerundive -undum
            r'tivs\b': 'tius',                # -tius endings
            r'tiva\b': 'tia',                 # -tia endings  
            r'tivm\b': 'tium',                # -tium endings
        }
        
        for pattern, replacement in suffix_patterns.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
        return text
    
    def remove_diacritics(self, text: str) -> str:
        """Remove diacritical marks for standardization."""
        # Normalize to NFD (decomposed) form
        text = unicodedata.normalize('NFD', text)
        
        # Remove combining diacritical marks
        text = ''.join(char for char in text 
                      if not unicodedata.combining(char))
        
        # Handle specific Latin diacritics
        diacritic_map = {
            'ā': 'a', 'ē': 'e', 'ī': 'i', 'ō': 'o', 'ū': 'u', 'ȳ': 'y',
            'Ā': 'A', 'Ē': 'E', 'Ī': 'I', 'Ō': 'O', 'Ū': 'U', 'Ȳ': 'Y',
            'ă': 'a', 'ĕ': 'e', 'ĭ': 'i', 'ŏ': 'o', 'ŭ': 'u',
            'Ă': 'A', 'Ĕ': 'E', 'Ĭ': 'I', 'Ŏ': 'O', 'Ŭ': 'U',
        }
        
        for diacritic, base in diacritic_map.items():
            text = text.replace(diacritic, base)
        
        return text
    
    def normalize_case(self, text: str, mode: str = 'lowercase') -> str:
        """Normalize text case for consistent training."""
        if mode == 'lowercase':
            return text.lower()
        elif mode == 'uppercase':
            return text.upper()
        elif mode == 'title':
            return text.title()
        elif mode == 'sentence':
            # Capitalize first letter of sentences
            sentences = re.split(r'([.!?]\s*)', text)
            result = []
            for i, part in enumerate(sentences):
                if i % 2 == 0 and part:  # Text parts (not punctuation)
                    part = part.strip()
                    if part:
                        part = part[0].upper() + part[1:].lower()
                result.append(part)
            return ''.join(result)
        else:
            return text
    
    def normalize_punctuation(self, text: str) -> str:
        """Normalize punctuation for consistency."""
        # Standardize quotes
        text = re.sub(r'[""„"]', '"', text)
        text = re.sub(r'[''‛]', "'", text)
        
        # Standardize dashes
        text = re.sub(r'[—–]', '-', text)
        
        # Fix spacing around punctuation
        text = re.sub(r'\s+([,.;:!?])', r'\1', text)  # Remove space before
        text = re.sub(r'([,.;:!?])([^\s])', r'\1 \2', text)  # Add space after
        
        # Normalize ellipses
        text = re.sub(r'\.{3,}', '...', text)
        
        return text
    
    def standardize(self, text: str, **options) -> str:
        """
        Complete orthography standardization pipeline.
        
        Args:
            text: Input Latin text
            **options: Configuration options:
                - normalize_medieval: bool (default True)
                - expand_abbreviations: bool (default True)
                - expand_names: bool (default False)
                - classical_spelling: bool (default True)
                - remove_diacritics: bool (default True)
                - case_mode: str (default 'lowercase')
                - normalize_punctuation: bool (default True)
        """
        # Default options
        opts = {
            'normalize_medieval': True,
            'expand_abbreviations': True,
            'expand_names': False,
            'classical_spelling': True,
            'remove_diacritics': True,
            'case_mode': 'lowercase',
            'normalize_punctuation': True,
            **options
        }
        
        original_length = len(text)
        
        # Step 1: Normalize medieval variants
        if opts['normalize_medieval']:
            text = self.normalize_medieval_variants(text)
        
        # Step 2: Expand abbreviations
        if opts['expand_abbreviations']:
            text = self.expand_abbreviations(text, expand_names=opts['expand_names'])
        
        # Step 3: Classical spelling normalization
        if opts['classical_spelling']:
            text = self.normalize_classical_spelling(text)
        
        # Step 4: Remove diacritics
        if opts['remove_diacritics']:
            text = self.remove_diacritics(text)
        
        # Step 5: Normalize punctuation
        if opts['normalize_punctuation']:
            text = self.normalize_punctuation(text)
        
        # Step 6: Case normalization (do last to preserve abbreviation detection)
        if opts['case_mode'] != 'preserve':
            text = self.normalize_case(text, opts['case_mode'])
        
        # Final cleanup
        text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
        text = text.strip()
        
        # Log transformation
        compression_ratio = len(text) / original_length if original_length > 0 else 1.0
        self.logger.debug(f"Orthography standardization: {original_length} -> {len(text)} chars "
                         f"(ratio: {compression_ratio:.3f})")
        
        return text
    
    def analyze_text(self, text: str) -> Dict:
        """Analyze text for orthographic features."""
        analysis = {
            'original_length': len(text),
            'word_count': len(re.findall(r'\w+', text)),
            'has_diacritics': bool(re.search(r'[āēīōūȳăĕĭŏŭ]', text)),
            'has_j_letters': bool(re.search(r'[jJ]', text)),
            'has_v_vowels': bool(re.search(r'[aeiou][vV][aeiou]', text)),
            'abbreviation_count': 0,
            'medieval_variants': []
        }
        
        # Count abbreviations
        for pattern in self.abbreviations.keys():
            matches = re.findall(pattern, text, re.IGNORECASE)
            analysis['abbreviation_count'] += len(matches)
        
        # Find medieval variants
        for pattern, standard in self.medieval_variants.items():
            if re.search(pattern, text, re.IGNORECASE):
                analysis['medieval_variants'].append(standard)
        
        return analysis