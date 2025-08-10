#!/usr/bin/env python3
"""
Micro-scraper for testing problematic Latin Wikisource texts.

Tests a curated list of ~40-50 works that represent common issues:
- Index pages that should download chapters
- Single works that should download as-is  
- Various text structures and patterns
- Edge cases for genre classification

Uses the main scraper logic but with reduced scope for fast testing.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import json
from pathlib import Path
from vicifons_scraper_optimized import ComprehensiveVicifonsDownloader
import pywikibot

class MicroScraper:
    def __init__(self):
        self.output_dir = Path("test_downloaded_texts")
        self.output_dir.mkdir(exist_ok=True)
        
        # Curated list of problematic and representative texts
        self.test_works = [
            # Major classical works (should be indices)
            "Aeneis",
            "Commentarii de bello Gallico", 
            "Noctes Atticae",
            "Ab Urbe Condita - Periochae",
            "Metamorphoses (Ovidius)",
            "De Bello Civili (Caesar)",
            "Naturalis Historia",
            "Annales (Tacitus)",
            "Historiae (Tacitus)",
            "Germania (Tacitus)",
            "Agricola (Tacitus)",
            "Dialogus de oratoribus",
            
            # Philosophy and rhetoric  
            "De Officiis",
            "De Natura Deorum",
            "De Re Publica (Cicero)",
            "De Legibus (Cicero)",
            "De Oratore",
            "Brutus (Cicero)",
            "Orator (Cicero)",
            "Tusculanae Disputationes",
            "De Finibus Bonorum et Malorum",
            "Academica",
            "De Divinatione",
            "De Fato",
            "De Senectute",
            "De Amicitia",
            "Paradoxa Stoicorum",
            
            # Poetry collections
            "Carmina (Horatius)",
            "Satirae (Horatius)", 
            "Epistulae (Horatius)",
            "Ars Poetica",
            "Elegiae (Tibullus)",
            "Elegiae (Propertius)",
            "Carmina (Catullus)",
            "Tristia",
            "Epistulae ex Ponto",
            "Ars Amatoria",
            "Remedia Amoris",
            "Medicamina Faciei Femineae",
            "Heroides",
            "Fasti",
            
            # Later/medieval works
            "Etymologiae",
            "Historia Francorum",
            "De Gestis Regum Anglorum",
            "Chronica Majora",
            "Summa Theologica",
            "De Civitate Dei",
            "Confessiones (Augustinus)",
            "Consolatio Philosophiae",
            
            # Single chapter/shorter works
            "Cato Maior de Senectute",
            "Laelius de Amicitia", 
            "Somnium Scipionis",
            "Bellum Jugurthinum",
            "Bellum Catilinarium",
        ]
        
        # Initialize with minimal caching and concurrency for testing
        self.downloader = ComprehensiveVicifonsDownloader(
            str(self.output_dir), 
            single_folder=True,
            max_concurrent=3,  # Lower concurrency for testing
            use_cache=True,    # Keep some caching but will optimize
            cache_duration_hours=1  # Much shorter cache
        )
    
    async def test_single_work(self, work_title: str):
        """Test a single work and report results."""
        print(f"\n{'='*60}")
        print(f"TESTING: {work_title}")
        print(f"{'='*60}")
        
        try:
            # Check if page exists
            page = pywikibot.Page(self.downloader.site, work_title)
            if not page.exists():
                print(f"❌ Page does not exist: {work_title}")
                return {"title": work_title, "status": "not_found", "chapters": 0}
            
            print(f"✅ Page exists: {work_title}")
            
            # Get page text and analyze
            page_text = page.text
            is_index = self.downloader.is_index_page(page_text)
            
            if is_index:
                print(f"📋 Detected as INDEX page")
                chapters = await self.downloader.find_chapters_comprehensive(page, page_text)
                print(f"📚 Found {len(chapters)} chapters")
                
                # Show first few chapters
                for i, chapter in enumerate(chapters[:5], 1):
                    print(f"  {i}. {chapter}")
                if len(chapters) > 5:
                    print(f"  ... and {len(chapters) - 5} more")
                
                # Actually download the chapters (limited)
                downloaded = 0
                for chapter_title in chapters[:3]:  # Download first 3 for testing
                    try:
                        result = await self.downloader.download_single_page(chapter_title)
                        if result and result.get('success'):
                            downloaded += 1
                    except Exception as e:
                        print(f"  ⚠️ Failed to download {chapter_title}: {e}")
                
                print(f"📥 Downloaded {downloaded}/{min(3, len(chapters))} test chapters")
                return {"title": work_title, "status": "index", "chapters": len(chapters), "downloaded": downloaded}
                
            else:
                print(f"📄 Detected as SINGLE page")
                # Download the single work
                try:
                    result = await self.downloader.download_single_page(work_title)
                    if result and result.get('success'):
                        print(f"📥 Downloaded successfully")
                        return {"title": work_title, "status": "single", "chapters": 1, "downloaded": 1}
                    else:
                        print(f"❌ Download failed")
                        return {"title": work_title, "status": "single", "chapters": 1, "downloaded": 0}
                except Exception as e:
                    print(f"❌ Download error: {e}")
                    return {"title": work_title, "status": "single", "chapters": 1, "downloaded": 0}
                    
        except Exception as e:
            print(f"❌ Error processing {work_title}: {e}")
            return {"title": work_title, "status": "error", "chapters": 0, "downloaded": 0}
    
    async def run_micro_scraper(self):
        """Run the micro-scraper on all test works."""
        print("="*80)
        print("MICRO-SCRAPER FOR PROBLEMATIC LATIN WIKISOURCE TEXTS")
        print(f"Testing {len(self.test_works)} representative works")
        print(f"Output directory: {self.output_dir}")
        print("="*80)
        
        results = []
        
        for i, work in enumerate(self.test_works, 1):
            print(f"\n[{i}/{len(self.test_works)}]", end="")
            result = await self.test_single_work(work)
            results.append(result)
            
            # Brief pause between requests
            await asyncio.sleep(0.5)
        
        # Generate summary report
        self.generate_report(results)
        
        return results
    
    def generate_report(self, results):
        """Generate a summary report of the micro-scraper results."""
        report_file = self.output_dir / "micro_scraper_report.json"
        
        # Calculate statistics
        total = len(results)
        found = len([r for r in results if r['status'] != 'not_found'])
        indices = len([r for r in results if r['status'] == 'index'])
        singles = len([r for r in results if r['status'] == 'single'])
        errors = len([r for r in results if r['status'] == 'error'])
        
        total_chapters = sum(r['chapters'] for r in results)
        total_downloaded = sum(r['downloaded'] for r in results)
        
        summary = {
            "micro_scraper_results": {
                "total_tested": total,
                "pages_found": found,
                "pages_not_found": total - found,
                "index_pages": indices,
                "single_pages": singles, 
                "errors": errors,
                "total_chapters_identified": total_chapters,
                "total_files_downloaded": total_downloaded,
                "success_rate": f"{(total_downloaded/max(total_chapters,1)*100):.1f}%"
            },
            "detailed_results": results
        }
        
        # Save report
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        # Print summary
        print(f"\n{'='*80}")
        print("MICRO-SCRAPER SUMMARY REPORT")
        print(f"{'='*80}")
        print(f"📊 Total works tested: {total}")
        print(f"✅ Pages found: {found}")
        print(f"❌ Pages not found: {total - found}")
        print(f"📋 Index pages: {indices}")
        print(f"📄 Single pages: {singles}")
        print(f"⚠️ Errors: {errors}")
        print(f"📚 Total chapters identified: {total_chapters}")
        print(f"📥 Total files downloaded: {total_downloaded}")
        print(f"🎯 Success rate: {(total_downloaded/max(total_chapters,1)*100):.1f}%")
        print(f"📝 Full report saved to: {report_file}")
        
        # Show problematic cases
        problematic = [r for r in results if r['status'] in ['error', 'not_found'] or r['downloaded'] == 0]
        if problematic:
            print(f"\n⚠️ PROBLEMATIC CASES ({len(problematic)}):")
            for case in problematic[:10]:  # Show first 10
                print(f"  - {case['title']}: {case['status']}")


async def main():
    """Run the micro-scraper."""
    scraper = MicroScraper()
    await scraper.run_micro_scraper()


if __name__ == "__main__":
    asyncio.run(main())