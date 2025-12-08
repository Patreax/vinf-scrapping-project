#!/usr/bin/env python3
"""
PyLucene indexer for joined company data with bucketing functionality.

This script creates a Lucene index from the joined company data TSV file,
incorporating all the bucketing methods from indexer.py.
"""

import csv
import math
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

try:
    import lucene
    from java.nio.file import Paths
    from java.lang import Long, Double
    from org.apache.lucene.analysis.standard import StandardAnalyzer
    from org.apache.lucene.document import Document, Field, FieldType, StringField, TextField
    from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexOptions
    from org.apache.lucene.store import FSDirectory
    from org.apache.lucene.document import LongPoint, DoublePoint
except ImportError as e:
    print("Error: PyLucene is not properly installed or initialized.")
    print(f"Import error: {e}")
    sys.exit(1)

# query: public company technology software nasdaq founded>2000 employees>1000
class JoinedDataLuceneIndexer:
    """PyLucene indexer for joined company data with bucketing."""
    
    def __init__(self, data_file: str = "lucene/joined_company_data.tsv", 
                 index_dir: str = "lucene/joined_company_data_index"):
        self.data_file = data_file
        self.index_dir = index_dir
        self.analyzer = StandardAnalyzer()
        
    def bucket_price(self, price: str) -> Optional[str]:
        """
        Bucket stock price into categories.
        
        0 - $10: price_micro
        10 - $50: price_low
        50 - $150: price_medium
        150 - $500: price_high
        500+: price_very_high
        """
        try:
            value = float(price.replace('$', '').replace(',', ''))
            if value < 10:
                return "price_micro"
            elif value < 50:
                return "price_low"
            elif value < 150:
                return "price_medium"
            elif value < 500:
                return "price_high"
            else:
                return "price_very_high"
        except (ValueError, AttributeError):
            return None
    
    def bucket_market_cap(self, market_cap: str) -> Optional[str]:
        """
        Bucket market capitalization into categories.
        
        <$50M: cap_nano
        $50M-300M: cap_micro
        $300M-2B: cap_small
        $2B-10B: cap_mid
        $10B-200B: cap_large
        $200B+: cap_mega
        """
        try:
            value_str = market_cap.replace('USD', '').replace('$', '').strip()

            if 'B' in value_str:
                value = float(value_str.replace('B', '').strip()) * 1_000_000_000
            elif 'M' in value_str:
                value = float(value_str.replace('M', '').strip()) * 1_000_000
            else:
                value = float(value_str)
            
            if value < 50_000_000:
                return "cap_nano"
            elif value < 300_000_000:
                return "cap_micro"
            elif value < 2_000_000_000:
                return "cap_small"
            elif value < 10_000_000_000:
                return "cap_mid"
            elif value < 200_000_000_000:
                return "cap_large"
            else:
                return "cap_mega"
        except (ValueError, AttributeError):
            return None
    
    def bucket_price_change(self, change_pct: str) -> Optional[str]:
        """
        Bucket price change percentage into movement categories.
        
        <-5%: move_crash
        -5% to -2%: move_down_strong
        -2% to -0.5%: move_down_weak
        -0.5% to 0.5%: move_flat
        0.5% to 2%: move_up_weak
        2% to 5%: move_up_strong
        >5%: move_surge
        """
        try:
            value = float(change_pct.replace('%', '').replace('+', ''))
            if value < -5:
                return "move_crash"
            elif value < -2:
                return "move_down_strong"
            elif value < -0.5:
                return "move_down_weak"
            elif value <= 0.5:
                return "move_flat"
            elif value <= 2:
                return "move_up_weak"
            elif value <= 5:
                return "move_up_strong"
            else:
                return "move_surge"
        except (ValueError, AttributeError):
            return None
    
    def bucket_employees(self, employees: str) -> Optional[str]:
        """
        Bucket employee count into company size categories.
        
        <1K: size_tiny
        1K-10K: size_small
        10K-50K: size_medium
        50K-100K: size_large
        100K+: size_huge
        """
        try:
            value = int(employees.replace(',', ''))
            if value < 1_000:
                return "size_tiny"
            elif value < 10_000:
                return "size_small"
            elif value < 50_000:
                return "size_medium"
            elif value < 100_000:
                return "size_large"
            else:
                return "size_huge"
        except (ValueError, AttributeError):
            return None
    
    def bucket_revenue(self, revenue: str) -> Optional[str]:
        """
        Bucket revenue into categories.
        
        <$100M: rev_startup
        $100M-1B: rev_small
        $1B-10B: rev_medium
        $10B-50B: rev_large
        $50B+: rev_huge
        """
        try:
            value_str = revenue.replace('$', '').strip()

            if 'B' in value_str:
                value = float(value_str.replace('B', '').strip()) * 1_000_000_000
            elif 'M' in value_str:
                value = float(value_str.replace('M', '').strip()) * 1_000_000
            else:
                value = float(value_str)
            
            if value < 100_000_000:
                return "rev_startup"
            elif value < 1_000_000_000:
                return "rev_small"
            elif value < 10_000_000_000:
                return "rev_medium"
            elif value < 50_000_000_000:
                return "rev_large"
            else:
                return "rev_huge"
        except (ValueError, AttributeError):
            return None
    
    def extract_year_from_founded(self, founded: str) -> Optional[str]:
        """
        Extract year from founded field and bucket by decade.
        
        Examples:
        1900-1909: founded_1900s
        1910-1919: founded_1910s
        ...
        2020-2029: founded_2020s
        """
        try:
            founded = founded.strip()
            if not founded:
                return None
            
            # Try to find a 4-digit year in the string
            year_match = re.search(r'\b(\d{4})\b', founded)
            
            if year_match:
                year = int(year_match.group(1))
                # Bucket by decade
                decade = (year // 10) * 10
                return f"founded_{decade}s"
            
            return None
        except (ValueError, AttributeError):
            return None
    
    def parse_numeric_price(self, price: str) -> Optional[float]:
        """Parse price string (e.g., '$25', '$476.7') to float."""
        try:
            return float(price.replace('$', '').replace(',', '').strip())
        except (ValueError, AttributeError):
            return None
    
    def parse_numeric_percentage_change(self, change_pct: str) -> Optional[float]:
        """Parse percentage change string (e.g., '+0.16%', '-5.2%') to float."""
        try:
            return float(change_pct.replace('%', '').replace('+', '').strip())
        except (ValueError, AttributeError):
            return None
    
    def parse_numeric_market_cap(self, market_cap: str) -> Optional[float]:
        """Parse market cap string (e.g., '2.34B USD', '99.25B INR') to float in USD."""
        try:
            value_str = market_cap.replace('USD', '').replace('INR', '').replace('$', '').strip()
            if not value_str:
                return None
            
            if 'B' in value_str:
                value = float(value_str.replace('B', '').strip()) * 1_000_000_000
            elif 'M' in value_str:
                value = float(value_str.replace('M', '').strip()) * 1_000_000
            else:
                value = float(value_str)
            
            # Convert INR to USD (approximate rate, adjust if needed)
            if 'INR' in market_cap:
                value = value / 83.0  # Approximate conversion rate
            
            return value
        except (ValueError, AttributeError):
            return None
    
    def parse_numeric_employees(self, employees: str) -> Optional[int]:
        """Parse employees string (e.g., '388', '32,100') to int."""
        try:
            return int(employees.replace(',', '').strip())
        except (ValueError, AttributeError):
            return None
    
    def parse_numeric_founded(self, founded: str) -> Optional[int]:
        """Parse founded year string (e.g., '1933') to int."""
        try:
            founded = founded.strip()
            if not founded:
                return None
            
            # Try to find a 4-digit year in the string
            year_match = re.search(r'\b(\d{4})\b', founded)
            if year_match:
                return int(year_match.group(1))
            return None
        except (ValueError, AttributeError):
            return None
    
    def parse_numeric_revenue(self, revenue: str) -> Optional[float]:
        """Parse revenue string (e.g., '4.09B', '$100M') to float in USD."""
        try:
            value_str = revenue.replace('$', '').strip()
            if not value_str:
                return None
            
            if 'B' in value_str:
                value = float(value_str.replace('B', '').strip()) * 1_000_000_000
            elif 'M' in value_str:
                value = float(value_str.replace('M', '').strip()) * 1_000_000
            else:
                value = float(value_str)
            
            return value
        except (ValueError, AttributeError):
            return None
    
    def parse_numeric_ebitda(self, ebitda: str) -> Optional[float]:
        """Parse EBITDA string (e.g., '990.61M', '$50B') to float in USD."""
        try:
            value_str = ebitda.replace('$', '').strip()
            if not value_str:
                return None
            
            if 'B' in value_str:
                value = float(value_str.replace('B', '').strip()) * 1_000_000_000
            elif 'M' in value_str:
                value = float(value_str.replace('M', '').strip()) * 1_000_000
            else:
                value = float(value_str)
            
            return value
        except (ValueError, AttributeError):
            return None
    
    def parse_timestamp_to_epoch(self, timestamp_str: str) -> Optional[int]:
        """Parse timestamp string (e.g., '2025-10-17 13:31:13') to Unix epoch seconds."""
        try:
            if not timestamp_str:
                return None
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            return int(dt.timestamp())
        except (ValueError, AttributeError):
            return None
    
    def create_document(self, row: Dict[str, str]) -> Document:
        """
        Create a Lucene document from a row of joined data.
        """
        doc = Document()
        
        # Company name (searchable)
        company = row.get('company', '').strip()
        if company:
            doc.add(TextField("company", company, Field.Store.YES))
        
        # Symbol (exact match, stored)
        symbol = row.get('symbol', '').strip()
        if symbol:
            doc.add(StringField("symbol", symbol.lower(), Field.Store.YES))
            # Also add as searchable term with prefix
            doc.add(TextField("symbol_search", f"symbol_{symbol.lower()}", Field.Store.NO))
        
        # Exchange (exact match, stored)
        exchange = row.get('exchange', '').strip()
        if exchange:
            doc.add(StringField("exchange", exchange.lower(), Field.Store.YES))
            doc.add(TextField("exchange_search", f"exchange_{exchange.lower()}", Field.Store.NO))
        
        # Store timestamp (needed for recency weighting)
        if row.get('timestamp'):
            doc.add(StringField("timestamp", row.get('timestamp'), Field.Store.YES))
            # Also store as numeric for range queries (Unix epoch seconds)
            timestamp_epoch = self.parse_timestamp_to_epoch(row.get('timestamp'))
            if timestamp_epoch is not None:
                doc.add(LongPoint("timestamp_numeric", timestamp_epoch))
        
        # Store website
        if row.get('website'):
            doc.add(StringField("website", row.get('website'), Field.Store.YES))
        
        # Additional searchable text fields
        if row.get('industries'):
            doc.add(TextField("industries", row.get('industries'), Field.Store.YES))
        if row.get('founders'):
            doc.add(TextField("founders", row.get('founders'), Field.Store.YES))
        if row.get('headquarters'):
            doc.add(TextField("headquarters", row.get('headquarters'), Field.Store.YES))
        if row.get('title'):
            doc.add(TextField("title", row.get('title'), Field.Store.YES))
        if row.get('keyword'):
            doc.add(StringField("keyword", row.get('keyword'), Field.Store.YES))
        if row.get('first_paragraph'):
            doc.add(TextField("first_paragraph", row.get('first_paragraph'), Field.Store.YES))
        if row.get('description'):
            doc.add(TextField("description", row.get('description'), Field.Store.YES))
        
        # Bucketed fields (all searchable)
        price_bucket = self.bucket_price(row.get('current_price', ''))
        if price_bucket:
            doc.add(StringField("price_bucket", price_bucket, Field.Store.YES))
        
        # Store numeric price for range queries
        price_numeric = self.parse_numeric_price(row.get('current_price', ''))
        if price_numeric is not None:
            doc.add(DoublePoint("current_price_numeric", price_numeric))
        
        cap_bucket = self.bucket_market_cap(row.get('market_cap', ''))
        if cap_bucket:
            doc.add(StringField("cap_bucket", cap_bucket, Field.Store.YES))
        
        # Store numeric market cap for range queries
        market_cap_numeric = self.parse_numeric_market_cap(row.get('market_cap', ''))
        if market_cap_numeric is not None:
            doc.add(DoublePoint("market_cap_numeric", market_cap_numeric))
        
        change_bucket = self.bucket_price_change(row.get('calculated_percentage_change', ''))
        if change_bucket:
            doc.add(StringField("change_bucket", change_bucket, Field.Store.YES))
        
        # Store numeric percentage change for range queries
        change_numeric = self.parse_numeric_percentage_change(row.get('calculated_percentage_change', ''))
        if change_numeric is not None:
            doc.add(DoublePoint("calculated_percentage_change_numeric", change_numeric))
        
        emp_bucket = self.bucket_employees(row.get('employees', ''))
        if emp_bucket:
            doc.add(StringField("size_bucket", emp_bucket, Field.Store.YES))
        
        # Store numeric employees for range queries
        employees_numeric = self.parse_numeric_employees(row.get('employees', ''))
        if employees_numeric is not None:
            doc.add(LongPoint("employees_numeric", employees_numeric))
        
        rev_bucket = self.bucket_revenue(row.get('revenue', ''))
        if rev_bucket:
            doc.add(StringField("rev_bucket", rev_bucket, Field.Store.YES))
        
        # Store numeric revenue for range queries
        revenue_numeric = self.parse_numeric_revenue(row.get('revenue', ''))
        if revenue_numeric is not None:
            doc.add(DoublePoint("revenue_numeric", revenue_numeric))
        
        founded_bucket = self.extract_year_from_founded(row.get('founded', ''))
        if founded_bucket:
            doc.add(StringField("founded_bucket", founded_bucket, Field.Store.YES))
        
        # Store numeric founded year for range queries
        founded_numeric = self.parse_numeric_founded(row.get('founded', ''))
        if founded_numeric is not None:
            doc.add(LongPoint("founded_numeric", founded_numeric))
        
        # Store numeric EBITDA for range queries
        ebitda_numeric = self.parse_numeric_ebitda(row.get('ebitda', ''))
        if ebitda_numeric is not None:
            doc.add(DoublePoint("ebitda_numeric", ebitda_numeric))
        
        # Combined searchable content field (for general search across whole document)
        # This field contains ALL searchable content from the document
        searchable_content = []
        if company:
            searchable_content.append(company)
        if symbol:
            searchable_content.append(symbol)
            searchable_content.append(f"symbol_{symbol.lower()}")  # Include prefixed version
        if exchange:
            searchable_content.append(exchange)
            searchable_content.append(f"exchange_{exchange.lower()}")  # Include prefixed version
        if row.get('industries'):
            searchable_content.append(row.get('industries'))
        if row.get('founders'):
            searchable_content.append(row.get('founders'))
        if row.get('headquarters'):
            searchable_content.append(row.get('headquarters'))
        if row.get('title'):
            searchable_content.append(row.get('title'))
        if row.get('keyword'):
            searchable_content.append(row.get('keyword'))
        if row.get('first_paragraph'):
            searchable_content.append(row.get('first_paragraph'))
        if row.get('description'):
            searchable_content.append(row.get('description'))
        
        # Add all bucketed terms to searchable content
        for bucket in [price_bucket, cap_bucket, change_bucket, emp_bucket, rev_bucket, founded_bucket]:
            if bucket:
                searchable_content.append(bucket)
        
        if searchable_content:
            doc.add(TextField("content", " ".join(searchable_content), Field.Store.NO))
        
        return doc
    
    def build_index(self):
        """Build Lucene index from the joined data file."""
        print(f"Loading data from {self.data_file}...")
        
        # Create index directory if it doesn't exist
        index_path = Paths.get(self.index_dir)
        Path(self.index_dir).mkdir(parents=True, exist_ok=True)
        
        # Open index directory
        directory = FSDirectory.open(index_path)
        
        # Configure index writer
        config = IndexWriterConfig(self.analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)  # Create new index
        
        writer = IndexWriter(directory, config)
        
        try:
            # Read TSV file and index documents
            with open(self.data_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                
                doc_count = 0
                for row in reader:
                    doc = self.create_document(row)
                    writer.addDocument(doc)
                    doc_count += 1
                    
                    if doc_count % 1000 == 0:
                        print(f"Indexed {doc_count} documents...")
                
                print(f"Indexed {doc_count} documents total")
            
            # Commit and close
            writer.commit()
            writer.close()
            print(f"\n✓ Index built successfully!")
            print(f"  Index location: {self.index_dir}")
            print(f"  Total documents: {doc_count}")
            
        except Exception as e:
            print(f"Error building index: {e}")
            writer.rollback()
            writer.close()
            raise


class JoinedDataLuceneSearcher:
    """
    PyLucene searcher for joined company data with field boosting support.
    
    Field boosting allows you to assign different importance weights to different fields.
    Higher weights mean matches in those fields will rank higher in search results.
    
    Example usage to customize field weights:
        searcher = JoinedDataLuceneSearcher()
        searcher.set_field_weight('company', 2.0)  # Company name matches are 2x more important
        searcher.set_field_weight('description', 1.5)  # Description matches are 1.5x more important
        searcher.set_field_weight('keyword', 0.5)  # Keyword matches are less important
    
    Or set multiple weights at once:
        searcher.set_field_weights({
            'company': 2.0,
            'title': 1.8,
            'first_paragraph': 1.5,
            'description': 1.2,
            'industries': 1.0,
            'keyword': 0.8
        })
    """
    
    def __init__(self, index_dir: str = "lucene/joined_company_data_index", 
                 field_weights: Optional[Dict[str, float]] = None,
                 half_life_days: float = 62.0,
                 recency_punishment_factor: float = 10.0):
        self.index_dir = index_dir
        self.analyzer = StandardAnalyzer()
        self.directory = None
        self.reader = None
        self.searcher = None
        
        # Recency weighting parameters
        self.half_life_days = half_life_days
        self.recency_punishment_factor = recency_punishment_factor
        
        self.field_weights = {
            'company': 1.3,  # Company name matches are very important
            'symbol_search': 1.3,  # Symbol matches are very important (e.g., "AAPL" for Apple)
            'title': 1.3,  # Title matches are important
            'keyword': 1.1,  # Keywords are moderately important
            'industries': 1.2,  # Industry matches help with categorization
            'founders': 1.0,  # Founder names are useful but less critical
            'head quarters': 0.8,  # Location is less important for search relevance
            'description': 0.6,  # Less useful since it can have false matches
            'first_paragraph': 0.6,  # Less useful since it can have false matches
            'price_bucket': 1.0,  # Bucketed fields are useful for filtering
            'cap_bucket': 1.0,  # Bucketed fields are useful for filtering
            'change_bucket': 1.0,  # Bucketed fields are useful for filtering
            'size_bucket': 1.0,  # Bucketed fields are useful for filtering
            'rev_bucket': 1.0,  # Bucketed fields are useful for filtering
            'founded_bucket': 1.0,  # Bucketed fields are useful for filtering
            'exchange_search': 0.8,  # Exchange is less important for relevance
        }
        
        # Override with custom weights if provided
        if field_weights:
            self.field_weights.update(field_weights)
        
    def open_index(self):
        """Open the Lucene index for searching."""
        try:
            from org.apache.lucene.index import DirectoryReader
            from org.apache.lucene.search import IndexSearcher
            
            index_path = Paths.get(self.index_dir)
            self.directory = FSDirectory.open(index_path)
            self.reader = DirectoryReader.open(self.directory)
            self.searcher = IndexSearcher(self.reader)
            return True
        except Exception as e:
            print(f"Error opening index: {e}")
            return False
    
    def close_index(self):
        """Close the Lucene index."""
        try:
            if self.reader:
                self.reader.close()
            if self.directory:
                self.directory.close()
        except Exception:
            pass
    
    def set_field_weight(self, field: str, weight: float):
        """Set the boost weight for a specific field."""
        if field in self.field_weights:
            self.field_weights[field] = weight
        else:
            print(f"Warning: Field '{field}' not found in field_weights. Available fields: {list(self.field_weights.keys())}")
    
    def set_field_weights(self, weights: Dict[str, float]):
        """Set multiple field weights at once."""
        for field, weight in weights.items():
            self.set_field_weight(field, weight)
    
    def get_field_weights(self) -> Dict[str, float]:
        """Get current field weights."""
        return self.field_weights.copy()
    
    def print_field_weights(self):
        """Print current field weights."""
        print("\n" + "=" * 100)
        print("FIELD WEIGHTS (Boosts)")
        print("=" * 100)
        for field, weight in sorted(self.field_weights.items()):
            print(f"  {field:20s}: {weight:.2f}")
        print("=" * 100)
    
    def _extract_timestamp_from_filename(self, source_file: str) -> Optional[str]:
        """
        Extract timestamp from source_file filename.
        
        Filename format: SYMBOL_YYYYMMDD_HHMMSS.html
        Example: SCHJ_20251017_133113.html -> 2025-10-17 13:31:13
        
        Args:
            source_file: Path to source file, e.g., '/path/to/SCHJ_20251017_133113.html'
        
        Returns:
            Timestamp string in format '%Y-%m-%d %H:%M:%S' or None if extraction fails
        """
        if not source_file:
            return None
        
        try:
            # Extract filename from path
            filename = source_file.split('/')[-1]
            # Remove .html extension
            basename = filename.replace('.html', '')
            
            # Split by underscore: SYMBOL_YYYYMMDD_HHMMSS
            parts = basename.split('_')
            if len(parts) >= 3:
                date_str = parts[1]  # YYYYMMDD
                time_str = parts[2]   # HHMMSS
                
                # Parse date and time
                if len(date_str) == 8 and len(time_str) == 6:
                    year = date_str[0:4]
                    month = date_str[4:6]
                    day = date_str[6:8]
                    hour = time_str[0:2]
                    minute = time_str[2:4]
                    second = time_str[4:6]
                    
                    timestamp_str = f"{year}-{month}-{day} {hour}:{minute}:{second}"
                    # Validate by trying to parse it
                    datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    return timestamp_str
        except Exception:
            pass
        
        return None
    
    def _compute_recency_weight(self, timestamp_str: str, source_file: str = None) -> float:
        """
        Compute exponential decay weight for a document based on timestamp.
        
        Weight function uses half-life with punishment factor:
        weight = exp(-ln2 * recency_punishment_factor * (age_days / half_life_days))
        
        The recency_punishment_factor controls how aggressively older records are penalized:
        - factor = 1.0: Standard exponential decay (default)
        - factor > 1.0: More aggressive penalty for older records (e.g., 1.5, 2.0)
        - factor < 1.0: Less aggressive penalty (e.g., 0.5)
        
        Documents without a valid timestamp receive weight 1.0.
        
        If timestamp_str is not valid, tries to extract timestamp from source_file filename.
        
        Args:
            timestamp_str: Timestamp string in format '%Y-%m-%d %H:%M:%S'
            source_file: Optional source file path to extract timestamp from if timestamp_str is invalid
        
        Returns:
            Weight in [0, 1] range
        """
        # Try to use provided timestamp first
        if timestamp_str:
            try:
                now = datetime.now()
                ts = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                age_days = max((now - ts).total_seconds() / 86400.0, 0.0)
                
                if self.half_life_days > 0:
                    ln2 = math.log(2)
                    weight = math.exp(-ln2 * self.recency_punishment_factor * (age_days / self.half_life_days))
                else:
                    weight = 1.0
                
                return float(max(min(weight, 1.0), 0.0))
            except Exception:
                pass
        
        # If timestamp_str is invalid, try to extract from source_file
        if source_file:
            extracted_timestamp = self._extract_timestamp_from_filename(source_file)
            if extracted_timestamp:
                try:
                    now = datetime.now()
                    ts = datetime.strptime(extracted_timestamp, '%Y-%m-%d %H:%M:%S')
                    age_days = max((now - ts).total_seconds() / 86400.0, 0.0)
                    
                    if self.half_life_days > 0:
                        ln2 = math.log(2)
                        weight = math.exp(-ln2 * self.recency_punishment_factor * (age_days / self.half_life_days))
                    else:
                        weight = 1.0
                    
                    return float(max(min(weight, 1.0), 0.0))
                except Exception:
                    pass
        
        # Default weight if no valid timestamp found
        return 1.0
    
    def _deduplicate_by_symbol(self, results: List[tuple]) -> List[tuple]:
        """
        Deduplicate results by company symbol, keeping the best (highest scoring) result per symbol.
        
        Args:
            results: List of (doc_id, score, document_dict) tuples
        
        Returns:
            Deduplicated list of results, keeping only the best result per unique symbol
        """
        if not results:
            return results
        
        # Group by symbol, keeping the best (highest scoring) result for each symbol
        symbol_to_best_result = {}
        
        for result in results:
            doc_id, score, doc_dict = result
            symbol = doc_dict.get('symbol', '').lower().strip() if doc_dict.get('symbol') else ''
            
            # Use symbol as key, or fallback to company name if symbol is missing
            if not symbol:
                symbol = doc_dict.get('company', '').lower().strip() if doc_dict.get('company') else ''
            
            # Skip if we still don't have a key
            if not symbol:
                continue
            
            # Keep the result with the highest score for this symbol
            if symbol not in symbol_to_best_result:
                symbol_to_best_result[symbol] = result
            else:
                # Compare scores and keep the better one
                existing_score = symbol_to_best_result[symbol][1]
                if score > existing_score:
                    symbol_to_best_result[symbol] = result
        
        # Convert back to list and sort by score
        deduplicated = list(symbol_to_best_result.values())
        deduplicated.sort(key=lambda x: x[1], reverse=True)
        
        return deduplicated
    
    def search(self, query_str: str, top_k: int = 5, require_all_terms: bool = False) -> List[tuple]:
        """
        Search the Lucene index across multiple fields with field-specific boosts.
        
        This searches across individual fields (company, title, description, etc.) with
        different importance weights, prioritizing company name matches over other fields.
        
        Returns a list of (doc_id, score, document_dict) tuples.
        """
        if not self.searcher:
            if not self.open_index():
                print("Error: Could not open index")
                return []
        
        # Check index has documents
        if self.reader.numDocs() == 0:
            print("Warning: Index contains no documents")
            return []
        
        try:
            from org.apache.lucene.queryparser.classic import QueryParser
            from org.apache.lucene.search import BooleanQuery, BooleanClause, BoostQuery
            
            # Build query string based on require_all_terms
            query_terms = query_str.strip().split()
            
            if not query_terms:
                return []
            
            # If require_all_terms is True, use AND operator; otherwise use OR
            if require_all_terms:
                # Join terms with AND to require all terms
                query_string = " AND ".join(query_terms)
            else:
                # Join terms with OR to match any term
                query_string = " OR ".join(query_terms)
            
            # Build a BooleanQuery that searches across multiple fields with boosts
            boolean_query_builder = BooleanQuery.Builder()
            
            # Get list of fields to search and their boosts
            # Only include fields that exist in field_weights
            for field_name, weight in self.field_weights.items():
                # Skip the 'content' field - we'll search individual fields instead
                if field_name != 'content':
                    # Create a QueryParser for this specific field
                    field_parser = QueryParser(field_name, self.analyzer)
                    try:
                        # Parse the query for this field
                        field_query = field_parser.parse(query_string)
                        # Apply boost and add to BooleanQuery with SHOULD (OR) clause
                        boosted_query = BoostQuery(field_query, float(weight))
                        boolean_query_builder.add(boosted_query, BooleanClause.Occur.SHOULD)
                    except Exception:
                        # If parsing fails for this field, skip it
                        pass
            
            # Build the final BooleanQuery
            query = boolean_query_builder.build()
            
            # Fetch more results to ensure diversity (get unique companies)
            # Fetch 10x more results to ensure we have enough unique companies
            fetch_count = max(top_k * 10, 50)
            top_docs = self.searcher.search(query, fetch_count)
            
            # Extract results
            results = []
            stored_fields = self.searcher.storedFields()
            for score_doc in top_docs.scoreDocs:
                doc_id = score_doc.doc
                lucene_score = score_doc.score
                doc = stored_fields.document(doc_id)
                
                # Convert Lucene document to dictionary
                doc_dict = {}
                for field in doc.getFields():
                    field_name = field.name()
                    field_value = doc.get(field_name)
                    if field_value:
                        doc_dict[field_name] = field_value
                
                # Apply recency weighting
                # Get timestamp from timestamp field
                timestamp_str = doc_dict.get('timestamp', '')
                recency_weight = self._compute_recency_weight(timestamp_str, None)
                final_score = lucene_score * recency_weight
                
                results.append((doc_id, final_score, doc_dict))
            
            # Re-sort by final score (recency-weighted)
            results.sort(key=lambda x: x[1], reverse=True)
            
            # Return top_k results (may include same company multiple times if they rank high)
            return results[:top_k]
            
        except Exception as e:
            print(f"Search error: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def range_query(self, range_filters: Dict[str, Dict[str, Optional[float]]], 
                    top_k: int = 5) -> List[tuple]:
        """
        Execute range queries on numeric fields.
        
        Args:
            range_filters: Dictionary mapping field names to range specifications.
                          Each range spec is a dict with optional 'min' and 'max' keys.
                          Supported fields:
                          - 'timestamp' (Unix epoch seconds)
                          - 'current_price' (float)
                          - 'calculated_percentage_change' (float)
                          - 'market_cap' (float, in USD)
                          - 'founded' (int, year)
                          - 'employees' (int)
                          - 'revenue' (float, in USD)
                          - 'ebitda' (float, in USD)
            top_k: Maximum number of results to return
        
        Returns:
            List of (doc_id, score, document_dict) tuples
        
        Example:
            # Find companies with employees > 1000 and < 50000
            results = searcher.range_query({
                'employees': {'min': 1000, 'max': 50000}
            })
            
            # Find companies with market cap > 1B and revenue > 100M
            results = searcher.range_query({
                'market_cap': {'min': 1_000_000_000},
                'revenue': {'min': 100_000_000}
            })
        """
        if not self.searcher:
            if not self.open_index():
                print("Error: Could not open index")
                return []
        
        # Check index has documents
        if self.reader.numDocs() == 0:
            print("Warning: Index contains no documents")
            return []
        
        try:
            from org.apache.lucene.search import BooleanQuery, BooleanClause
            
            # Map user-friendly field names to internal numeric field names
            field_mapping = {
                'timestamp': 'timestamp_numeric',
                'current_price': 'current_price_numeric',
                'calculated_percentage_change': 'calculated_percentage_change_numeric',
                'market_cap': 'market_cap_numeric',
                'founded': 'founded_numeric',
                'employees': 'employees_numeric',
                'revenue': 'revenue_numeric',
                'ebitda': 'ebitda_numeric'
            }
            
            # Build BooleanQuery with range filters
            boolean_query_builder = BooleanQuery.Builder()
            
            for field_name, range_spec in range_filters.items():
                if field_name not in field_mapping:
                    print(f"Warning: Unknown field '{field_name}'. Supported fields: {list(field_mapping.keys())}")
                    continue
                
                internal_field = field_mapping[field_name]
                min_val = range_spec.get('min')
                max_val = range_spec.get('max')
                
                if min_val is None and max_val is None:
                    continue  # Skip empty range specs
                
                # Determine if this is a LongPoint or DoublePoint field
                is_long_field = field_name in ['timestamp', 'founded', 'employees']
                
                if is_long_field:
                    # Use LongPoint for integer fields
                    min_long = int(min_val) if min_val is not None else None
                    max_long = int(max_val) if max_val is not None else None
                    
                    if min_long is not None and max_long is not None:
                        range_query = LongPoint.newRangeQuery(internal_field, min_long, max_long)
                    elif min_long is not None:
                        range_query = LongPoint.newRangeQuery(internal_field, min_long, Long.MAX_VALUE)
                    elif max_long is not None:
                        range_query = LongPoint.newRangeQuery(internal_field, Long.MIN_VALUE, max_long)
                    else:
                        continue
                else:
                    # Use DoublePoint for float fields
                    min_double = float(min_val) if min_val is not None else None
                    max_double = float(max_val) if max_val is not None else None
                    
                    if min_double is not None and max_double is not None:
                        range_query = DoublePoint.newRangeQuery(internal_field, min_double, max_double)
                    elif min_double is not None:
                        range_query = DoublePoint.newRangeQuery(internal_field, min_double, Double.MAX_VALUE)
                    elif max_double is not None:
                        range_query = DoublePoint.newRangeQuery(internal_field, Double.MIN_VALUE, max_double)
                    else:
                        continue
                
                # Add range query as MUST clause (AND condition)
                boolean_query_builder.add(range_query, BooleanClause.Occur.MUST)
            
            # Build the final query
            query = boolean_query_builder.build()
            
            # Fetch more results to ensure diversity (get unique companies)
            fetch_count = max(top_k * 5, 50)
            top_docs = self.searcher.search(query, fetch_count)
            
            # Extract results
            results = []
            stored_fields = self.searcher.storedFields()
            for score_doc in top_docs.scoreDocs:
                doc_id = score_doc.doc
                lucene_score = score_doc.score
                doc = stored_fields.document(doc_id)
                
                # Convert Lucene document to dictionary
                doc_dict = {}
                for field in doc.getFields():
                    field_name = field.name()
                    field_value = doc.get(field_name)
                    if field_value:
                        doc_dict[field_name] = field_value
                
                # Apply recency weighting
                timestamp_str = doc_dict.get('timestamp', '')
                recency_weight = self._compute_recency_weight(timestamp_str, None)
                final_score = lucene_score * recency_weight
                
                results.append((doc_id, final_score, doc_dict))
            
            # Re-sort by final score (recency-weighted)
            results.sort(key=lambda x: x[1], reverse=True)
            
            # Return top_k results (may include same company multiple times if they rank high)
            return results[:top_k]
            
        except Exception as e:
            print(f"Range query error: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def search_with_range_filters(self, query_str: str, range_filters: Dict[str, Dict[str, Optional[float]]] = None,
                                  top_k: int = 5, require_all_terms: bool = False) -> List[tuple]:
        """
        Combine text search with range query filters.
        
        Args:
            query_str: Text query string to search across fields
            range_filters: Optional dictionary of range filters (same format as range_query)
            top_k: Maximum number of results to return
            require_all_terms: If True, all query terms must match (AND); otherwise any term matches (OR)
        
        Returns:
            List of (doc_id, score, document_dict) tuples
        
        Example:
            # Search for "tech" companies with employees > 1000
            results = searcher.search_with_range_filters(
                "tech",
                range_filters={'employees': {'min': 1000}}
            )
        """
        if not self.searcher:
            if not self.open_index():
                print("Error: Could not open index")
                return []
        
        # Check index has documents
        if self.reader.numDocs() == 0:
            print("Warning: Index contains no documents")
            return []
        
        try:
            from org.apache.lucene.queryparser.classic import QueryParser
            from org.apache.lucene.search import BooleanQuery, BooleanClause, BoostQuery
            
            # Build text search query (same as search method)
            query_terms = query_str.strip().split()
            if not query_terms:
                # If no text query, just do range query
                if range_filters:
                    return self.range_query(range_filters, top_k)
                return []
            
            if require_all_terms:
                query_string = " AND ".join(query_terms)
            else:
                query_string = " OR ".join(query_terms)
            
            boolean_query_builder = BooleanQuery.Builder()
            
            # Add text search clauses
            for field_name, weight in self.field_weights.items():
                if field_name != 'content':
                    field_parser = QueryParser(field_name, self.analyzer)
                    try:
                        field_query = field_parser.parse(query_string)
                        boosted_query = BoostQuery(field_query, float(weight))
                        boolean_query_builder.add(boosted_query, BooleanClause.Occur.SHOULD)
                    except Exception:
                        pass
            
            # Add range filter clauses if provided
            if range_filters:
                field_mapping = {
                    'timestamp': 'timestamp_numeric',
                    'current_price': 'current_price_numeric',
                    'calculated_percentage_change': 'calculated_percentage_change_numeric',
                    'market_cap': 'market_cap_numeric',
                    'founded': 'founded_numeric',
                    'employees': 'employees_numeric',
                    'revenue': 'revenue_numeric',
                    'ebitda': 'ebitda_numeric'
                }
                
                for field_name, range_spec in range_filters.items():
                    if field_name not in field_mapping:
                        continue
                    
                    internal_field = field_mapping[field_name]
                    min_val = range_spec.get('min')
                    max_val = range_spec.get('max')
                    
                    if min_val is None and max_val is None:
                        continue
                    
                    is_long_field = field_name in ['timestamp', 'founded', 'employees']
                    
                    if is_long_field:
                        min_long = int(min_val) if min_val is not None else None
                        max_long = int(max_val) if max_val is not None else None
                        
                        if min_long is not None and max_long is not None:
                            range_query = LongPoint.newRangeQuery(internal_field, min_long, max_long)
                        elif min_long is not None:
                            range_query = LongPoint.newRangeQuery(internal_field, min_long, Long.MAX_VALUE)
                        elif max_long is not None:
                            range_query = LongPoint.newRangeQuery(internal_field, Long.MIN_VALUE, max_long)
                        else:
                            continue
                    else:
                        min_double = float(min_val) if min_val is not None else None
                        max_double = float(max_val) if max_val is not None else None
                        
                        if min_double is not None and max_double is not None:
                            range_query = DoublePoint.newRangeQuery(internal_field, min_double, max_double)
                        elif min_double is not None:
                            range_query = DoublePoint.newRangeQuery(internal_field, min_double, Double.MAX_VALUE)
                        elif max_double is not None:
                            range_query = DoublePoint.newRangeQuery(internal_field, Double.MIN_VALUE, max_double)
                        else:
                            continue
                    
                    boolean_query_builder.add(range_query, BooleanClause.Occur.MUST)
            
            # Build and execute query
            query = boolean_query_builder.build()
            
            # Fetch more results to ensure diversity (get unique companies)
            fetch_count = max(top_k * 5, 50)
            top_docs = self.searcher.search(query, fetch_count)
            
            # Extract results
            results = []
            stored_fields = self.searcher.storedFields()
            for score_doc in top_docs.scoreDocs:
                doc_id = score_doc.doc
                lucene_score = score_doc.score
                doc = stored_fields.document(doc_id)
                
                doc_dict = {}
                for field in doc.getFields():
                    field_name = field.name()
                    field_value = doc.get(field_name)
                    if field_value:
                        doc_dict[field_name] = field_value
                
                timestamp_str = doc_dict.get('timestamp', '')
                recency_weight = self._compute_recency_weight(timestamp_str, None)
                final_score = lucene_score * recency_weight
                
                results.append((doc_id, final_score, doc_dict))
            
            results.sort(key=lambda x: x[1], reverse=True)
            
            # Return top_k results (may include same company multiple times if they rank high)
            return results[:top_k]
            
        except Exception as e:
            print(f"Search with range filters error: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def display_results(self, results: List[tuple]):
        """Display search results in a readable format."""
        if not results:
            print("No results found.")
            return
        
        print(f"\nFound {len(results)} results:\n")
        print("-" * 100)
        
        for rank, (doc_id, score, doc) in enumerate(results, 1):
            company = doc.get('company', 'N/A')
            symbol = doc.get('symbol', 'N/A')
            exchange = doc.get('exchange', 'N/A')
            timestamp = doc.get('timestamp', 'N/A')
            
            # Get bucketed values instead of original values
            price_bucket = doc.get('price_bucket', 'N/A')
            change_bucket = doc.get('change_bucket', 'N/A')
            cap_bucket = doc.get('cap_bucket', 'N/A')
            founded_bucket = doc.get('founded_bucket', 'N/A')
            rev_bucket = doc.get('rev_bucket', 'N/A')
            size_bucket = doc.get('size_bucket', 'N/A')
            
            print(f"{rank}. {company} ({symbol})")
            print(f"   Exchange: {exchange} | Price Bucket: {price_bucket} | Change Bucket: {change_bucket}")
            print(f"   Market Cap Bucket: {cap_bucket} | Founded Bucket: {founded_bucket}")
            print(f"   Revenue Bucket: {rev_bucket} | Size Bucket: {size_bucket}")
            print(f"   Timestamp: {timestamp}")
            print(f"   Relevance Score: {score:.4f}")
            print("-" * 100)
    
    def print_statistics(self):
        """Print index statistics."""
        if not self.reader:
            if not self.open_index():
                return
        
        try:
            num_docs = self.reader.numDocs()
            print("\n" + "=" * 100)
            print("LUCENE INDEX STATISTICS")
            print("=" * 100)
            print(f"Total documents: {num_docs}")
            print(f"Index location: {self.index_dir}")
            print(f"Recency weighting: half_life_days = {self.half_life_days}, punishment_factor = {self.recency_punishment_factor}")
            
            # Try to get field names and sample content from a document
            if num_docs > 0 and self.searcher:
                try:
                    stored_fields = self.searcher.storedFields()
                    sample_doc = stored_fields.document(0)
                    fields = {}
                    doc_fields = sample_doc.getFields()
                    for field in doc_fields:
                        field_name = field.name()
                        field_value = sample_doc.get(field_name)
                        if field_value:
                            # Truncate long values for display
                            display_value = field_value[:100] + "..." if len(field_value) > 100 else field_value
                            fields[field_name] = display_value
                    
                    print(f"\nSample document fields ({len(fields)} fields):")
                    for field_name in sorted(fields.keys()):
                        value = fields[field_name]
                        print(f"  {field_name:20s}: {value}")
                except Exception as e:
                    print(f"  Error reading sample document: {e}")
            
            print("=" * 100)
        except Exception as e:
            print(f"Error getting statistics: {e}")


def main():
    """Main function to build the Lucene index."""
    # Initialize JVM for PyLucene
    try:
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        print("JVM initialized for PyLucene")
    except Exception as e:
        print(f"Error initializing JVM: {e}")
    
    # Default data file
    data_file = "lucene/joined_company_data.tsv"
    index_dir = "lucene/joined_company_data_index"
    
    # Allow command-line override
    if len(sys.argv) > 1:
        data_file = sys.argv[1]
    if len(sys.argv) > 2:
        index_dir = sys.argv[2]
    
    print(f"\n{'=' * 80}")
    print("PyLucene Indexer for Joined Company Data")
    print(f"{'=' * 80}")
    print(f"Data file: {data_file}")
    print(f"Index directory: {index_dir}")
    print()
    
    # Create indexer and build index
    indexer = JoinedDataLuceneIndexer(data_file=data_file, index_dir=index_dir)
    indexer.build_index()
    
    print("\nIndexing complete!")


if __name__ == "__main__":
    main()

