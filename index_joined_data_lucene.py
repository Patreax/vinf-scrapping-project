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
    from org.apache.lucene.analysis.standard import StandardAnalyzer
    from org.apache.lucene.document import Document, Field, FieldType, StringField, TextField
    from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexOptions
    from org.apache.lucene.store import FSDirectory
except ImportError as e:
    print("Error: PyLucene is not properly installed or initialized.")
    print(f"Import error: {e}")
    sys.exit(1)


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
    
    def create_document(self, row: Dict[str, str]) -> Document:
        """
        Create a Lucene document from a row of joined data.
        """
        doc = Document()
        
        # TODO: some fields might be StringField instead of TextField
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
        
        # TODO: this might not be required
        # Store original values
        if row.get('current_price'):
            doc.add(StringField("current_price", row.get('current_price'), Field.Store.YES))
        if row.get('market_cap'):
            doc.add(StringField("market_cap", row.get('market_cap'), Field.Store.YES))
        if row.get('calculated_percentage_change'):
            doc.add(StringField("calculated_percentage_change", row.get('calculated_percentage_change'), Field.Store.YES))
        if row.get('employees'):
            doc.add(StringField("employees", row.get('employees'), Field.Store.YES))
        if row.get('revenue'):
            doc.add(StringField("revenue", row.get('revenue'), Field.Store.YES))
        if row.get('founded'):
            doc.add(StringField("founded", row.get('founded'), Field.Store.YES))
        if row.get('timestamp'):
            doc.add(StringField("timestamp", row.get('timestamp'), Field.Store.YES))
        # if row.get('source_file'):
        #     doc.add(StringField("source_file", row.get('source_file'), Field.Store.YES))
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
            doc.add(TextField("keyword", row.get('keyword'), Field.Store.YES))
        if row.get('first_paragraph'):
            doc.add(TextField("first_paragraph", row.get('first_paragraph'), Field.Store.YES))
        if row.get('description'):
            doc.add(TextField("description", row.get('description'), Field.Store.YES))
        
        # Bucketed fields (all searchable)
        price_bucket = self.bucket_price(row.get('current_price', ''))
        if price_bucket:
            doc.add(TextField("price_bucket", price_bucket, Field.Store.YES))
        
        cap_bucket = self.bucket_market_cap(row.get('market_cap', ''))
        if cap_bucket:
            doc.add(TextField("cap_bucket", cap_bucket, Field.Store.YES))
        
        change_bucket = self.bucket_price_change(row.get('calculated_percentage_change', ''))
        if change_bucket:
            doc.add(TextField("change_bucket", change_bucket, Field.Store.YES))
        
        emp_bucket = self.bucket_employees(row.get('employees', ''))
        if emp_bucket:
            doc.add(TextField("size_bucket", emp_bucket, Field.Store.YES))
        
        rev_bucket = self.bucket_revenue(row.get('revenue', ''))
        if rev_bucket:
            doc.add(TextField("rev_bucket", rev_bucket, Field.Store.YES))
        
        founded_bucket = self.extract_year_from_founded(row.get('founded', ''))
        if founded_bucket:
            doc.add(TextField("founded_bucket", founded_bucket, Field.Store.YES))
        
        # Combined searchable content field (for general search)
        searchable_content = []
        if company:
            searchable_content.append(company)
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
                 half_life_days: float = 7.0):
        self.index_dir = index_dir
        self.analyzer = StandardAnalyzer()
        self.directory = None
        self.reader = None
        self.searcher = None
        
        # Recency weighting parameters
        self.half_life_days = half_life_days
        
        self.field_weights = {
            'company': 1.0,
            'industries': 1.0,
            'founders': 1.0,
            'headquarters': 1.0,
            'title': 1.0,
            'keyword': 1.0,
            'first_paragraph': 1.0,
            'description': 1.0,
            'price_bucket': 1.0,
            'cap_bucket': 1.0,
            'change_bucket': 1.0,
            'size_bucket': 1.0,
            'rev_bucket': 1.0,
            'founded_bucket': 1.0,
            'symbol_search': 1.0,
            'exchange_search': 1.0,
            'content': 1.0,  # Combined field
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
        
        Weight function uses half-life: weight = exp(-ln2 * (age_days / half_life_days))
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
                    weight = math.exp(-ln2 * (age_days / self.half_life_days))
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
                        weight = math.exp(-ln2 * (age_days / self.half_life_days))
                    else:
                        weight = 1.0
                    
                    return float(max(min(weight, 1.0), 0.0))
                except Exception:
                    pass
        
        # Default weight if no valid timestamp found
        return 1.0
    
    def search(self, query_str: str, top_k: int = 10, require_all_terms: bool = True) -> List[tuple]:
        """
        Search the Lucene index across multiple searchable fields.
        
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
            from org.apache.lucene.search import BooleanQuery, BooleanClause
            from org.apache.lucene.index import Term
            from org.apache.lucene.search import TermQuery
            
            # Search across multiple fields
            search_fields = [
                "company",
                "title", 
                "description",
                "first_paragraph",
                "industries",
                "founders",
                "headquarters",
                "keyword",
                "price_bucket",
                "cap_bucket",
                "change_bucket",
                "size_bucket",
                "rev_bucket",
                "founded_bucket",
                "symbol_search",
                "exchange_search"
            ]
            
            # Split query into terms
            query_terms = query_str.strip().split()
            
            if not query_terms:
                return []
            
            # Create a BooleanQuery for the overall query
            main_query = BooleanQuery.Builder()
            
            # For each term, create a query that searches across all fields (OR)
            for term in query_terms:
                term_query = BooleanQuery.Builder()
                
                # Search this term across all fields
                for field in search_fields:
                    try:
                        # Create a simple term query for this field
                        field_term = Term(field, term.lower())
                        term_query.add(TermQuery(field_term), BooleanClause.Occur.SHOULD)
                    except Exception:
                        continue
                
                term_query_built = term_query.build()
                # Add this term's query to main query with MUST (AND) or SHOULD (OR)
                if require_all_terms:
                    main_query.add(term_query_built, BooleanClause.Occur.MUST)
                else:
                    main_query.add(term_query_built, BooleanClause.Occur.SHOULD)
            
            query = main_query.build()
            
            # Execute search
            top_docs = self.searcher.search(query, top_k)
            
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
                # Try to get timestamp from timestamp field, or extract from source_file
                timestamp_str = doc_dict.get('timestamp', '')
                source_file = doc_dict.get('source_file', '')
                recency_weight = self._compute_recency_weight(timestamp_str, source_file)
                final_score = lucene_score * recency_weight
                
                results.append((doc_id, final_score, doc_dict))
            
            # Re-sort by final score (recency-weighted)
            results.sort(key=lambda x: x[1], reverse=True)
            
            return results
            
        except Exception as e:
            print(f"Search error: {e}")
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
            price = doc.get('current_price', 'N/A')
            change = doc.get('calculated_percentage_change', 'N/A')
            market_cap = doc.get('market_cap', 'N/A')
            # Try to get timestamp from timestamp field, or extract from source_file
            timestamp = doc.get('timestamp', 'N/A')
            source_file = doc.get('source_file', '')
            if timestamp == 'N/A' or timestamp == '2025-11-09 19:15:21':  # If it's the join timestamp, try to extract from filename
                extracted_ts = self._extract_timestamp_from_filename(source_file)
                if extracted_ts:
                    timestamp = extracted_ts
            founded = doc.get('founded', 'N/A')
            revenue = doc.get('revenue', 'N/A')
            employees = doc.get('employees', 'N/A')
            
            print(f"{rank}. {company} ({symbol})")
            print(f"   Exchange: {exchange} | Price: {price} | Change: {change}")
            print(f"   Market Cap: {market_cap} | Founded: {founded}")
            print(f"   Revenue: {revenue} | Employees: {employees}")
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
            print(f"Recency weighting: half_life_days = {self.half_life_days}")
            
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

