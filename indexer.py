import csv
import math
import pickle
import re
from collections import defaultdict, Counter
from datetime import datetime
from typing import Dict, List, Set, Tuple, Any
from pathlib import Path

# TODO: vahy podla casu

class StockIndexer:
    """TF-IDF indexer for stock market time-series data."""
    
    def __init__(self, data_file: str = "data/extracted_data.tsv", half_life_days: float = 7.0):
        self.data_file = data_file
        self.documents: List[Dict[str, Any]] = []  # All stock records
        self.index: Dict[str, Dict[int, float]] = defaultdict(dict)  # term -> {doc_id: tf-idf}
        self.doc_frequencies: Dict[str, int] = Counter()  # term -> number of docs containing it
        self.doc_norms: Dict[int, float] = {}  # doc_id -> L2 norm for cosine similarity
        self.latest_snapshots: Dict[str, int] = {}  # symbol -> latest doc_id
        # Recency weighting
        self.half_life_days: float = half_life_days
        self.recency_weights: Dict[int, float] = {}  # doc_id -> weight in [0,1]
    
    def bucket_price(self, price: str) -> str:
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
    
    def bucket_market_cap(self, market_cap: str) -> str:
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
    
    def bucket_price_change(self, change_pct: str) -> str:
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
    
    def bucket_employees(self, employees: str) -> str:
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
    
    def bucket_revenue(self, revenue: str) -> str:
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
    
    def extract_year_from_founded(self, founded: str) -> str:
        """
        Extract year from founded field and bucket by decade.
        
        Examples:
        1900-1909: founded_1900s
        1910-1919: founded_1910s
        1920-1929: founded_1920s
        1930-1939: founded_1930s
        1940-1949: founded_1940s
        1950-1959: founded_1950s
        1960-1969: founded_1960s
        1970-1979: founded_1970s
        1980-1989: founded_1980s
        1990-1999: founded_1990s
        2000-2009: founded_2000s
        2010-2019: founded_2010s
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

    
    def tokenize(self, text: str) -> List[str]:
        """Tokenize text into lowercase words."""
        if not text:
            return []
        # Split on non-alphanumeric, lowercase, remove empty
        tokens = re.findall(r'\b\w+\b', text.lower())
        return [t for t in tokens if len(t) > 1]  # Remove single characters
    
    def extract_terms(self, row: Dict[str, str]) -> List[str]:
        """
        Extract all searchable terms from a stock record.
        
        Used fields:
        - company
        - symbol
        - exchange
        - current_price
        - market_cap
        - calculated_percentage_change
        - employees
        - revenue
        - founded
        """
        terms = []
        
        # Text fields - company name (tokenized)
        company_tokens = self.tokenize(row.get('company', ''))
        terms.extend(company_tokens)
        
        # Symbol (exact, lowercased)
        symbol = row.get('symbol', '').strip().lower()
        if symbol:
            terms.append(f"symbol_{symbol}")
        
        # Exchange (exact, lowercased)
        exchange = row.get('exchange', '').strip().lower()
        if exchange:
            terms.append(f"exchange_{exchange}")
        
        # Bucketed numerical fields
        price_bucket = self.bucket_price(row.get('current_price', ''))
        if price_bucket:
            terms.append(price_bucket)
        
        cap_bucket = self.bucket_market_cap(row.get('market_cap', ''))
        if cap_bucket:
            terms.append(cap_bucket)
        
        change_bucket = self.bucket_price_change(row.get('calculated_percentage_change', ''))
        if change_bucket:
            terms.append(change_bucket)
        
        emp_bucket = self.bucket_employees(row.get('employees', ''))
        if emp_bucket:
            terms.append(emp_bucket)
        
        rev_bucket = self.bucket_revenue(row.get('revenue', ''))
        if rev_bucket:
            terms.append(rev_bucket)
        
        founded_bucket = self.extract_year_from_founded(row.get('founded', ''))
        if founded_bucket:
            terms.append(founded_bucket)
        
        return terms
    
    def load_data(self):
        """
        Load stock data from TSV file.
        
        This function also tracks the latest snapshot per symbol.
        """
        print(f"Loading data from {self.data_file}...")
        
        with open(self.data_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            
            for row in reader:
                doc_id = len(self.documents)
                self.documents.append(row)
                
                # Track latest snapshot per symbol
                symbol = row.get('symbol', '').strip()
                timestamp_str = row.get('timestamp', '')
                
                if symbol:
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                        
                        # Update if this is the latest snapshot
                        if symbol not in self.latest_snapshots:
                            self.latest_snapshots[symbol] = doc_id
                        else:
                            prev_id = self.latest_snapshots[symbol]
                            prev_timestamp = datetime.strptime(
                                self.documents[prev_id].get('timestamp', ''),
                                '%Y-%m-%d %H:%M:%S'
                            )
                            if timestamp > prev_timestamp:
                                self.latest_snapshots[symbol] = doc_id
                    except (ValueError, AttributeError):
                        pass
        
        print(f"Loaded {len(self.documents)} records")
        print(f"Found {len(self.latest_snapshots)} unique stocks")
        # Compute recency weights now that all documents are loaded
        self._compute_recency_weights()
    
    def _compute_recency_weights(self):
        """Compute exponential decay weights for each document based on timestamp.
        
        Weight function uses half-life: weight = 0.5 ** (age_days / half_life_days)
        Documents without a valid timestamp receive weight 1.0.
        """
        now = datetime.now()
        ln2 = math.log(2)
        for doc_id, doc in enumerate(self.documents):
            ts_str = doc.get('timestamp', '')
            try:
                ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
                age_days = max((now - ts).total_seconds() / 86400.0, 0.0)
                if self.half_life_days > 0:
                    weight = math.exp(-ln2 * (age_days / self.half_life_days))
                else:
                    weight = 1.0
            except Exception:
                weight = 1.0
            self.recency_weights[doc_id] = float(max(min(weight, 1.0), 0.0))
    
    def build_index(self):
        """
        Build TF-IDF index from loaded documents.
        
        Always indexes all records; recency is handled via exponential decay in scoring.
        """
        print(f"\nBuilding TF-IDF index (indexing all records)...")
        
        # Determine which documents to index (all)
        doc_ids = set(range(len(self.documents)))
        print(f"Indexing all {len(doc_ids)} records")
        
        # Step 1: Compute term frequencies (TF) and document frequencies (DF)
        term_freqs: Dict[int, Counter] = defaultdict(Counter)
        # Track how many documents contain each term
        for doc_id in doc_ids:
            terms = self.extract_terms(self.documents[doc_id])
            term_freqs[doc_id] = Counter(terms)
            
            # Update document frequency
            for term in set(terms):
                self.doc_frequencies[term] += 1
        
        print(f"Extracted {len(self.doc_frequencies)} unique terms")
        
        # Step 2: Compute TF-IDF scores
        num_docs = len(doc_ids)
        
        for doc_id in doc_ids:
            tf_counter = term_freqs[doc_id]
            doc_length = sum(tf_counter.values())
            
            for term, count in tf_counter.items():
                # TF: normalized term frequency
                tf = count / doc_length if doc_length > 0 else 0
                
                # IDF: inverse document frequency
                df = self.doc_frequencies[term]
                idf = math.log(num_docs / df) if df > 0 else 0
                
                # TF-IDF score
                tfidf = tf * idf
                self.index[term][doc_id] = tfidf
        
        # Step 3: Compute document norms for cosine similarity (L2 norm)
        for doc_id in doc_ids:
            norm = 0.0
            for term in self.index:
                if doc_id in self.index[term]:
                    norm += self.index[term][doc_id] ** 2
            self.doc_norms[doc_id] = math.sqrt(norm)
        
        print(f"Index built successfully!")
    
    
    def search(self, query: str, top_k: int = 10, require_all_terms: bool = True, 
               ranking_method: str = 'tfidf') -> List[Tuple[int, float, Dict]]:
        """
        Search the index using a query string.
        
        Available ranking methods:
        - tfidf: TF-IDF with cosine similarity
        - bm25: BM25 ranking algorithm
        
        Returns a list of (doc_id, score, document) tuples, sorted by relevance.
        """
        
        if ranking_method == 'bm25':
            return self.search_bm25(query, top_k, require_all_terms)
        else:
            return self.search_tfidf(query, top_k, require_all_terms)
    
    def search_tfidf(self, query: str, top_k: int = 10, require_all_terms: bool = True) -> List[Tuple[int, float, Dict]]:
        """Search using TF-IDF with cosine similarity (default method)."""
        # Tokenize query
        query_terms = []
        
        # Check for special query terms (symbol_, exchange_, ...)
        tokens = query.lower().split()
        for token in tokens:
            if token.startswith('symbol_') or token.startswith('exchange_'):
                query_terms.append(token)
            elif '_' in token and any(token.startswith(prefix) for prefix in 
                ['price_', 'cap_', 'move_', 'size_', 'rev_', 'founded_']):
                query_terms.append(token)
            else:
                # Regular word tokens
                query_terms.extend(self.tokenize(token))
        
        if not query_terms:
            return []
        
        # Compute query vector (TF-IDF for query)
        query_tf = Counter(query_terms)
        query_length = len(query_terms)
        query_vector = {}
        
        for term in set(query_terms):
            if term in self.doc_frequencies:
                tf = query_tf[term] / query_length
                df = self.doc_frequencies[term]
                idf = math.log(len(self.doc_norms) / df) if df > 0 else 0
                query_vector[term] = tf * idf
        
        # Compute query norm
        query_norm = math.sqrt(sum(v ** 2 for v in query_vector.values()))
        
        if query_norm == 0:
            return []
        
        # Score documents using cosine similarity
        scores = {}
        doc_term_counts = defaultdict(int)  # Track how many query terms each doc matches
        
        for term, query_weight in query_vector.items():
            if term in self.index:
                for doc_id, doc_weight in self.index[term].items():
                    if doc_id not in scores:
                        scores[doc_id] = 0.0
                    scores[doc_id] += query_weight * doc_weight
                    doc_term_counts[doc_id] += 1
        
        # Filter documents if require_all_terms is True
        if require_all_terms:
            num_query_terms = len(query_vector)
            scores = {doc_id: score for doc_id, score in scores.items() 
                     if doc_term_counts[doc_id] == num_query_terms}
        
        # Normalize by document norms (cosine similarity)
        for doc_id in scores:
            if self.doc_norms[doc_id] > 0:
                scores[doc_id] /= (query_norm * self.doc_norms[doc_id])
            # Apply recency weight
            weight = self.recency_weights.get(doc_id, 1.0)
            scores[doc_id] *= weight
        
        # Sort by score and return top_k
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        
        return [(doc_id, score, self.documents[doc_id]) for doc_id, score in ranked]
    
    def search_bm25(self, query: str, top_k: int = 10, require_all_terms: bool = True) -> List[Tuple[int, float, Dict]]:
        """Search using BM25 ranking algorithm.
        
        BM25 is an improved probabilistic ranking function that addresses term saturation.
        It's better than TF-IDF for handling repeated query terms and document length normalization.
        """
        # Tokenize query
        query_terms = []
        
        # Check for special query terms (symbol_, exchange_, etc.)
        tokens = query.lower().split()
        for token in tokens:
            if token.startswith('symbol_') or token.startswith('exchange_'):
                query_terms.append(token)
            elif '_' in token and any(token.startswith(prefix) for prefix in 
                ['price_', 'cap_', 'move_', 'size_', 'rev_', 'founded_']):
                query_terms.append(token)
            else:
                # Regular word tokens
                query_terms.extend(self.tokenize(token))
        
        if not query_terms:
            return []
        
        # BM25 parameters
        k1 = 1.5  # Term frequency saturation parameter (usually 1.2-2.0)
        b = 0.75  # Length normalization parameter (usually 0.0-1.0)
        
        # Calculate average document length
        if not hasattr(self, '_avg_doc_length'):
            self._avg_doc_length = sum(len(self.extract_terms(doc)) for doc in self.documents) / len(self.documents)
        
        # BM25 scoring
        scores = {}
        doc_term_counts = defaultdict(int)
        
        for term in set(query_terms):
            if term not in self.doc_frequencies:
                continue
            
            # IDF for BM25 (logarithmic smoothing)
            df = self.doc_frequencies[term]
            idf = math.log((len(self.doc_norms) - df + 0.5) / (df + 0.5) + 1)
            
            if term in self.index:
                term_freq_in_query = query_terms.count(term)
                
                for doc_id, tf_weight in self.index[term].items():
                    if doc_id not in scores:
                        scores[doc_id] = 0.0
                    
                    # Get document length (number of terms)
                    doc_length = len(self.extract_terms(self.documents[doc_id]))
                    
                    # Calculate BM25 score component for this term
                    numerator = (k1 + 1) * tf_weight
                    denominator = tf_weight + k1 * (1 - b + b * (doc_length / self._avg_doc_length))
                    score_component = term_freq_in_query * idf * (numerator / denominator)
                    
                    scores[doc_id] += score_component
                    doc_term_counts[doc_id] += 1
        
        # Filter documents if require_all_terms is True
        if require_all_terms:
            num_query_terms = len(set(query_terms))
            scores = {doc_id: score for doc_id, score in scores.items() 
                     if doc_term_counts[doc_id] == num_query_terms}
        
        # Apply recency weights and sort
        for doc_id in list(scores.keys()):
            weight = self.recency_weights.get(doc_id, 1.0)
            scores[doc_id] *= weight
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        
        return [(doc_id, score, self.documents[doc_id]) for doc_id, score in ranked]
    
    def display_results(self, results: List[Tuple[int, float, Dict]]):
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
            timestamp = doc.get('timestamp', 'N/A')
            founded = doc.get('founded', 'N/A')
            revenue = doc.get('revenue', 'N/A')
            
            print(f"{rank}. {company} ({symbol})")
            print(f"   Exchange: {exchange} | Price: {price} | Change: {change}")
            print(f"   Market Cap: {market_cap} | Founded: {founded}")
            print(f"   Revenue: {revenue} | Timestamp: {timestamp}")
            print(f"   Relevance Score: {score:.4f}")
            print("-" * 100)
    
    def save_index(self, filepath: str = "indexes/stock_index.pkl"):
        """Save the index to disk."""
        print(f"\nSaving index to {filepath}...")
        
        index_data = {
            'documents': self.documents,
            'index': dict(self.index),
            'doc_frequencies': dict(self.doc_frequencies),
            'doc_norms': self.doc_norms,
            'latest_snapshots': self.latest_snapshots,
            'data_file': self.data_file,
            'half_life_days': self.half_life_days,
            'recency_weights': self.recency_weights
        }
        
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'wb') as f:
            pickle.dump(index_data, f)
        
        print(f"Index saved successfully!")
    
    def load_index(self, filepath: str = "data/stock_index.pkl"):
        """Load a previously saved index from disk."""
        print(f"Loading index from {filepath}...")
        
        with open(filepath, 'rb') as f:
            index_data = pickle.load(f)
        
        self.documents = index_data['documents']
        self.index = defaultdict(dict, index_data['index'])
        self.doc_frequencies = Counter(index_data['doc_frequencies'])
        self.doc_norms = index_data['doc_norms']
        self.latest_snapshots = index_data['latest_snapshots']
        self.data_file = index_data['data_file']
        self.half_life_days = index_data.get('half_life_days', self.half_life_days)
        self.recency_weights = index_data.get('recency_weights', {})
        if not self.recency_weights:
            # Backfill if loading an old index
            self._compute_recency_weights()
        
        print(f"Index loaded successfully!")
        print(f"  - {len(self.documents)} documents")
        print(f"  - {len(self.doc_frequencies)} unique terms")
        print(f"  - {len(self.latest_snapshots)} unique stocks")
        print(f"  - half_life_days = {self.half_life_days}")
    
    # ==================== STATISTICS ====================
    
    def print_statistics(self):
        """Print index statistics."""
        print("\n" + "=" * 100)
        print("INDEX STATISTICS")
        print("=" * 100)
        print(f"Total documents: {len(self.documents)}")
        print(f"Indexed documents: {len(self.doc_norms)}")
        print(f"Unique stocks: {len(self.latest_snapshots)}")
        print(f"Unique terms: {len(self.doc_frequencies)}")
        print(f"\nTop 20 most common terms:")
        
        for term, count in self.doc_frequencies.most_common(20):
            print(f"  {term}: {count} documents")
        
        print("\n" + "=" * 100)
