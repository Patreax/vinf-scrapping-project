"""
Search interface for the Stock Market Indexes.
This script provides a simple way to search the indexed stock data.

Usage:
    python search.py                    # Interactive mode - choose index type
    python search.py --tfidf            # Use TF-IDF index
    python search.py --lucene           # Use Lucene index
    python search.py --tfidf --data <file>  # Use TF-IDF with custom data file
    python search.py --lucene --index <dir> # Use Lucene with custom index directory
"""

import sys
import os

try:
    import lucene
    from index_joined_data_lucene import JoinedDataLuceneSearcher
    LUCENE_AVAILABLE = True
except ImportError:
    LUCENE_AVAILABLE = False
    print("Warning: PyLucene not available. Lucene search will be disabled.")


def parse_arguments():
    """Parse command-line arguments."""
    use_tfidf = False
    use_lucene = False
    data_file = None
    index_dir = None
    
    i = 0
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--tfidf':
            use_tfidf = True
        elif arg == '--lucene':
            use_lucene = True
        elif arg == '--data' and i + 1 < len(sys.argv):
            data_file = sys.argv[i + 1]
            i += 1
        elif arg == '--index' and i + 1 < len(sys.argv):
            index_dir = sys.argv[i + 1]
            i += 1
        i += 1
    
    return use_tfidf, use_lucene, data_file, index_dir


def choose_index_type():
    """Interactive prompt to choose index type."""
    print("\n" + "=" * 100)
    print("SELECT INDEX TYPE")
    print("=" * 100)
    print("1. TF-IDF Index (from indexer.py)")
    if LUCENE_AVAILABLE:
        print("2. Lucene Index (from index_joined_data_lucene.py)")
    else:
        print("2. Lucene Index (not available - PyLucene not installed)")
    print("=" * 100)
    
    while True:
        choice = input("\nEnter choice (1 or 2): ").strip()
        if choice == '1':
            return 'tfidf'
        elif choice == '2' and LUCENE_AVAILABLE:
            return 'lucene'
        else:
            print("Invalid choice. Please enter 1 or 2.")


def setup_tfidf_indexer(data_file=None):
    """Setup and load TF-IDF indexer."""
    try:
        from indexer import StockIndexer
    except ImportError:
        print("Error: indexer module not found. Cannot use TF-IDF index.")
        return None
    
    if data_file is None:
        data_file = "data/extracted_data.tsv"
    
    print(f"Using TF-IDF index")
    print(f"Data file: {data_file}")
    
    print(f"\nInitializing TF-IDF indexer (recency-weighted, indexing all records)...")
    indexer = StockIndexer(data_file=data_file)
    
    # Try to load existing index first
    index_filename = f"indexes/{data_file.split('/')[-1].replace('.tsv', '_index.pkl')}"
    try:
        indexer.load_index(index_filename)
        print("\nLoaded existing TF-IDF index")
    except FileNotFoundError:
        print("\nNo existing index found, building new one...")
        indexer.load_data()
        indexer.build_index()
        indexer.save_index(index_filename)
        print(f"Index saved to {index_filename}")
    
    indexer.print_statistics()
    
    return indexer


def setup_lucene_searcher(index_dir=None):
    """Setup and load Lucene searcher."""
    if not LUCENE_AVAILABLE:
        print("Error: PyLucene is not available. Cannot use Lucene index.")
        return None
    
    # Initialize JVM for PyLucene
    try:
        lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        print("JVM initialized for PyLucene")
    except Exception as e:
        print(f"Warning: JVM may already be initialized: {e}")
    
    if index_dir is None:
        index_dir = "lucene/joined_company_data_index"
    
    print(f"Using Lucene index")
    print(f"Index directory: {index_dir}")
    
    # Create searcher with default field weights (all 1.0)
    # To customize weights, modify this section or use searcher.set_field_weights() after creation
    # Example:
    #   custom_weights = {'company': 2.0, 'title': 1.5, 'description': 1.2}
    #   searcher = JoinedDataLuceneSearcher(index_dir=index_dir, field_weights=custom_weights)
    # 
    # To make recency weighting more aggressive (penalize older records more):
    #   searcher = JoinedDataLuceneSearcher(index_dir=index_dir, recency_punishment_factor=1.5)
    #   - factor = 1.0: Standard decay (default)
    #   - factor > 1.0: More aggressive (e.g., 1.5, 2.0)
    #   - factor < 1.0: Less aggressive (e.g., 0.5)
    searcher = JoinedDataLuceneSearcher(index_dir=index_dir)
    
    if not searcher.open_index():
        print(f"\nError: Could not open Lucene index at {index_dir}")
        print("Please make sure the index exists. Run index_joined_data_lucene.py to create it.")
        return None
    
    print("\nOpened Lucene index")
    searcher.print_statistics()
    
    return searcher


def main():
    # Parse command-line arguments
    use_tfidf, use_lucene, data_file, index_dir = parse_arguments()
    
    # Determine which index to use
    index_type = None
    if use_tfidf:
        index_type = 'tfidf'
    elif use_lucene:
        index_type = 'lucene'
    else:
        # Interactive mode - let user choose
        index_type = choose_index_type()
    
    # Setup the appropriate indexer/searcher
    indexer = None
    searcher = None
    
    if index_type == 'tfidf':
        indexer = setup_tfidf_indexer(data_file)
        if indexer is None:
            return
    elif index_type == 'lucene':
        searcher = setup_lucene_searcher(index_dir)
        if searcher is None:
            return
    
    # Interactive search mode
    print("\n" + "=" * 100)
    print("INTERACTIVE SEARCH MODE")
    print("=" * 100)
    print("\nEnter search queries (or 'quit' to exit, 'help' for examples)")
    if index_type == 'lucene':
        print("Type 'weights' to see current field boost weights")
        print("Type 'range' for interactive range query builder")
    
    try:
        while True:
            try:
                query = input("\nSearch> ").strip()
                
                if query.lower() in ['quit', 'exit', 'q']:
                    break
                
                if query.lower() == 'help':
                    print_help(index_type)
                    continue
                
                if query.lower() == 'weights' and index_type == 'lucene':
                    searcher.print_field_weights()
                    continue
                
                if query.lower() == 'range' and index_type == 'lucene':
                    interactive_range_query(searcher)
                    continue
                
                if not query:
                    continue
                
                # Parse search mode (AND/OR)
                # Default: OR for Lucene, AND for TF-IDF
                if index_type == 'lucene':
                    require_all_terms = False  # Default to OR for Lucene
                else:
                    require_all_terms = True  # Default to AND for TF-IDF
                
                if query.upper().startswith('OR:'):
                    require_all_terms = False
                    query = query[3:].strip()
                elif query.upper().startswith('AND:'):
                    require_all_terms = True
                    query = query[4:].strip()
                
                # Parse top_k if specified (e.g., "Nike:5" for top 5 results)
                top_k = 5
                if ':' in query and not query.upper().startswith(('OR:', 'AND:')):
                    parts = query.rsplit(':', 1)
                    if len(parts) == 2:
                        try:
                            potential_k = int(parts[1].strip())
                            if 1 <= potential_k <= 1000:
                                query = parts[0].strip()
                                top_k = potential_k
                        except ValueError:
                            pass
                
                # Parse range filters from query (for Lucene only)
                range_filters = None
                text_query = query
                if index_type == 'lucene':
                    text_query, range_filters = parse_range_filters_from_query(query)
                
                # Perform search
                if index_type == 'tfidf':
                    # TF-IDF specific parsing
                    ranking_method = 'tfidf'
                    if text_query.upper().startswith('BM25:'):
                        ranking_method = 'bm25'
                        text_query = text_query[5:].strip()
                    elif text_query.upper().startswith('TFIDF:'):
                        ranking_method = 'tfidf'
                        text_query = text_query[6:].strip()
                    
                    results = indexer.search(text_query.strip(), top_k=top_k, require_all_terms=require_all_terms, 
                                           ranking_method=ranking_method)
                    mode = "AND" if require_all_terms else "OR"
                    print(f"\n[Search mode: {mode} | Ranking: {ranking_method.upper()}]")
                    indexer.display_results(results)
                else:  # lucene
                    if range_filters and not text_query.strip():
                        # Pure range query
                        results = searcher.range_query(range_filters, top_k=top_k)
                        print(f"\n[Range Query | Index: Lucene]")
                        searcher.display_results(results)
                    elif range_filters:
                        # Combined text + range query
                        results = searcher.search_with_range_filters(
                            text_query.strip(), 
                            range_filters=range_filters,
                            top_k=top_k, 
                            require_all_terms=require_all_terms
                        )
                        mode = "AND" if require_all_terms else "OR"
                        print(f"\n[Search mode: {mode} | Range filters: {len(range_filters)} | Index: Lucene]")
                        searcher.display_results(results)
                    else:
                        # Pure text search
                        results = searcher.search(text_query.strip(), top_k=top_k, require_all_terms=require_all_terms)
                        mode = "AND" if require_all_terms else "OR"
                        print(f"\n[Search mode: {mode} | Index: Lucene]")
                        searcher.display_results(results)
                
            except (KeyboardInterrupt, EOFError):
                break
    finally:
        # Cleanup
        if searcher:
            searcher.close_index()
    
    print("\nGoodbye!")


def parse_range_filters_from_query(query: str):
    """
    Parse range filters from query string.
    
    Supports syntax:
    - range:field:min:max (e.g., range:employees:1000:50000)
    - range:field:min (e.g., range:employees:1000 for min only)
    - field>min (e.g., employees>1000)
    - field<max (e.g., employees<50000)
    - field:min-max (e.g., employees:1000-50000)
    
    Returns: (text_query, range_filters_dict)
    """
    import re
    
    range_filters = {}
    text_query = query
    
    # Pattern 1: range:field:min:max or range:field:min
    pattern1 = r'\brange:(\w+):([\d.]+)(?::([\d.]+))?\b'
    matches = re.finditer(pattern1, query)
    for match in matches:
        field = match.group(1)
        min_val = float(match.group(2))
        max_val = float(match.group(3)) if match.group(3) else None
        
        if field not in range_filters:
            range_filters[field] = {}
        if min_val is not None:
            range_filters[field]['min'] = min_val
        if max_val is not None:
            range_filters[field]['max'] = max_val
        
        # Remove from text query
        text_query = text_query.replace(match.group(0), '').strip()
    
    # Pattern 2: field>min or field<max
    pattern2 = r'(\w+)([><])([\d.]+)'
    matches = re.finditer(pattern2, query)
    for match in matches:
        field = match.group(1)
        operator = match.group(2)
        value = float(match.group(3))
        
        if field not in range_filters:
            range_filters[field] = {}
        
        if operator == '>':
            range_filters[field]['min'] = value
        elif operator == '<':
            range_filters[field]['max'] = value
        
        # Remove from text query
        text_query = text_query.replace(match.group(0), '').strip()
    
    # Pattern 3: field:min-max
    pattern3 = r'(\w+):([\d.]+)-([\d.]+)'
    matches = re.finditer(pattern3, query)
    for match in matches:
        field = match.group(1)
        min_val = float(match.group(2))
        max_val = float(match.group(3))
        
        if field not in range_filters:
            range_filters[field] = {}
        range_filters[field]['min'] = min_val
        range_filters[field]['max'] = max_val
        
        # Remove from text query
        text_query = text_query.replace(match.group(0), '').strip()
    
    # Clean up extra spaces
    text_query = ' '.join(text_query.split())
    
    return text_query, range_filters if range_filters else None


def interactive_range_query(searcher):
    """Interactive range query builder."""
    print("\n" + "=" * 100)
    print("RANGE QUERY BUILDER")
    print("=" * 100)
    print("\nSupported fields:")
    print("  - timestamp (Unix epoch seconds)")
    print("  - current_price (float)")
    print("  - calculated_percentage_change (float)")
    print("  - market_cap (float, in USD)")
    print("  - founded (int, year)")
    print("  - employees (int)")
    print("  - revenue (float, in USD)")
    print("  - ebitda (float, in USD)")
    print("\nEnter range filters (press Enter with empty field name to execute query)")
    
    range_filters = {}
    
    while True:
        field = input("\nField name (or 'done' to execute, 'clear' to reset): ").strip().lower()
        
        if field == 'done':
            break
        elif field == 'clear':
            range_filters = {}
            print("Range filters cleared.")
            continue
        elif not field:
            break
        
        if field not in ['timestamp', 'current_price', 'calculated_percentage_change', 
                        'market_cap', 'founded', 'employees', 'revenue', 'ebitda']:
            print(f"Unknown field: {field}")
            continue
        
        min_str = input(f"  Minimum value for {field} (or press Enter for no minimum): ").strip()
        max_str = input(f"  Maximum value for {field} (or press Enter for no maximum): ").strip()
        
        range_spec = {}
        if min_str:
            try:
                if field in ['timestamp', 'founded', 'employees']:
                    range_spec['min'] = int(float(min_str))
                else:
                    range_spec['min'] = float(min_str)
            except ValueError:
                print(f"Invalid minimum value: {min_str}")
                continue
        
        if max_str:
            try:
                if field in ['timestamp', 'founded', 'employees']:
                    range_spec['max'] = int(float(max_str))
                else:
                    range_spec['max'] = float(max_str)
            except ValueError:
                print(f"Invalid maximum value: {max_str}")
                continue
        
        if range_spec:
            range_filters[field] = range_spec
            print(f"  Added: {field} = {range_spec}")
        else:
            print(f"  Skipped: no valid range specified")
    
    if not range_filters:
        print("\nNo range filters specified.")
        return
    
    # Get top_k
    top_k_str = input("\nNumber of results (default 5): ").strip()
    top_k = 5
    if top_k_str:
        try:
            top_k = int(top_k_str)
            if top_k < 1 or top_k > 1000:
                top_k = 5
        except ValueError:
            pass
    
    # Execute query
    print(f"\nExecuting range query with {len(range_filters)} filter(s)...")
    results = searcher.range_query(range_filters, top_k=top_k)
    searcher.display_results(results)


def print_help(index_type='tfidf'):
    """Print help information about search syntax."""
    print("\n" + "=" * 100)
    print("SEARCH HELP")
    if index_type == 'lucene':
        print("(Using Lucene Index)")
    else:
        print("(Using TF-IDF Index)")
    print("=" * 100)
    print("\nQuery Syntax:")
    print("  - Simple text search: 'Nike', 'IBM', 'nvidia'")
    print("  - Specify number of results: 'nvidia:20' (returns top 20)")
    print("\nSearch Modes:")
    print("  - AND: (default) Only return documents matching ALL terms")
    print("    Example: 'exchange_nasdaq move_flat' or 'AND: exchange_nasdaq move_flat'")
    print("  - OR: Return documents matching ANY term")
    print("    Example: 'OR: exchange_nasdaq move_flat'")
    print("\nSpecial Query Terms:")
    print("  Symbols:     symbol_aapl, symbol_ibm, symbol_nke")
    print("  Exchanges:   exchange_nyse, exchange_nasdaq, exchange_nse")
    print("\nPrice Buckets:")
    print("  price_micro      ($0-10)")
    print("  price_low        ($10-50)")
    print("  price_medium     ($50-150)")
    print("  price_high       ($150-500)")
    print("  price_very_high  ($500+)")
    print("\nMarket Cap Buckets:")
    print("  cap_nano   (<$50M)")
    print("  cap_micro  ($50M-300M)")
    print("  cap_small  ($300M-2B)")
    print("  cap_mid    ($2B-10B)")
    print("  cap_large  ($10B-200B)")
    print("  cap_mega   ($200B+)")
    print("\nPrice Movement Buckets:")
    print("  move_crash       (<-5%)")
    print("  move_down_strong (-5% to -2%)")
    print("  move_down_weak   (-2% to -0.5%)")
    print("  move_flat        (-0.5% to 0.5%)")
    print("  move_up_weak     (0.5% to 2%)")
    print("  move_up_strong   (2% to 5%)")
    print("  move_surge       (>5%)")
    print("\nCompany Size (Employees):")
    print("  size_tiny    (<1K)")
    print("  size_small   (1K-10K)")
    print("  size_medium  (10K-50K)")
    print("  size_large   (50K-100K)")
    print("  size_huge    (100K+)")
    print("\nRevenue Buckets:")
    print("  rev_startup  (<$100M)")
    print("  rev_small    ($100M-1B)")
    print("  rev_medium   ($1B-10B)")
    print("  rev_large    ($10B-50B)")
    print("  rev_huge     ($50B+)")
    print("\nFounded Buckets by decades (Examples):")
    print("  founded_1900s  (1900-1909)")
    print("  founded_1910s  (1910-1919)")
    print("  founded_1920s  (1920-1929)")
    print("  founded_1930s  (1930-1939)")
    print("  founded_1940s  (1940-1949)")
    print("  founded_1950s  (1950-1959)")
    print("  founded_1960s  (1960-1969)")
    print("  ... (and so on)")
    print("\nExample Queries:")
    print("  'etf move_up_weak price_high'")
    print("  'move_up_strong founded_2000s'")
    print("  'size_tiny move_up_strong rev_startup'")
    
    if index_type == 'lucene':
        print("\n" + "=" * 100)
        print("RANGE QUERIES (Lucene only)")
        print("=" * 100)
        print("\nRange Query Syntax:")
        print("  1. range:field:min:max")
        print("     Example: 'range:employees:1000:50000'")
        print("  2. range:field:min (minimum only)")
        print("     Example: 'range:employees:1000'")
        print("  3. field>min (greater than)")
        print("     Example: 'employees>1000'")
        print("  4. field<max (less than)")
        print("     Example: 'employees<50000'")
        print("  5. field:min-max (range)")
        print("     Example: 'employees:1000-50000'")
        print("\nCombining Text Search with Range Queries:")
        print("  'tech range:employees:1000:50000'")
        print("  'nvidia employees>5000 market_cap>1000000000'")
        print("  'OR: technology employees:1000-50000 revenue>100000000'")
        print("\nPure Range Queries:")
        print("  'range:employees:1000:50000'")
        print("  'employees>1000 market_cap>1000000000'")
        print("\nInteractive Range Query:")
        print("  Type 'range' to use the interactive range query builder")
        print("\nSupported Range Query Fields:")
        print("  - timestamp (Unix epoch seconds)")
        print("  - current_price (float)")
        print("  - calculated_percentage_change (float)")
        print("  - market_cap (float, in USD)")
        print("  - founded (int, year)")
        print("  - employees (int)")
        print("  - revenue (float, in USD)")
        print("  - ebitda (float, in USD)")
        print("\nRange Query Examples:")
        print("  'range:employees:1000:50000'  # Employees between 1000 and 50000")
        print("  'employees>1000'              # Employees greater than 1000")
        print("  'market_cap:1000000000-10000000000'  # Market cap between 1B and 10B")
        print("  'tech range:employees:1000 founded>2000'  # Tech companies with employees>1000, founded after 2000")
    
    print("=" * 100)


if __name__ == "__main__":
    main()