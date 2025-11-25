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
                top_k = 10
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
                
                # Perform search
                if index_type == 'tfidf':
                    # TF-IDF specific parsing
                    ranking_method = 'tfidf'
                    if query.upper().startswith('BM25:'):
                        ranking_method = 'bm25'
                        query = query[5:].strip()
                    elif query.upper().startswith('TFIDF:'):
                        ranking_method = 'tfidf'
                        query = query[6:].strip()
                    
                    results = indexer.search(query.strip(), top_k=top_k, require_all_terms=require_all_terms, 
                                           ranking_method=ranking_method)
                    mode = "AND" if require_all_terms else "OR"
                    print(f"\n[Search mode: {mode} | Ranking: {ranking_method.upper()}]")
                    indexer.display_results(results)
                else:  # lucene
                    results = searcher.search(query.strip(), top_k=top_k, require_all_terms=require_all_terms)
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
    print("=" * 100)


if __name__ == "__main__":
    main()