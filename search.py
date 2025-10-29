"""
Search interface for the Stock Market TF-IDF Index.
This script provides a simple way to search the indexed stock data.

Usage:
    python search.py                              # Full dataset, latest snapshots only
    python search.py --example                   # Example dataset, latest snapshots only
    python search.py --all-history               # Full dataset, all historical snapshots
    python search.py --example --all-history     # Example dataset, all historical snapshots
"""

import sys
from indexer import StockIndexer


def main():
    # Parse command-line arguments
    use_example = False
    use_latest_only = True  # Default to True
    
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--example":
            use_example = True
        elif arg == "--all-history":
            use_latest_only = False
    
    # Check if user wants to use example data or full data
    if use_example:
        data_file = "data/example_extracted_data.tsv"
        print("Using example data file: data/example_extracted_data.tsv")
    else:
        data_file = "data/extracted_data.tsv"
        print("Using full data file: data/extracted_data.tsv")
    
    print(f"\nInitializing indexer (use_latest_only={use_latest_only})...")
    indexer = StockIndexer(data_file=data_file)
    
    # Try to load existing index first
    index_filename = f"indexes/{data_file.split('/')[-1].replace('.tsv', '_index.pkl')}"
    try:
        indexer.load_index(index_filename)
        print("\n✓ Loaded existing index")
    except FileNotFoundError:
        print("\n✗ No existing index found, building new one...")
        indexer.load_data()
        indexer.build_index(use_latest_only=use_latest_only)
        indexer.save_index(index_filename)
        print(f"✓ Index saved to {index_filename}")
    
    # Print statistics
    indexer.print_statistics()
    
    # Interactive search mode
    print("\n" + "=" * 100)
    print("INTERACTIVE SEARCH MODE")
    print("=" * 100)
    print("\nEnter search queries (or 'quit' to exit, 'help' for examples)")
    
    while True:
        try:
            query = input("\nSearch> ").strip()
            
            if query.lower() in ['quit', 'exit', 'q']:
                break
            
            if query.lower() == 'help':
                print_help()
                continue
            
            if not query:
                continue
            
            # Parse search mode (AND/OR) and ranking method if specified
            require_all_terms = True  # Default to AND
            ranking_method = 'tfidf'  # Default to TF-IDF
            
            if query.upper().startswith('OR:'):
                require_all_terms = False
                query = query[3:].strip()
            elif query.upper().startswith('AND:'):
                require_all_terms = True
                query = query[4:].strip()
            
            # Check for ranking method specification
            if query.upper().startswith('BM25:'):
                ranking_method = 'bm25'
                query = query[5:].strip()
            elif query.upper().startswith('TFIDF:'):
                ranking_method = 'tfidf'
                query = query[6:].strip()
            
            # Parse top_k if specified (e.g., "Nike:5" for top 5 results)
            top_k = 10
            if ':' in query:
                query, top_k_str = query.rsplit(':', 1)
                try:
                    top_k = int(top_k_str.strip())
                except ValueError:
                    pass
            
            results = indexer.search(query.strip(), top_k=top_k, require_all_terms=require_all_terms, 
                                   ranking_method=ranking_method)
            
            # Show which mode was used
            mode = "AND" if require_all_terms else "OR"
            method = ranking_method.upper()
            print(f"\n[Search mode: {mode} | Ranking: {method}]")
            
            indexer.display_results(results)
            
        except (KeyboardInterrupt, EOFError):
            break
    
    print("\nGoodbye!")


def print_help():
    """Print help information about search syntax."""
    print("\n" + "=" * 100)
    print("SEARCH HELP")
    print("=" * 100)
    print("\nQuery Syntax:")
    print("  - Simple text search: 'Nike', 'IBM', 'nvidia'")
    print("  - Specify number of results: 'nvidia:20' (returns top 20)")
    print("\nSearch Modes:")
    print("  - AND: (default) Only return documents matching ALL terms")
    print("    Example: 'exchange_nasdaq move_flat' or 'AND: exchange_nasdaq move_flat'")
    print("  - OR: Return documents matching ANY term")
    print("    Example: 'OR: exchange_nasdaq move_flat'")
    print("\nRanking Methods:")
    print("  - TF-IDF: (default) Cosine similarity with TF-IDF weighting")
    print("    Example: 'Nike' or 'TFIDF: Nike'")
    print("  - BM25: Better handling of term saturation and document length")
    print("    Example: 'BM25: exchange_nyse cap_large'")
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