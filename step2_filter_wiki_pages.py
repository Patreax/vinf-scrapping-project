"""
Step 2: Filter Wikipedia pages by match keys

This script:
1. Reads match_keys.txt from step1 (lowercase first words, 3+ characters)
2. Iterates through Wikipedia dump (bz2)
3. Filters pages where title matches any match_key
4. Saves filtered pages to a standalone directory for later processing
"""

import argparse
import re
import os
from pyspark.sql import SparkSession, Row

def load_match_keys(keys_file):
    """Load match keys from file, one per line."""
    keys = set()
    if not os.path.exists(keys_file):
        raise FileNotFoundError(f"Match keys file not found: {keys_file}")
    
    with open(keys_file, 'r', encoding='utf-8') as f:
        for line in f:
            key = line.strip().lower()
            if key and len(key) >= 3:  # Ensure 3+ characters
                keys.add(key)
    
    return keys

def main():
    # Program parameters
    parser = argparse.ArgumentParser(description="Filter Wikipedia pages by match keys")
    parser.add_argument("--wiki", required=True, 
                       help="Path to enwiki-latest-pages-articles.xml.bz2")
    parser.add_argument("--keys", default="data/match_keys.txt",
                       help="Path to match_keys.txt from step1 (default: data/match_keys.txt)")
    parser.add_argument("--out", default="out/wiki_filtered_pages",
                       help="Output directory for filtered pages (default: out/wiki_filtered_pages)")
    parser.add_argument("--format", choices=["json", "parquet"], default="parquet",
                       help="Output format (default: parquet)")
    parser.add_argument("--partitions", type=int, default=0,
                       help="Min partitions for reading bzip2 file (0 = auto based on CPU)")
    parser.add_argument("--only-ns0", action="store_true", default=True,
                       help="Keep only namespace 0 (main) pages (default: True)")
    parser.add_argument("--skip-redirects", action="store_true",
                       help="Skip pages that are redirects")
    args = parser.parse_args()

    print("=" * 60)
    print("STAGE 2: Filtering Wikipedia pages by match keys")
    print("=" * 60)
    
    # Load match keys
    print(f"\nLoading match keys from {args.keys}...")
    match_keys = load_match_keys(args.keys)
    print(f"Loaded {len(match_keys)} unique match keys")
    
    if not match_keys:
        print("ERROR: No match keys found!")
        return
    
    # Show sample keys
    sample_keys = sorted(list(match_keys))[:10]
    print(f"Sample keys: {', '.join(sample_keys)}...")
    
    # Initialize Spark
    import os as os_module
    num_cores = os_module.cpu_count() or 4
    min_parts = args.partitions if args.partitions > 0 else num_cores * 2
    
    spark = (
        SparkSession.builder
        .appName("step2-filter-wiki-pages")
        .config("spark.sql.files.maxPartitionBytes", 134217728)  # 128 MB
        .config("spark.sql.shuffle.partitions", num_cores * 2)
        .config("spark.default.parallelism", num_cores)
        .getOrCreate()
    )
    
    sc = spark.sparkContext
    
    # Broadcast match keys for efficient filtering
    print(f"\nBroadcasting {len(match_keys)} match keys to workers...")
    keys_bc = sc.broadcast(match_keys)
    
    # Read Wikipedia dump as text
    print(f"\nReading Wikipedia dump from {args.wiki}...")
    print(f"Using {min_parts} partitions...")
    text_rdd = sc.textFile(args.wiki, minPartitions=min_parts)
    
    # Regex patterns
    title_re = re.compile(r"<title>(.*?)</title>", re.IGNORECASE)
    ns0_re = re.compile(r"<ns>\s*0\s*</ns>")  # main namespace
    redirect_re = re.compile(r"^#REDIRECT\s", re.IGNORECASE | re.MULTILINE)
    
    def filter_pages(iterator):
        """Filter pages by title matching any match key."""
        keys = keys_bc.value
        buf = []
        
        for line in iterator:
            buf.append(line)
            
            # Check for end of page
            if "</page>" in line:
                page = "\n".join(buf)
                buf = []  # reset buffer
                
                # Filter by namespace (only main namespace)
                if args.only_ns0:
                    if not ns0_re.search(page):
                        continue
                
                # Extract title
                m = title_re.search(page)
                if not m:
                    continue
                
                title = m.group(1).strip()
                title_lower = title.lower()
                
                # Extract words from title (lowercase)
                title_words = title_lower.split()
                title_first_word = title_words[0] if title_words else ""
                
                # Check if title matches any match key
                # Since match keys are first words, check:
                # 1. First word of title equals a match key
                # 2. Any complete word in title equals a match key (not substring)
                matched = False
                if title_first_word in keys:
                    matched = True
                else:
                    # Fallback: check if any key matches any complete word in the title
                    # This prevents substring matches (e.g., "ark" matching "Mark")
                    title_words_set = set(title_words)
                    for key in keys:
                        if key in title_words_set:
                            matched = True
                            break
                
                if matched:
                    # Optional: skip redirects
                    if args.skip_redirects and redirect_re.search(page):
                        continue
                    
                    yield (title, page)
    
    print("\nFiltering and saving pages (single pass through Wikipedia)...")
    filtered_rdd = text_rdd.mapPartitions(filter_pages)
    
    # Convert to DataFrame
    df = spark.createDataFrame(
        filtered_rdd.map(lambda tp: Row(title=tp[0], page=tp[1])),
        schema="title string, page string"
    )
    
    # Save filtered pages immediately - single pass through Wikipedia
    # Pages are filtered as they're read and saved directly
    print(f"\nSaving filtered pages to {args.out}...")
    os.makedirs(os.path.dirname(args.out) if os.path.dirname(args.out) else '.', exist_ok=True)
    
    if args.format == "json":
        df.write.mode("overwrite").json(args.out)
    else:  # parquet
        df.write.mode("overwrite").option("compression", "snappy").parquet(args.out)
    
    print(f"\nSuccess! Filtered pages saved to: {args.out}")
    print(f"   Format: {args.format.upper()}")
    print(f"   Columns: title (string), page (string)")
    
    spark.stop()

if __name__ == "__main__":
    main()

