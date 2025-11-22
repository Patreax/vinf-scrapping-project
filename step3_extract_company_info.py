"""
Step 3: Extract company information from filtered Wikipedia pages

This script:
1. Reads filtered Wikipedia pages from step2
2. Extracts: founders, headquarters, industry, traded_as, website, first_paragraph
3. Only keeps pages about public companies (have traded_as)
4. Matches pages to keywords from step1
5. Saves to TSV with keyword and extracted fields
"""

import argparse
import re
import os
import csv
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import udf

# Load match keys to map titles to keywords
def load_match_keys(keys_file):
    """Load match keys from file."""
    keys = set()
    if not os.path.exists(keys_file):
        raise FileNotFoundError(f"Match keys file not found: {keys_file}")
    
    with open(keys_file, 'r', encoding='utf-8') as f:
        for line in f:
            key = line.strip().lower()
            if key and len(key) >= 3:
                keys.add(key)
    
    return keys

# Wikitext extraction helpers (from pipeline)
INFOBOX_BLOCK = re.compile(r"\{\{\s*Infobox[\s\S]*?\n\}\}", re.IGNORECASE)

def _clean_wiki_markup(v: str) -> str:
    if not v:
        return ""
    # Remove HTML comments first
    v = re.sub(r"<!--.*?-->", "", v, flags=re.DOTALL)
    # Remove ref tags and their content
    v = re.sub(r"<ref[^>]*>.*?</ref>", "", v, flags=re.DOTALL | re.IGNORECASE)
    v = re.sub(r"<ref[^>]*/>", "", v, flags=re.IGNORECASE)
    # Remove templates
    v = re.sub(r"\{\{.*?\}\}", "", v, flags=re.DOTALL)
    # Remove wiki links but keep text
    v = re.sub(r"\[\[(?:[^\]|]*\|)?([^\]]+)\]\]", r"\1", v)
    # Remove external links but keep text
    v = re.sub(r"\[([^\]]+)\]", r"\1", v)
    # Remove HTML tags
    v = re.sub(r"<[^>]+>", "", v)
    # Decode HTML entities
    v = v.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    v = v.replace("&nbsp;", " ").replace("&quot;", '"').replace("&#39;", "'")
    # Remove template parameters that might be left (e.g., | param =)
    v = re.sub(r"\|\s*\w+\s*=", "", v)
    # Remove image/file references
    v = re.sub(r"(?i)(image|file):[^\s]+", "", v)
    # Remove citation remnants
    v = re.sub(r"\|[^|]*=.*?}}", "", v)
    # Remove triple quotes (''' or '''''') used for bold/italic in Wikipedia
    v = re.sub(r"'{3,}", "", v)
    # Clean up whitespace
    v = " ".join(v.split())
    return v.strip()

def parse_infobox_py(wikitext: str):
    """Parse infobox fields from wikitext."""
    if not wikitext:
        return {}
    m = INFOBOX_BLOCK.search(wikitext)
    if not m:
        return {}
    block = m.group(0)
    out = {}
    for line in block.splitlines():
        line = line.strip()
        if line.startswith("|") and "=" in line:
            try:
                k, v = line.split("=", 1)
                k = k.strip("| ").lower()
                # Skip if key is empty or looks like a template parameter
                if not k or k.startswith("<!--") or "<!--" in k:
                    continue
                v = v.strip()
                # Skip if value is just a comment or template
                if v.startswith("<!--") or (v.startswith("|") and "=" in v):
                    continue
                v = _clean_wiki_markup(v)
                if v and len(v) > 0:
                    out[k] = v
            except ValueError:
                pass
    return out

def _extract_field_list(wikitext: str, field_names_regex: str):
    """Extract list field from wikitext."""
    if not wikitext:
        return []
    # First try to get from infobox
    infobox = parse_infobox_py(wikitext)
    # Try common field name variations - extract base names from regex
    # Handle patterns like "industr(?:y|ies)" -> try "industry", "industries"
    # Handle patterns like "headquarters|hq_location|location" -> try all
    field_names_to_try = []
    # Split by | first
    for part in field_names_regex.split("|"):
        part = part.strip()
        # Remove regex groups like (?:y|ies) and extract base
        base = re.sub(r"\([^)]*\)", "", part).strip()
        if base:
            field_names_to_try.append(base)
        # Also try with common suffixes if there was a group
        if "(?:y|ies)" in part or "(y|ies)" in part:
            field_names_to_try.append(base + "y")
            field_names_to_try.append(base + "ies")
    
    for field_name in field_names_to_try:
        if field_name in infobox:
            value = infobox[field_name]
            if value:
                text = _clean_wiki_markup(value)
                parts = re.split(r"[,\n•·;]", text)
                result = [p.strip() for p in parts if p.strip() and len(p.strip()) > 1]
                if result:
                    return result
    
    # Fallback to regex search in wikitext
    pattern = re.compile(rf"\|\s*(?:{field_names_regex})\s*=\s*(.*?)(?:\n\||\n\}})", re.IGNORECASE | re.DOTALL)
    m = pattern.search(wikitext)
    if not m:
        return []
    text = _clean_wiki_markup(m.group(1).strip())
    # Filter out template parameters and other junk
    if text.startswith("|") or "<!--" in text or len(text) < 2:
        return []
    parts = re.split(r"[,\n•·;]", text)
    return [p.strip() for p in parts if p.strip() and len(p.strip()) > 1]

def extract_industries_py(wikitext: str):
    """Extract industries from wikitext."""
    return _extract_field_list(wikitext, r"industr(?:y|ies)")

def extract_founders_py(wikitext: str):
    """Extract founders from wikitext."""
    return _extract_field_list(wikitext, r"founders?")

def extract_headquarters_py(wikitext: str):
    """Extract headquarters from wikitext."""
    return _extract_field_list(wikitext, r"headquarters|hq_location|location")

def extract_website_py(wikitext: str):
    """Extract website URL from wikitext."""
    if not wikitext:
        return ""
    
    # First try to get from infobox
    infobox = parse_infobox_py(wikitext)
    
    # Try common field names for website
    website_fields = ["website", "website_url", "url", "homepage", "official_website", "web"]
    for field in website_fields:
        if field in infobox:
            value = infobox[field]
            if value:
                # Clean the value but preserve URLs
                value = _clean_wiki_markup(value)
                # Extract URL if it's in a link format [url text] -> url
                # Check if it looks like a URL
                url_pattern = re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+', re.IGNORECASE)
                url_match = url_pattern.search(value)
                if url_match:
                    return url_match.group(0)
                # If no http:// found, check if it's a domain name
                domain_pattern = re.compile(r'[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}', re.IGNORECASE)
                domain_match = domain_pattern.search(value)
                if domain_match:
                    domain = domain_match.group(0)
                    # Add http:// if not present
                    if not domain.startswith(('http://', 'https://')):
                        return f"https://{domain}"
                    return domain
                # Return cleaned value if it looks reasonable
                if len(value) > 3 and len(value) < 200 and not value.startswith("|"):
                    return value
    
    # Fallback: search in wikitext for website field
    pattern = re.compile(r"\|\s*(?:website|website_url|url|homepage|official_website)\s*=\s*(.*?)(?:\n\||\n\}})", re.IGNORECASE | re.DOTALL)
    m = pattern.search(wikitext)
    if m:
        text = _clean_wiki_markup(m.group(1).strip())
        if text and len(text) > 3 and len(text) < 200:
            # Try to extract URL
            url_pattern = re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+', re.IGNORECASE)
            url_match = url_pattern.search(text)
            if url_match:
                return url_match.group(0)
            # Try domain pattern
            domain_pattern = re.compile(r'[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.[a-zA-Z]{2,}', re.IGNORECASE)
            domain_match = domain_pattern.search(text)
            if domain_match:
                domain = domain_match.group(0)
                if not domain.startswith(('http://', 'https://')):
                    return f"https://{domain}"
                return domain
            return text
    
    return ""

def extract_traded_as_py(wikitext: str):
    """Extract traded_as from infobox."""
    infobox = parse_infobox_py(wikitext)
    # Check multiple possible field names
    traded_as = infobox.get("traded_as") or infobox.get("symbol") or infobox.get("ticker")
    
    if not traded_as:
        return ""
    
    # Extract content from Wikipedia templates before cleaning
    # Handle templates like {{BSE|500295}} -> BSE:500295
    # Handle {{NSE|VEDL}} -> NSE:VEDL
    # Handle {{Unbulleted list|{{BSE|500295}}|{{NSE|VEDL}}}} -> extract all symbols
    symbols = []
    
    # Find all exchange template patterns like {{BSE|500295}} or {{NSE|VEDL}}
    # Handle nested templates by finding innermost templates first
    # Common exchange codes that appear in templates
    exchange_codes = ['BSE', 'NSE', 'NYSE', 'NASDAQ', 'LSE', 'ASX', 'TSX', 'TSE', 'HKEX', 'XETR', 'FWB']
    
    # Find all patterns like {{EXCHANGE|SYMBOL}} where EXCHANGE is a known exchange code
    # Use a more robust pattern that handles nested braces
    remaining = traded_as
    max_iterations = 10  # Prevent infinite loops
    iteration = 0
    
    while iteration < max_iterations:
        iteration += 1
        # Find the innermost template that matches an exchange code
        found = False
        for exchange_code in exchange_codes:
            # Pattern: {{EXCHANGE_CODE|SYMBOL}} where SYMBOL doesn't contain nested templates
            # Match innermost templates first (those without {{ or }} in the symbol part)
            pattern = r'\{\{' + re.escape(exchange_code) + r'\|([^{}]+)\}\}'
            match = re.search(pattern, remaining, re.IGNORECASE)
            if match:
                symbol = match.group(1).strip()
                # Clean symbol of any remaining template syntax or special chars
                symbol = re.sub(r'[|{}]', '', symbol).strip()
                if symbol and len(symbol) <= 20:  # Reasonable symbol length
                    symbols.append(f"{exchange_code}:{symbol}")
                remaining = remaining[:match.start()] + remaining[match.end():]
                found = True
                break
        
        if not found:
            break
    
    # If we found symbols from templates, join them
    if symbols:
        traded_as = "; ".join(symbols)
    else:
        # Clean the value (original behavior for non-template formats)
        traded_as = _clean_wiki_markup(traded_as)
    
    # Return the cleaned value - no validation, just return what's in the infobox
    return traded_as.strip() if traded_as else ""

def extract_first_paragraph_py(wikitext: str):
    """Extract the first paragraph from a Wikipedia page."""
    if not wikitext:
        return ""
    
    # If the wikitext contains XML tags, extract just the text content
    # Check if it looks like XML (contains <text> tags)
    if "<text" in wikitext and "</text>" in wikitext:
        # Extract content between <text> and </text> tags
        text_match = re.search(r'<text[^>]*>(.*?)</text>', wikitext, re.DOTALL | re.IGNORECASE)
        if text_match:
            wikitext = text_match.group(1)
    
    # Remove the infobox first
    text = INFOBOX_BLOCK.sub("", wikitext)
    
    # Remove any XML metadata that might be at the start
    # Remove patterns like "Title 0 123456 ..." (page metadata)
    text = re.sub(r'^[A-Z][^a-z]*?\d+\s+\d+\s+\d+\s+\d+.*?wikitext', '', text, flags=re.MULTILINE | re.DOTALL)
    
    # Remove revision metadata patterns (e.g., "Title 0 123456 1314501620 1309608472 2025-10-01T19:20:08Z User 12345")
    text = re.sub(r'^[A-Z][^a-z]*?\s+\d+\s+\d+\s+\d+\s+\d+\s+\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z[^\n]*\n', '', text, flags=re.MULTILINE)
    
    # Find the first section header (==) to mark the end of the lead section
    # Split by section headers
    sections = re.split(r'^==+[^=].*?==+', text, flags=re.MULTILINE)
    
    # The first section (before any == header) is the lead section
    lead_section = sections[0] if sections else text
    
    # Remove common Wikipedia metadata patterns
    # Remove revision history, timestamps, etc.
    lead_section = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', '', lead_section)
    lead_section = re.sub(r'/\*.*?\*/', '', lead_section)  # Remove /* comments */
    
    # Remove lines that look like metadata (contain only numbers, timestamps, usernames)
    lines = lead_section.split('\n')
    filtered_lines = []
    for line in lines:
        line_stripped = line.strip()
        # Skip lines that are mostly numbers, timestamps, or look like metadata
        if (not line_stripped or
            re.match(r'^\d+\s+\d+\s+\d+', line_stripped) or  # Starts with numbers
            re.match(r'^[A-Z][^a-z]*?\s+\d+', line_stripped) or  # Pattern like "Title 0 123"
            'wikitext' in line_stripped.lower() or
            'text/x-wiki' in line_stripped.lower()):
            continue
        filtered_lines.append(line)
    lead_section = '\n'.join(filtered_lines)
    
    # Split by double newlines (paragraph breaks)
    paragraphs = re.split(r'\n\n+', lead_section)
    
    for para in paragraphs:
        # Skip if it looks like metadata or templates
        para_stripped = para.strip()
        if not para_stripped:
            continue
        
        # Skip if it starts with common metadata patterns
        if (para_stripped.startswith("|") or 
            para_stripped.startswith("{{") or 
            para_stripped.startswith("<!--") or
            re.match(r'^[A-Z][^a-z]*?\d+', para_stripped) or  # Pattern like "Title 0 123..."
            len(para_stripped) < 30):  # Too short
            continue
        
        # Clean the paragraph
        cleaned = _clean_wiki_markup(para)
        
        # Skip if it's too short after cleaning or looks like metadata
        if len(cleaned) < 50:
            continue
        
        # Check if it looks like a real paragraph
        # Should contain common words that indicate it's descriptive text
        # Wikipedia first paragraphs often contain: "is a", "is an", "was", "are", "is", etc.
        has_common_words = (
            re.search(r'\b(is|was|are|were|has|have|had)\s+(a|an|the)', cleaned, re.IGNORECASE) or
            re.search(r'\b(founded|established|headquartered|based|located)', cleaned, re.IGNORECASE) or
            re.search(r'\b(company|corporation|organization|firm|business)', cleaned, re.IGNORECASE)
        )
        
        # Should have sentence structure (at least one sentence ending)
        has_sentence = re.search(r'[.!?]', cleaned)
        
        # Should look like natural text (not just numbers, symbols, or template syntax)
        has_natural_text = re.search(r'[a-z]{3,}', cleaned, re.IGNORECASE)  # At least one 3+ letter word
        
        if not (has_common_words or (has_sentence and has_natural_text)):
            continue
        
        # This looks like a real paragraph
        # Limit to a reasonable length (first ~1000 characters)
        if len(cleaned) > 1000:
            # Try to cut at a sentence boundary
            # Find the last complete sentence within 1000 chars
            match = re.search(r'^(.{1,1000}[.!?])\s+', cleaned, re.MULTILINE)
            if match:
                cleaned = match.group(1).strip()
            else:
                # Fallback: just take first 1000 chars
                cleaned = cleaned[:1000].strip()
        
        return cleaned
    
    # Fallback: if no good paragraph found, try to get first substantial text from lead section
    # Clean the entire lead section
    cleaned = _clean_wiki_markup(lead_section)
    
    # Remove any remaining metadata patterns
    cleaned = re.sub(r'^\s*\d+\s+\d+\s+\d+.*?$', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'wikitext\s+text/x-wiki', '', cleaned, flags=re.IGNORECASE)
    
    # Get first substantial chunk
    if len(cleaned) > 50:
        # Find first sentence or first 1000 chars
        match = re.search(r'^(.{50,1000}[.!?])\s+', cleaned, re.MULTILINE)
        if match:
            cleaned = match.group(1).strip()
        else:
            # Take first 1000 chars, trying to cut at word boundary
            if len(cleaned) > 1000:
                cleaned = cleaned[:1000].rsplit(' ', 1)[0]  # Cut at last space
            cleaned = cleaned.strip()
        
        if len(cleaned) >= 50:
            return cleaned
    
    return ""

def main():
    # Program parameters
    parser = argparse.ArgumentParser(description="Extract company info from filtered Wikipedia pages")
    parser.add_argument("--pages", default="out/wiki_filtered_pages",
                       help="Path to filtered pages directory (default: out/wiki_filtered_pages)")
    parser.add_argument("--keys", default="data/match_keys_final.txt",
                       help="Path to match_keys.txt (default: data/match_keys_final.txt)")
    parser.add_argument("--out", default="data/company_info.tsv",
                       help="Output TSV file (default: data/company_info.tsv)")
    parser.add_argument("--format", choices=["json", "parquet"], default="json",
                       help="Input format of filtered pages (default: json)")
    args = parser.parse_args()

    print("=" * 60)
    print("STAGE 3: Extracting company information from Wikipedia pages")
    print("=" * 60)
    
    # Load match keys
    print(f"\nLoading match keys from {args.keys}...")
    match_keys = load_match_keys(args.keys)
    print(f"Loaded {len(match_keys)} match keys")
    
    # Broadcast match keys
    spark = (
        SparkSession.builder
        .appName("step3-extract-company-info")
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate()
    )
    
    sc = spark.sparkContext
    keys_bc = sc.broadcast(match_keys)
    
    # Read filtered pages
    print(f"\nReading filtered pages from {args.pages}...")
    if args.format == "json":
        df = spark.read.json(args.pages)
    else:
        df = spark.read.parquet(args.pages)
    
    print(f"Loaded {df.count():,} pages")
    
    # Register UDFs
    parse_infobox = udf(parse_infobox_py, T.MapType(T.StringType(), T.StringType()))
    extract_industries = udf(extract_industries_py, T.ArrayType(T.StringType()))
    extract_founders = udf(extract_founders_py, T.ArrayType(T.StringType()))
    extract_headquarters = udf(extract_headquarters_py, T.ArrayType(T.StringType()))
    extract_traded_as = udf(extract_traded_as_py, T.StringType())
    extract_website = udf(extract_website_py, T.StringType())
    extract_first_paragraph = udf(extract_first_paragraph_py, T.StringType())
    
    # Extract keyword from title - create UDF that uses broadcast variable
    def get_keyword_func(title):
        if not title:
            return None
        keys = keys_bc.value
        title_lower = title.lower()
        title_words = title_lower.split()
        if title_words:
            first_word = title_words[0]
            if first_word in keys:
                return first_word
            # Fallback: check if any key is in the title
            for key in keys:
                if key in title_lower:
                    return key
        return None
    
    get_keyword = udf(get_keyword_func, T.StringType())
    
    # Extract information
    print("\nExtracting company information...")
    extracted = (
        df
        .withColumn("keyword", get_keyword(F.col("title")))
        .withColumn("infobox", parse_infobox(F.col("page")))
        .withColumn("traded_as", extract_traded_as(F.col("page")))
        .withColumn("industries", extract_industries(F.col("page")))
        .withColumn("founders", extract_founders(F.col("page")))
        .withColumn("headquarters", extract_headquarters(F.col("page")))
        .withColumn("website", extract_website(F.col("page")))
        .withColumn("first_paragraph", extract_first_paragraph(F.col("page")))
        # Only keep pages with traded_as (public companies)
        .filter(F.col("traded_as").isNotNull() & (F.col("traded_as") != ""))
        # Only keep pages with a matching keyword
        .filter(F.col("keyword").isNotNull())
        .select(
            "keyword",
            "title",
            "traded_as",
            F.concat_ws("; ", F.col("industries")).alias("industries"),
            F.concat_ws("; ", F.col("founders")).alias("founders"),
            F.concat_ws("; ", F.col("headquarters")).alias("headquarters"),
            "website",
            "first_paragraph"
        )
    )
    
    count = extracted.count()
    print(f"\nFound {count:,} public companies with matching keywords")
    
    if count == 0:
        print("No matching companies found. Exiting.")
        spark.stop()
        return
    
    # Save to TSV
    print(f"\nSaving to {args.out}...")
    os.makedirs(os.path.dirname(args.out) if os.path.dirname(args.out) else '.', exist_ok=True)
    
    # Collect and write as TSV
    print("Collecting results...")
    rows = extracted.collect()
    
    print(f"Writing {len(rows)} records to TSV...")
    with open(args.out, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["keyword", "title", "traded_as", "industries", "founders", "headquarters", "website", "first_paragraph"], 
                               delimiter='\t', extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "keyword": row["keyword"] or "",
                "title": row["title"] or "",
                "traded_as": row["traded_as"] or "",
                "industries": row["industries"] or "",
                "founders": row["founders"] or "",
                "headquarters": row["headquarters"] or "",
                "website": row["website"] or "",
                "first_paragraph": row["first_paragraph"] or ""
            })
    
    print(f"\nSuccess! Saved {count:,} company records to: {args.out}")
    print("   Columns: keyword, title, traded_as, industries, founders, headquarters, website, first_paragraph")
    
    spark.stop()

if __name__ == "__main__":
    main()

