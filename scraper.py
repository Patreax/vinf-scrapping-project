import requests
import time
import random
import os
import re
import json
import threading
from extractor import process_html_file, StockDataExtractor
from datetime import datetime
from typing import List, Dict, Optional

QUEUES_DIR = "queues"
HTML_DIR = "html"
DATA_DIR = "data"

# If enabled, the scraper will download URLs from the page and add them to the stack
# If disabled, the scraper will download only the URLs in the stack, and will not add any new URLs to the stacks
ADD_EXTRACTED_URLS_TO_STACK = False

WEB_PAGE_METADATA_FILE = f"{DATA_DIR}/web-page-metadata.tsv"
EXTRACTED_DATA_FILE = f"{DATA_DIR}/extracted_data.tsv"

URL_STACK_FILE = f"{QUEUES_DIR}/web-url-stack.txt"
PAGE_EXTRACTION_STACK_FILE = f"{QUEUES_DIR}/page-extraction-stack.txt"

WEB_PAGE_METADATA_HEADER = "url\tfile_path\ttimestamp\tstatus\n"
EXTRACTED_DATA_HEADER = "company\tsymbol\texchange\tsource_file\ttimestamp\tcurrent_price\tprevious_close\tcalculated_percentage_change\tcalculated_difference\tmarket_cap\tfounded\temployees\trevenue\tebitda\n"

# https://www.google.com/finance/quote/NVDA:NASDAQ

def read_and_remove_last_url(stack_file: str = URL_STACK_FILE) -> Optional[str]:
    """
    Read the last URL from the stack file and remove it
    
    Args:
        stack_file: Path to the URL stack file
    
    Returns:
        The last URL from the file, or None if file is empty or doesn't exist
    """
    if not os.path.exists(stack_file):
        print(f"URL stack file not found: {stack_file}")
        return None
    
    try:
        # Read all URLs from the file
        with open(stack_file, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f.readlines() if line.strip()]
        
        if not urls:
            print(f"URL stack file is empty: {stack_file}")
            return None
        
        # Get the last URL
        last_url = urls[-1]
        
        # Remove the last URL from the list
        remaining_urls = urls[:-1]
        
        # Write remaining URLs back to file
        with open(stack_file, 'w', encoding='utf-8') as f:
            for url in remaining_urls:
                f.write(url + '\n')
                
        return last_url
        
    except Exception as e:
        print(f"Error reading URL from stack: {e}")
        return None


def add_urls_to_stack(urls: List[str], stack_file: str = URL_STACK_FILE) -> bool:
    """
    Add new URLs to the stack file
    
    Args:
        urls: List of URLs to add
        stack_file: Path to the URL stack file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        with open(stack_file, 'a', encoding='utf-8') as f:
            for url in urls:
                f.write(url + '\n')
        
        return True
        
    except Exception as e:
        print(f"Error adding URLs to stack: {e}")
        return False


def add_html_to_extraction_stack(html_filepath: str, extraction_stack_file: str = PAGE_EXTRACTION_STACK_FILE) -> bool:
    """
    Add HTML file path to the extraction stack for processing by the extractor
    
    Args:
        html_filepath: Path to the HTML file to add to extraction stack
        extraction_stack_file: Path to the extraction stack file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        with open(extraction_stack_file, 'a', encoding='utf-8') as f:
            f.write(html_filepath + '\n')
        
        return True
        
    except Exception as e:
        print(f"Error adding HTML file to extraction stack: {e}")
        return False


def get_enhanced_headers():
    """Get enhanced headers to bypass consent page"""
    # List of common Chrome versions
    chrome_versions = ["119.0.0.0", "120.0.0.0", "121.0.0.0"]
    # List of common Windows versions
    windows_versions = ["10.0", "11.0"]
    
    # Randomly select the following versions
    chrome_ver = random.choice(chrome_versions)
    windows_ver = random.choice(windows_versions)
    
    return {
        "User-Agent": f"Mozilla/5.0 (Windows NT {windows_ver}; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_ver} Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br", 
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate", 
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
        "Referer": "https://www.google.com/",
        "Sec-Ch-Ua": f'"Not_A Brand";v="8", "Chromium";v="{chrome_ver.split(".")[0]}", "Google Chrome";v="{chrome_ver.split(".")[0]}"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "From": "xtomco@stuba.sk",  # Serves for identification of the scraper
    }


def extract_urls_from_page(html_content: str, base_url: str) -> List[str]:
    """
    Extract all URLs from the current page HTML content
    
    Args:
        html_content: The HTML content of the page
        base_url: The base URL of the current page
    
    Returns:
        List of extracted URLs
    """
    
    # Extract URLs matching Google Finance stock quotes
    quote_urls = re.findall(r'href="\./(quote/[^"]+)"', html_content)
    
    # Convert relative URLs to absolute
    extracted_urls = set([f"https://www.google.com/finance/{url}" for url in quote_urls])
    
    if not extracted_urls:
        return []
    
    # Filter out URLs that were already processed or are in stack
    try:
        processed_urls = set()
        stack_urls = set()
        
        # Get processed URLs from metadata file
        if os.path.exists(WEB_PAGE_METADATA_FILE):
            with open(WEB_PAGE_METADATA_FILE, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line_num == 1:  # Skip header line
                        continue
                    line = line.strip()
                    if line:  # Skip empty lines
                        parts = line.split('\t')
                        if len(parts) >= 1:  # At least URL column exists
                            processed_urls.add(parts[0])
        
        # Get URLs already in stack
        if os.path.exists(URL_STACK_FILE):
            with open(URL_STACK_FILE, 'r', encoding='utf-8') as f:
                stack_urls = set(line.strip() for line in f)
        
        new_urls = [url for url in extracted_urls 
                   if url not in processed_urls and url not in stack_urls]
        
        print(f"Found {len(new_urls)} new URLs.")
        
        return new_urls
        
    except Exception as e:
        print(f"Error filtering URLs: {e}")
        return []


def download_stock_page(url: str) -> Dict[str, any]:
    """
    Scrape a single stock page from Google Finance
    
    Args:
        url: The URL to scrape
    
    Returns:
        Dictionary with scraping results
    """
    symbol = ""
    exchange = ""
    
    url_parts = url.split('/')
    if len(url_parts) > 0:
        last_part = url_parts[-1]
        if ':' in last_part:
            symbol, exchange = last_part.split(':')
        else:
            symbol = last_part
    
    headers = get_enhanced_headers()
    
    try:
        session = requests.Session()
        session.headers.update(headers)
        
        cookies = {
            'CONSENT': 'YES+cb.20210328-17-p0.en+FX+667',
            'SOCS': 'CAI',
            'NID': '511=example_value'
        }
        session.cookies.update(cookies)
        
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        print(f"Successfully downloaded HTML from url: {url}")
        
        return {
            "success": True,
            "symbol": symbol,
            "url": url,
            "html_content": response.text,
            "content_length": len(response.text),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
    except Exception as e:
        print(f"Error scraping {symbol}: {e}")
        return {
            "success": False,
            "symbol": symbol,
            "url": url,
            "error": str(e),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


def save_html_to_file(html_content: str, symbol: str, html_dir: str) -> str:
    """
    Save HTML content to a file
    
    Args:
        html_content: The HTML content to save
        symbol: Stock symbol for filename
        html_dir: Directory to save the file
    
    Returns:
        Path to the saved file
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{symbol}_{timestamp}.html"
    filepath = os.path.join(html_dir, filename)
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML saved to: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"Error saving HTML file: {e}")
        return None


def write_page_metadata(page_metadata: Dict[str, any], filename: str):
    """
    Write page metadata to TSV file
    
    Args:
        page_metadata: Dictionary containing page metadata
        filename: Name of TSV file to write to
    """
    
    # Append metadata
    with open(filename, 'a', encoding='utf-8') as f:
        url = page_metadata['url']
        html_file = page_metadata.get('saved_file', '')
        timestamp = page_metadata['timestamp']
        status = 'success' if page_metadata['success'] else 'failure'
        
        f.write(f"{url}\t{html_file}\t{timestamp}\t{status}\n")


def process_single_url_from_stack() -> bool:
    """
    Process a single URL from the stack file
    
    Returns:
        True if a URL was processed, False if stack is empty
    """
    url = read_and_remove_last_url()
    
    if url is None:
        print("No URLs remaining in stack")
        return False
    
    print(f"\n{'='*60}")
    print(f"Processing URL: {url}")
    
    result = download_stock_page(url)
    
    if result["success"]:
        filepath = save_html_to_file(
            result["html_content"], 
            result["symbol"], 
            HTML_DIR
        )
        
        if filepath:
            add_html_to_extraction_stack(filepath)
            result["saved_file"] = filepath
            
            # Add extracted URLs to the stack if enabled
            if ADD_EXTRACTED_URLS_TO_STACK:
                extracted_urls = extract_urls_from_page(result["html_content"], url)
                if extracted_urls:
                    add_urls_to_stack(extracted_urls)
            
            write_page_metadata(result, WEB_PAGE_METADATA_FILE)
            
        else:
            result["success"] = False
            result["error"] = "Failed to save HTML file"
            print(f"Failed to save HTML file for {result['symbol']}")
    else:
        print(f"Failed to scrape url: {url}")
        write_page_metadata(result, WEB_PAGE_METADATA_FILE)
    
    return True

def create_file_structure():
    # Create data directory if it doesn't exist
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    # Create metadata file if it doesn't exist
    if not os.path.exists(WEB_PAGE_METADATA_FILE):
        with open(WEB_PAGE_METADATA_FILE, 'w', encoding='utf-8') as f:
            f.write(WEB_PAGE_METADATA_HEADER)
            
    # Create extracted data file if it doesn't exist
    if not os.path.exists(EXTRACTED_DATA_FILE):
        with open(EXTRACTED_DATA_FILE, 'w', encoding='utf-8') as f:
            f.write(EXTRACTED_DATA_HEADER)
            
    # Create html directory if it doesn't exist
    if not os.path.exists(HTML_DIR):
        os.makedirs(HTML_DIR)

    # Create queues directory if it doesn't exist
    if not os.path.exists(QUEUES_DIR):
        os.makedirs(QUEUES_DIR)
    

def downloader_worker():
    """
    Downloads HTML pages from the URL stack and saves them.
    """
    
    print("Downloader Worker Started")

    create_file_structure()

    while True:
        if not process_single_url_from_stack():
            print("No more URLs to process. Stopping downloader.")
            break

        delay = random.uniform(1, 3)
        print(f"Waiting {delay:.1f} seconds before next download...")
        time.sleep(delay)

    print(f"\n Downloading completed!")


def extractor_worker():
    """
    Processes HTML files from the extraction stack.
    Waits 1s and retries if the stack is empty.
    """
    
    print("Extractor worker started")
    
    extractor = StockDataExtractor()
    
    while True:
        try:
            with open(PAGE_EXTRACTION_STACK_FILE, "r+") as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]
                if not lines:
                    time.sleep(1)
                    continue

                html_file = lines.pop(0)
                f.seek(0)
                f.truncate()
                f.writelines(line + "\n" for line in lines)

            process_html_file(html_file, output_file=EXTRACTED_DATA_FILE, extractor=extractor)
            print(f"Extracted data from: {html_file}")

        except FileNotFoundError:
            print("Extraction stack file not found, waiting...")
            time.sleep(1)
        except Exception as e:
            print(f"Error in extractor: {e}")
            time.sleep(1)


def main():  
    """
    Main entry point — starts two workers in parallel:
      Downloader: fetches HTML pages.
      Extractor: extracts data from HTML files.
    """
    downloader = threading.Thread(target=downloader_worker, daemon=True)
    extractor = threading.Thread(target=extractor_worker, daemon=True)

    downloader.start()
    extractor.start()

    downloader.join()
    time.sleep(3)
    extractor.join(timeout=2)

    print("All work completed!")

if __name__ == "__main__":
    main()
