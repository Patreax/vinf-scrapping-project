import os
import re
import csv
import glob
from datetime import datetime
from typing import Dict, List, Optional
 
class RegexPatterns:
    """Class containing all regex patterns for data extraction"""
    
    # Company name patterns
    COMPANY_NAME = [
        r'<title>([^<]+)\s*\([A-Z]+\)',  # Title tag pattern
        r'<h1[^>]*>([^<]+)</h1>',  # H1 tag pattern
        r'"companyName":"([^"]+)"',  # JSON company name
        r'<meta[^>]*name="description"[^>]*content="([^"]+)"',  # Meta description
    ]
    
    # Exchange patterns
    EXCHANGE = [
        r'Primary exchange.*?class="P6K39c">([^<]+)<',
    ]
    
    # Current price patterns
    CURRENT_PRICE = [
        r'data-last-price="([\d.]+)"',
    ]
    
    # Market cap patterns
    MARKET_CAP = [
        r'Market cap.*?class="P6K39c">([^<]+)<',
    ]
    
    # Founded year patterns
    FOUNDED_YEAR = [
        r'Founded.*?class="P6K39c">([^<]+)<',
    ]
    
    # Employee count patterns
    EMPLOYEES = [
        r'<div class="P6K39c">(\d{1,3}(?:,\d{3})*)</div>',
    ]
    
    # Revenue patterns
    REVENUE = [
        r'Revenue.*?class="QXDnM">([^<]+)<',
    ]
    
    # EBITDA patterns
    EBITDA = [
        r'EBITDA.*?class="QXDnM">([^<]+)<',
    ]

    # Previous close patterns
    PREVIOUS_CLOSE = [
        r'Previous close.*?class="P6K39c">([\$]?\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
    ]

class StockDataExtractor:
    """Main class for extracting stock data from HTML content"""
    
    def __init__(self):
        self.patterns = RegexPatterns()
    
    def extract_company_name(self, html_content: str, symbol: str) -> str:
        """Extract company name from the HTML content"""
        for pattern in self.patterns.COMPANY_NAME:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                company_name = matches[0].strip()
                if company_name:
                    return company_name
                
        # Default value
        return f"{symbol} Corporation"
    
    def extract_exchange(self, html_content: str, symbol: str) -> Optional[str]:
        """Extract exchange from the HTML content"""
        for pattern in self.patterns.EXCHANGE:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                exchange = matches[0].strip().upper()
                if exchange:
                    return exchange
        
        # Default value
        return None
    
    def extract_current_price(self, html_content: str) -> Optional[str]:
        """Extract current stock price from HTML content"""
        for pattern in self.patterns.CURRENT_PRICE:
            matches = re.search(pattern, html_content, re.DOTALL)
            if matches:
                price = matches.group(1).replace(',', '').replace('$', '')
                return f"${price}"
        return None
    
    def extract_previous_close(self, html_content: str) -> Optional[str]:
        """Extract previous close price from HTML content"""
        for pattern in self.patterns.PREVIOUS_CLOSE:
            matches = re.search(pattern, html_content, re.DOTALL)
            if matches:
                previous_close = matches.group(1).replace(',', '').replace('$', '')
                return f"${previous_close}"
        return None
    
    def extract_market_cap(self, html_content: str) -> Optional[str]:
        """Extract market cap from HTML content"""
        for pattern in self.patterns.MARKET_CAP:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                return matches[0]
        return None
    
    def extract_founded_year(self, html_content: str) -> Optional[str]:
        """Extract founded year from HTML content"""
        for pattern in self.patterns.FOUNDED_YEAR:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                return matches[0]
        return None
    
    def extract_employees(self, html_content: str) -> Optional[str]:
        """Extract number of employees from HTML content"""
        for pattern in self.patterns.EMPLOYEES:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                employees = matches[0].replace(',', '')
                return f"{employees}"
        return None
    
    def extract_revenue(self, html_content: str) -> Optional[str]:
        """Extract revenue from HTML content"""
        for pattern in self.patterns.REVENUE:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                return matches[0].strip()
        return None
    
    def extract_ebitda(self, html_content: str) -> Optional[str]:
        """Extract EBITDA from HTML content"""
        for pattern in self.patterns.EBITDA:
            matches = re.search(pattern, html_content, re.DOTALL)
            if matches:
                return matches.group(1).strip()
        return None
    
    def calculate_price_changes(self, current_price: Optional[str], previous_close: Optional[str]) -> Dict[str, str]:
        """Calculate percentage change and difference between current price and previous close"""
        changes = {}
        
        if current_price and previous_close:
            try:
                current_price_str = current_price.replace('$', '').replace(',', '')
                previous_close_str = previous_close.replace('$', '').replace(',', '')
                
                current_price_val = float(current_price_str)
                previous_close_val = float(previous_close_str)
                
                raw_difference = current_price_val - previous_close_val
                
                percentage_change = raw_difference / previous_close_val * 100
                changes["calculated_percentage_change"] = f"{percentage_change:+.2f}%"
                
                changes["calculated_difference"] = f"${raw_difference:+.2f}"
                
            except (ValueError, ZeroDivisionError) as e:
                print(f"Could not calculate percentage change: {e}")
        
        return changes
    
    def extract_stock_data_from_html(self, html_content: str, filename: str) -> Dict[str, any]:
        """
        Extract stock data from Google Finance HTML content
        
        Args:
            html_content: The HTML content to parse
            filename: The filename (used to extract symbol)
        
        Returns:
            Dictionary containing extracted stock data
        """
        # Extract symbol from filename
        symbol = os.path.basename(filename).split('_')[0]
        company_name = self.extract_company_name(html_content, symbol)
        exchange = self.extract_exchange(html_content, symbol)
        
        data = {
            "company": company_name,
            "symbol": symbol,
            "exchange": exchange,
            "source_file": filename,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Extract all financial data using regex patterns
        current_price = self.extract_current_price(html_content)
        if current_price:
            data["current_price"] = current_price
        
        market_cap = self.extract_market_cap(html_content)
        if market_cap:
            data["market_cap"] = market_cap
        
        founded_year = self.extract_founded_year(html_content)
        if founded_year:
            data["founded"] = founded_year
        
        employees = self.extract_employees(html_content)
        if employees:
            data["employees"] = employees
        
        revenue = self.extract_revenue(html_content)
        if revenue:
            data["revenue"] = revenue
        
        ebitda = self.extract_ebitda(html_content)
        if ebitda:
            data["ebitda"] = ebitda
        
        previous_close = self.extract_previous_close(html_content)
        if previous_close:
            data["previous_close"] = previous_close
        
        # Calculate price changes if we have both prices
        price_changes = self.calculate_price_changes(current_price, previous_close)
        data.update(price_changes)
        
        return data


def save_to_tsv(stock_data: Dict[str, any], filename: str) -> bool:
    """
    Save stock data to TSV file with tab delimiter and header columns
    
    Args:
        stock_data: Dictionary containing stock data
        filename: Output TSV filename
    
    Returns:
        True if successful, False otherwise
    """
    if not stock_data:
        return False
    
    try:
        # Define the order of columns for the TSV
        fieldnames = [
            "company",
            "symbol", 
            "exchange",
            "source_file",
            "timestamp",
            "current_price",
            "previous_close",
            "calculated_percentage_change",
            "calculated_difference",
            "market_cap",
            "founded",
            "employees",
            "revenue",
            "ebitda"
        ]
        
        # Write data to TSV with tab delimiter
        with open(filename, 'a', newline='', encoding='utf-8') as tsvfile:
            writer = csv.DictWriter(tsvfile, fieldnames=fieldnames, delimiter='\t')
            writer.writerow(stock_data)
        
        return True
        
    except Exception as e:
        print(f"Error saving to TSV: {e}")
        return False

def process_html_file(html_file: str, output_file: str = "data/extracted_data.tsv", extractor: Optional[StockDataExtractor] = None) -> Dict[str, any]:
    """
    Process a single HTML file and extract stock data
    
    Args:
        html_file: Path to HTML file to process
        output_file: Output TSV filename
        extractor: Optional StockDataExtractor instance to reuse (creates new one if None)
    
    Returns:
        Dictionary containing extracted stock data
    """
    
    if extractor is None:
        extractor = StockDataExtractor()
    
    try:
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
                
        stock_data = extractor.extract_stock_data_from_html(html_content, html_file)
        
        if stock_data:
            if save_to_tsv(stock_data, output_file):
                print(f"Successfully processed {stock_data['symbol']}")
                return stock_data
            else:
                print(f"Failed to save data for {stock_data['symbol']}")
                return None
        else:
            print(f"Failed to extract data from {html_file}")
            return None
            
    except Exception as e:
        print(f"Error processing {html_file}: {e}")
        return None
