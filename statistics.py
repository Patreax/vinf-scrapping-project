import os
import csv

from tiktoken._educational import *

def calculate_statistics():
    
    total_size_mb = calculate_all_size()
    total_tokens = calculate_all_tokens()
    return total_size_mb, total_tokens


def calculate_all_size():
    total_size = 0
    html_dir = "html"
    
    # Iterate through all files in html directory
    for filename in os.listdir(html_dir):
        file_path = os.path.join(html_dir, filename)
        
        # Get file size if it's a file (not directory)
        if os.path.isfile(file_path):
            total_size += os.path.getsize(file_path)
            
    # Convert to MB and round to 2 decimal places
    total_size_mb = round(total_size / (1024 * 1024), 2)
    
    return total_size_mb


def calculate_all_tokens():
    total_tokens = 0
    enc = SimpleBytePairEncoding.from_tiktoken("cl100k_base")
    tsv_path = "data/extracted_data.tsv"
    
    if os.path.exists(tsv_path):
        with open(tsv_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter='\t')
            for row in reader:
                html = row[0]
                tokens = enc.encode(html)
                total_tokens += len(tokens)
    
    return total_tokens


def calculate_number_of_pages():
    number_of_pages = 0
    html_dir = "html"
    for filename in os.listdir(html_dir):
        file_path = os.path.join(html_dir, filename)
        if os.path.isfile(file_path):
            number_of_pages += 1
    return number_of_pages

def main():
    statistics_path = "statistics/statistics.txt"
    
    total_size_mb, total_tokens = calculate_statistics()
    
    number_of_pages = calculate_number_of_pages()
    
    total_size_gb = round(total_size_mb / 1024, 3)
    
    print(f"Total size: {total_size_gb} GB")
    print(f"Total size: {total_size_mb} MB")
    print(f"Number of pages: {number_of_pages}")
    print(f"Relevant pages: 100%")
    print(f"Total tokens: {total_tokens}")
    
    with open(statistics_path, 'w', encoding='utf-8') as file:
        file.write(f"Total size: {total_size_mb} MB\n")
        total_size_gb = round(total_size_mb / 1024, 3)
        file.write(f"Total size: {total_size_gb} GB\n")
        file.write(f"Number of pages: {number_of_pages}\n")   
        file.write(f"Relevant pages: 100%\n")
        file.write(f"Total tokens: {total_tokens}\n")
    
    
if __name__ == "__main__":
    main()