import argparse
import os
import sys
import logging
import pdfkit
from bs4 import BeautifulSoup
from pdf2image import convert_from_bytes
from threading import Thread, Lock
from queue import Queue

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
script_dir = os.path.dirname(os.path.abspath(__file__))

config_pdfkit = pdfkit.configuration(
    wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
)

print_lock = Lock()


def remove_specific_inline_styles_within_tables(html_content):
    properties_to_replace = {
        'font-family': 'Arial',
        'font-size': '12px',
        'font-weight': '500',
        'word-spacing': '5px'
    }

    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    for table in soup.find_all('table'):
        for tag in table.find_all(style=True):
            style_content = tag['style']

            style_rules = [rule.strip() for rule in style_content.split(';') if rule.strip()]

            updated_style_rules = []
            for rule in style_rules:
                property_name, _, property_value = rule.partition(':')
                property_name = property_name.strip().lower()
                property_value = property_value.strip()

                if property_name in properties_to_replace:
                    updated_style_rules.append(f"{property_name}: {properties_to_replace[property_name]}")
                else:
                    updated_style_rules.append(f"{property_name}: {property_value}")

            if any('font' in rule for rule in style_rules):
                style_rules = [rule for rule in style_rules if not rule.startswith('font:')]

                if 'font-family' not in style_rules:
                    updated_style_rules.append(f"font-family: {properties_to_replace['font-family']}")
                if 'font-size' not in style_rules:
                    updated_style_rules.append(f"font-size: {properties_to_replace['font-size']}")
                if 'font-weight' not in style_rules:
                    updated_style_rules.append(f"font-weight: {properties_to_replace['font-weight']}")

            if updated_style_rules:
                tag['style'] = '; '.join(updated_style_rules)
            else:
                del tag['style']

    return str(soup)


def convert_html_to_pdf_bytes(html_file):
    css_file = os.path.join(os.path.dirname(script_dir), 'generic_styler.css')
    # logging.info(f"Converting HTML file '{html_file}' to PDF bytes")

    with open(html_file, 'r', encoding='utf-8') as f:
        original_html_content = f.read()

    modified_html_content = remove_specific_inline_styles_within_tables(original_html_content)

    modified_html_content = add_meta_charset(modified_html_content)

    modified_html_file = f"{os.path.splitext(html_file)[0]}_modified.html"
    with open(modified_html_file, 'w', encoding='utf-8') as f:
        f.write(modified_html_content)
    # logging.info(f"Modified HTML content written to '{modified_html_file}'")

    # Inject CSS into modified HTML (for PDF conversion)
    if css_file and os.path.exists(css_file):
        with open(css_file, 'r', encoding='utf-8') as f:
            css_content = f.read()
        styled_html = f"<style>{css_content}</style>{modified_html_content}"
    else:
        styled_html = modified_html_content

    options = {
        'page-size': 'A4',
        'margin-top': '0.5in',
        'margin-right': '0.5in',
        'margin-bottom': '0.5in',
        'margin-left': '0.5in',
        'encoding': "UTF-8",
        'enable-local-file-access': '',
        'quiet': ''
    }

    try:
        pdf_data = pdfkit.from_string(styled_html, False, options=options, configuration=config_pdfkit)
        # logging.info(f"PDF bytes created successfully for '{html_file}'")
        return pdf_data
    except Exception as e:
        logging.error(f"Error converting HTML to PDF for '{html_file}': {e}")
        raise


# Function to add a UTF-8 meta charset to the HTML content if not present
def add_meta_charset(html_content):
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Create the UTF-8 meta tag
    meta_tag = soup.new_tag('meta')
    meta_tag.attrs['charset'] = 'UTF-8'

    # Find the <head> tag and insert the meta tag
    if soup.head:
        soup.head.insert(0, meta_tag)
    else:
        # Create a <head> tag if it doesn't exist and insert it at the beginning of the document
        head_tag = soup.new_tag('head')
        head_tag.insert(0, meta_tag)
        soup.insert(0, head_tag)

    return str(soup)


def convert_pdf_bytes_to_images(pdf_data, output_dir, report_name):
    try:
        # Convert PDF bytes to images (one per page)
        logging.info(f"Converting PDF bytes to images for report '{report_name}'")
        images = convert_from_bytes(pdf_data, dpi=250, fmt='png', thread_count=4)
        logging.info(f"Converted PDF to {len(images)} images")
    except Exception as e:
        logging.error(f"Error converting PDF to images for report '{report_name}': {e}")
        raise

    # Save images to the output directory
    os.makedirs(output_dir, exist_ok=True)
    for i, image in enumerate(images):
        output_path = os.path.join(output_dir, f"{report_name}_page_{i + 1}.png")
        image.save(output_path)
        # logging.info(f"Saved page {i + 1} image to '{output_path}'")


def process_document(html_file):
    try:
        logging.info(f"Processing document: '{html_file}'")

        report_name = os.path.basename(os.path.dirname(html_file))

        output_dir = os.path.join(os.path.dirname(html_file), 'tables')

        if os.path.exists(output_dir):
            logging.info(f"Output directory '{output_dir}' already exists. Skipping.")
            return

        pdf_data = convert_html_to_pdf_bytes(html_file)

        convert_pdf_bytes_to_images(pdf_data, output_dir, report_name)

    except Exception as e:
        with print_lock:
            logging.error(f"Error processing document: '{html_file}'")
            logging.error(str(e))


def worker_thread(queue):
    while True:
        html_file = queue.get()
        if html_file is None:
            break
        try:
            process_document(html_file)
        except Exception as e:
            with print_lock:
                logging.error(f"Unhandled exception in thread for file '{html_file}': {e}")
        finally:
            queue.task_done()


def main(symbol):
    logging.info("Script started.")
    logging.info("Starting table extraction process")
    filings_dir = os.path.join('sec-edgar-filings', symbol)

    # Collect all primary-document.html files
    html_files = []
    for root, _, files in os.walk(filings_dir):
        for file in files:
            if file == "primary-document.html":
                html_file = os.path.join(root, file)
                html_files.append(html_file)

    if not html_files:
        logging.info(f"No 'primary-document.html' files found for symbol '{symbol}'")
        return
    else:
        logging.info(f"Found {len(html_files)} 'primary-document.html' files to process.")

    # Set up threading
    num_threads = max(1, os.cpu_count() // 2)
    # logging.info(f"Using {num_threads} threads")

    queue = Queue()
    threads = []
    for _ in range(num_threads):
        t = Thread(target=worker_thread, args=(queue,))
        t.start()
        threads.append(t)

    # Enqueue HTML files
    for html_file in html_files:
        queue.put(html_file)

    # Block until all tasks are done
    queue.join()

    # Stop workers
    for _ in range(num_threads):
        queue.put(None)
    for t in threads:
        t.join()

    logging.info("Table extraction process completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process SEC Edgar filings for a specific stock symbol')
    parser.add_argument('symbol', type=str, help='Stock symbol to process')
    args = parser.parse_args()
    main(args.symbol)
