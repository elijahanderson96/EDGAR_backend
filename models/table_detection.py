import argparse
import os
import sys
import uuid
from pdf2image import convert_from_path
import pdfkit
import logging
import torch

# Configure logging
logging.getLogger().setLevel(logging.INFO)
script_dir = os.path.dirname(os.path.abspath(__file__))

config = pdfkit.configuration(
    wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')  # r'/usr/local/bin/wkhtmltopdf')


def convert_html_to_pdf(html_file, pdf_file):
    css_file = os.path.join(os.path.dirname(script_dir), 'generic_styler.css')
    logging.info(f"Converting HTML file '{html_file}' to PDF")
    output_html_file = rf'C:\Users\Elijah\PycharmProjects\edgar_backend\{uuid.uuid4().hex}.html'

    # Read the HTML file
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # If a CSS file is provided, inject it into the HTML
    if css_file:
        with open(css_file, 'r', encoding='utf-8') as f:
            css_content = f.read()
        styled_html = f"<style>{css_content}</style>{html_content}"
    else:
        styled_html = html_content

    # If an output HTML file path is provided, write the styled HTML to that file
    if output_html_file:
        with open(output_html_file, 'w', encoding='utf-8') as f:
            f.write(styled_html)
        logging.info(f"Styled HTML file '{output_html_file}' created successfully")

    options = {
        'page-size': 'A4',
        'margin-top': '0.5in',
        'margin-right': '0.5in',
        'margin-bottom': '0.5in',
        'margin-left': '0.5in',
        'encoding': "UTF-8",
        'no-outline': None,
        'enable-local-file-access': '',
        'zoom': '1.0',  # Adjust the zoom level if needed
        'disable-smart-shrinking': '',  # Prevents content from shrinking too much
        'custom-header': [
            ('Accept-Encoding', 'gzip')
        ],
    }

    # Convert the HTML to a PDF file
    pdfkit.from_string(styled_html, pdf_file, options=options, configuration=config)
    logging.info(f"PDF file '{pdf_file}' created successfully")
    return output_html_file


def process_document(html_file, output_dir):
    logging.info(f"Processing document: {html_file}")

    # Extract the stock symbol from the path
    parts = html_file.split(os.sep)

    # Extract the report name from the path
    report_name = parts[4]

    # Generate a unique name for the PDF file
    pdf_file = f"temp_{uuid.uuid4().hex}.pdf"
    output_html_file = convert_html_to_pdf(html_file, pdf_file)

    # Convert PDF to images
    logging.info(f"Converting PDF file '{pdf_file}' to images")
    images = convert_from_path(pdf_file, dpi=400)
    logging.info(f"Converted PDF to {len(images)} images")

    logging.info("Loading TableTransformer model and feature extractor")

    # Create a directory to store table screenshots for the stock symbol
    os.makedirs(output_dir, exist_ok=True)

    # Process each page image
    for i, image in enumerate(images):
        output_path = os.path.join(output_dir, f"{report_name}_table_page{i + 1}.png")
        image.save(output_path)
        if i > 12:
            break

        logging.info(f"Processing page {i + 1}/{len(images)}")

    # Clean up temporary files
    logging.info(f"Removing temporary PDF file '{pdf_file}'")
    os.remove(pdf_file)

    logging.info(f"Removing temporary HTML file '{output_html_file}'")
    os.remove(output_html_file)


def main(symbol):
    logging.info("Starting table extraction process")

    filings_dir = os.path.join('sec-edgar-filings', symbol)

    # Traverse the symbol directory and find primary documents
    for root, dirs, files in os.walk(filings_dir):
        for file in files:
            if file == "primary-document.html":
                html_file = os.path.join(root, file)
                try:
                    # Set the output directory to the report level
                    output_dir = os.path.join(os.path.dirname(html_file), 'tables')

                    if os.path.exists(output_dir):
                        return

                    process_document(html_file, output_dir)
                except Exception as e:
                    logging.error(f"Error processing document: {html_file}")
                    logging.error(str(e))
                    raise e

    logging.info("Table extraction process completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process SEC Edgar filings for a specific stock symbol')
    parser.add_argument('symbol', type=str, help='Stock symbol to process')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    main(args.symbol)
