import argparse
import os
import sys
import uuid
from pdf2image import convert_from_path
from transformers import TableTransformerForObjectDetection, DetrFeatureExtractor
import pdfkit
import logging
import torch

# Configure logging
logging.getLogger().setLevel(logging.INFO)

config = pdfkit.configuration(
    wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe') # r'/usr/local/bin/wkhtmltopdf')


def convert_html_to_pdf(html_file, pdf_file, css_file=None, output_html_file=None):
    logging.info(f"Converting HTML file '{html_file}' to PDF")
    # css_file = r'/sec-edgar-filings/table_styler.css'
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
        'page-size': 'Letter',
        'margin-top': '0.75in',
        'margin-right': '0.75in',
        'margin-bottom': '0.75in',
        'margin-left': '0.75in',
        'encoding': "UTF-8",
        'custom-header': [
            ('Accept-Encoding', 'gzip')
        ],
        'cookie': [
            ('cookie-name1', 'cookie-value1'),
            ('cookie-name2', 'cookie-value2'),
        ],
        'no-outline': None,
        "enable-local-file-access": ""
    }

    # Convert the HTML to a PDF file
    pdfkit.from_string(styled_html, pdf_file, options=options, configuration=config)
    logging.info(f"PDF file '{pdf_file}' created successfully")
    return output_html_file



def preprocess_pdf(pdf_file):
    logging.info(f"Preprocessing PDF file '{pdf_file}'")
    # Perform any necessary preprocessing on the PDF file
    pass


def detect_tables(image, model, feature_extractor, padding=250):
    logging.info("Detecting tables in the image")
    # Preprocess the image
    encoding = feature_extractor(image, return_tensors="pt")

    # Use the TableTransformer model to detect tables in the image
    with torch.no_grad():
        outputs = model(**encoding)

    # Process the model outputs to get the bounding boxes of the detected tables
    width, height = image.size
    results = feature_extractor.post_process_object_detection(outputs, threshold=0.6, target_sizes=[(height, width)])[0]

    # Convert the tensor values to plain Python values
    boxes = [box.tolist() for box in results['boxes']]
    scores = results['scores'].tolist()

    # Add padding to the bounding boxes
    padded_boxes = []
    for box in boxes:
        x1, y1, x2, y2 = box
        x1 = max(0, x1 - padding)
        y1 = max(0, y1 - padding)
        x2 = min(width, x2 + padding)
        y2 = min(height, y2 + padding)
        padded_boxes.append([x1, y1, x2, y2])

    logging.info(f"Detected {len(padded_boxes)} tables in the image")
    logging.info(f"The scores of these tables are {scores}")

    return padded_boxes


def process_document(html_file):
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
    images = convert_from_path(pdf_file, dpi=300)
    logging.info(f"Converted PDF to {len(images)} images")

    logging.info("Loading TableTransformer model and feature extractor")
    model = TableTransformerForObjectDetection.from_pretrained("microsoft/table-transformer-detection")
    feature_extractor = DetrFeatureExtractor.from_pretrained("microsoft/table-transformer-detection")

    # Set the output directory to the report level
    output_dir = os.path.join(os.path.dirname(html_file), 'tables')

    # Create a directory to store table screenshots for the stock symbol
    os.makedirs(output_dir, exist_ok=True)

    # Process each page image
    for i, image in enumerate(images):
        if i > 10:
            break

        logging.info(f"Processing page {i + 1}/{len(images)}")

        # Detect tables in the image
        table_boxes = detect_tables(image, model, feature_extractor)

        # Visualize table regions on the image and save the screenshots
        for j, box in enumerate(table_boxes):
            table_image = image.crop(box)
            output_path = os.path.join(output_dir, f"{report_name}_table_page{i + 1}_table{j + 1}.png")
            table_image.save(output_path)
            logging.info(f"Saved table screenshot: {output_path}")

    # Clean up temporary files
    logging.info(f"Removing temporary PDF file '{pdf_file}'")
    os.remove(pdf_file)

    logging.info(f"Removing temporary HTML file '{output_html_file}'")
    os.remove(output_html_file)


def main(symbol):
    logging.info("Starting table extraction process")

    filings_dir = os.path.join("latest_quarterly_reports", 'sec-edgar-filings', symbol)

    # Traverse the symbol directory and find primary documents
    for root, dirs, files in os.walk(filings_dir):
        for file in files:
            if file == "primary-document.html":
                html_file = os.path.join(root, file)
                try:
                    process_document(html_file)
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
