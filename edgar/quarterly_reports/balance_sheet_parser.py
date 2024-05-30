import os

from pdf2image import convert_from_path
from transformers import TableTransformerForObjectDetection, DetrFeatureExtractor
from PIL import Image, ImageDraw
import pytesseract
import pdfkit
import logging
import matplotlib.pyplot as plt
import torch
import easyocr
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)

config = pdfkit.configuration(wkhtmltopdf=r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe')
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'


def convert_html_to_pdf(html_file, pdf_file):
    logging.info(f"Converting HTML file '{html_file}' to PDF")
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
        'no-outline': None
    }
    pdfkit.from_file(html_file, pdf_file, options=options, configuration=config)
    logging.info(f"PDF file '{pdf_file}' created successfully")


def preprocess_pdf(pdf_file):
    logging.info(f"Preprocessing PDF file '{pdf_file}'")
    # Perform any necessary preprocessing on the PDF file
    # This can include tasks like removing noise, enhancing contrast, etc.
    # For simplicity, we'll skip this step in this example
    pass


def detect_tables(image, model, feature_extractor):
    logging.info("Detecting tables in the image")
    # Preprocess the image
    encoding = feature_extractor(image, return_tensors="pt")

    # Use the TableTransformer model to detect tables in the image
    with torch.no_grad():
        outputs = model(**encoding)

    # Process the model outputs to get the bounding boxes of the detected tables
    width, height = image.size
    results = feature_extractor.post_process_object_detection(outputs, threshold=0.7, target_sizes=[(height, width)])[0]

    logging.info(f"Detected {len(results['boxes'])} tables in the image")
    return results['boxes']


def visualize_table_regions(image, table_boxes, output_path):
    draw = ImageDraw.Draw(image)
    for box in table_boxes:
        # Convert bounding box coordinates to integers
        box = [round(coord.item()) for coord in box]

        # Draw rectangle on the image
        draw.rectangle(box, outline="red", width=1)

    # Save the image with visualized table regions
    image.save(output_path)
    logging.info(f"Saved image with visualized table regions: {output_path}")



def extract_table_data(image, table_boxes, page_num):
    logging.info("Extracting table data using OCR")
    table_data = []
    reader = easyocr.Reader(['en'])  # Initialize EasyOCR reader for English language

    for i, box in enumerate(table_boxes):
        # Convert bounding box coordinates to integers
        box = [round(coord.item()) for coord in box]

        # Extract coordinates from the bounding box
        x1, y1, x2, y2 = box

        # Validate bounding box coordinates
        width, height = image.size
        x1 = max(0, x1)
        y1 = max(0, y1)
        x2 = min(width, x2)
        y2 = min(height, y2)

        logging.info(f"Table bounding box: ({x1}, {y1}, {x2}, {y2})")
        logging.info(f"Image dimensions: ({width}, {height})")

        # Skip processing if the bounding box is invalid or too small
        if x2 <= x1 or y2 <= y1:
            logging.warning(f"Skipping table with invalid bounding box: ({x1}, {y1}, {x2}, {y2})")
            continue

        # Crop the image to the table region
        table_image = image.crop((x1, y1, x2, y2))

        # Save the cropped table image
        table_image_path = f"table_page{page_num}_table{i + 1}.png"
        try:
            table_image.save(table_image_path)
            logging.info(f"Saved cropped table image: {table_image_path}")
        except Exception as e:
            logging.error(f"Error saving cropped table image: {str(e)}")

        try:
            # Convert the PIL image to a numpy array
            table_image_np = np.array(table_image)

            # Apply OCR to extract the text from the table image using EasyOCR
            results = reader.readtext(table_image_np)
            table_text = '\n'.join([result[1] for result in results])
            table_data.append(table_text)
        except Exception as e:
            logging.error(f"Error during OCR: {str(e)}")
            continue

    logging.info(f"Extracted data from {len(table_data)} tables")
    return table_data


def main(html_file):
    logging.info("Starting table extraction process")

    # Convert HTML to PDF
    pdf_file = "temp.pdf"
    convert_html_to_pdf(html_file, pdf_file)

    # Preprocess the PDF
    # preprocess_pdf(pdf_file)

    # Convert PDF to images
    logging.info(f"Converting PDF file '{pdf_file}' to images")
    images = convert_from_path(pdf_file)
    logging.info(f"Converted PDF to {len(images)} images")

    logging.info("Loading TableTransformer model and feature extractor")
    model = TableTransformerForObjectDetection.from_pretrained("microsoft/table-transformer-detection")
    feature_extractor = DetrFeatureExtractor.from_pretrained("microsoft/table-transformer-detection")

    # Process each page image
    for i, image in enumerate(images):
        logging.info(f"Processing page {i + 1}/{len(images)}")

        # Resize the image
        width, height = image.size
        image = image.resize((int(width * 0.5), int(height * 0.5)))

        # Detect tables in the image
        table_boxes = detect_tables(image, model, feature_extractor)

        # Visualize table regions on the image
        output_path = f"page{i + 1}_tables.png"
        visualize_table_regions(image, table_boxes, output_path)

        # Extract table data using OCR
        table_data = extract_table_data(image, table_boxes, i + 1)

        # Process the extracted table data as needed
        for data in table_data:
            print(data)
            print("---")

    # Clean up temporary files
    logging.info(f"Removing temporary PDF file '{pdf_file}'")
    os.remove(pdf_file)

    logging.info("Table extraction process completed")


# Provide the path to your HTML file
file_path = "sec-edgar-filings/GOOG/10-Q/0001652044-18-000035/primary-document.html"

# Run the script
main(file_path)
