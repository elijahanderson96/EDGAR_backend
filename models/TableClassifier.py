import os
from ultralytics import YOLO
import cv2
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths
data_yaml = '/Users/elijahanderson/PycharmProjects/EDGAR_backend/yolo_classifier/data.yaml'

# Loading pre-trained YOLOv8 model
logging.info("Loading pre-trained YOLOv8 model...")
model = YOLO("yolov8n-cls.pt")  # Load a pretrained classification model

# Starting training
logging.info("Starting training...")
model.train(data='/Users/elijahanderson/PycharmProjects/EDGAR_backend/yolo_classifier', epochs=75)
logging.info("Training complete.")

# Perform inference on a new image
def perform_inference(image_path, model, output_path):
    logging.info(f"Performing inference on image {image_path}")
    img = cv2.imread(image_path)
    results = model(img)
    for i, result in enumerate(results):
        result_img = result.plot()
        result_output_path = os.path.splitext(output_path)[0] + f"_{i}.jpg"
        cv2.imwrite(result_output_path, result_img)
        logging.info(f"Result saved to {result_output_path}")

# Example usage for inference
perform_inference(
    '/Users/elijahanderson/PycharmProjects/EDGAR_backend/tables/AMD/0000002488-15-000067_table_page5_table1.png',
    model,
    '/Users/elijahanderson/PycharmProjects/EDGAR_backend/test.png'
)
