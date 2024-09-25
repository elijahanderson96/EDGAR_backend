import os
from ultralytics import YOLO
import cv2
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

output_dir = r"C:\Users\Elijah\PycharmProjects\edgar_backend\yolo_dataset"

# Initialize YOLOv8 model from scratch
logging.info("Initializing YOLOv8 model from scratch...")
model = YOLO()

# Specify hyperparameters for training, including modified learning rate and batch size
hyperparameters = {
    'batch': 8,  # Batch size
    'imgsz': 1280,  # Image size
    'epochs': 250,  # Number of epochs
    'patience': 25,  # Early stopping patience
}

logging.info("Starting training...")
model.train(data=os.path.join(output_dir, "data.yaml"), **hyperparameters)
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
# perform_inference(
#     r"C:\Users\Elijah\PycharmProjects\edgar_backend\yolo_classifier\train\balance_sheet\0001744489-21-000181_table_page4_table1.png",
#     model,
#     r"C:\Users\Elijah\PycharmProjects\edgar_backend\test.png"
# )
