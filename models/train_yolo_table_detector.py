import os
from ultralytics import YOLO
import cv2
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

output_dir = r"C:\Users\Elijah\PycharmProjects\edgar_backend\yolo_table_detection"

# Initialize YOLOv8 model from scratch
logging.info("Initializing YOLOv8 model from scratch...")
model = YOLO()

# Specify hyperparameters for training, including modified learning rate and batch size
hyperparameters = {
    'batch': .7,          # Increased batch size
    'imgsz': 640,        # Image size
    'epochs': 300,        # Number of epochs
    'lr0': 0.03,          # Increased initial learning rate
    'lrf': 0.01,         # Final learning rate (lowered for stability)
    'patience': 25,       # Patience for early stopping
}

logging.info("Starting training with adjusted learning rate and batch size...")
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
