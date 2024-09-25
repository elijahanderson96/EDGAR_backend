import os

import torch
from ultralytics import YOLO
import cv2
import logging

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Loading pre-trained YOLOv8 model
    logging.info("Loading pre-trained YOLOv8 model...")
    # model = YOLO("yolov8n-cls.pt")  # Load a pretrained classification model
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = YOLO("yolov8n-cls.pt")
    model.to(device)
    # Hyperparameters
    hyperparameters = {
        'batch': .5,
        'imgsz': 640,  # Image size
        'epochs': 300,  # Number of epochs
        # 'lr0': 0.001,        # Initial learning rate
        'patience': 25,  # Patience for early stopping

        # 'pretrained': False  # Ensure no pre-trained weights are used
    }

    # Starting training
    logging.info("Starting training...")
    model.train(data=r'C:\Users\Elijah\PycharmProjects\edgar_backend\yolo_classifier', **hyperparameters)
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
#     r'C:\Users\Elijah\PycharmProjects\edgar_backend\tables\CSCO\0000858877-13-000013_table_page3_table1.png',
#     model,
#     r'C:\Users\Elijah\PycharmProjects\edgar_backend/test.png'
# )
