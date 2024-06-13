import os
from ultralytics import YOLO
import cv2
from sklearn.model_selection import train_test_split
import shutil
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Function to prepare the YOLO dataset
def prepare_yolo_dataset(base_dir, output_dir):
    # Create the main directories for images and labels
    for split in ["train", "val", "test"]:
        os.makedirs(os.path.join(output_dir, "images", split), exist_ok=True)
        os.makedirs(os.path.join(output_dir, "labels", split), exist_ok=True)

    logging.info(f"Created main directories for images and labels in {output_dir}")

    # Aggregate all images and labels
    all_images = []
    all_labels = []

    # Get all symbols (directories) in the base directory
    symbols = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    logging.info(f"Found symbols: {symbols}")

    for symbol in symbols:
        symbol_dir = os.path.join(base_dir, symbol, '10-Q')
        for report in os.listdir(symbol_dir):
            images_dir = os.path.join(symbol_dir, report, "tables")
            labels_dir = os.path.join(symbol_dir, report, "obj_train_data")

            if not os.path.exists(images_dir) or not os.path.exists(labels_dir):
                logging.warning(f"Skipping report {report} for symbol {symbol} as required directories are missing.")
                continue

            images = [f for f in os.listdir(images_dir) if f.endswith(('.jpg', '.jpeg', '.png'))]
            logging.info(f"Found {len(images)} images for symbol {symbol}, report {report}")

            for image in images:
                all_images.append((symbol, report, image))
                label_file = os.path.splitext(image)[0] + ".txt"
                if os.path.exists(os.path.join(labels_dir, label_file)):
                    all_labels.append((symbol, report, label_file))
                else:
                    logging.warning(f"Label file {label_file} does not exist for image {image}")

    # Split aggregated data into train, val, test
    train_images, test_images = train_test_split(all_images, test_size=0.1, random_state=42)
    val_images, test_images = train_test_split(test_images, test_size=0.5, random_state=42)

    splits = {
        "train": train_images,
        "val": val_images,
        "test": test_images
    }

    # Move files to the corresponding directories
    for split, image_list in splits.items():
        for symbol, report, image in image_list:
            images_dir = os.path.join(base_dir, symbol, '10-Q', report, "tables")
            labels_dir = os.path.join(base_dir, symbol, '10-Q', report, "obj_train_data")

            # Move image
            src_img_path = os.path.join(images_dir, image)
            dst_img_path = os.path.join(output_dir, "images", split, symbol + '_' + image)
            shutil.copy(src_img_path, dst_img_path)
            logging.info(f"Copied image {src_img_path} to {dst_img_path}")

            # Move corresponding label file
            label_file = os.path.splitext(image)[0] + ".txt"
            src_label_path = os.path.join(labels_dir, label_file)
            dst_label_path = os.path.join(output_dir, "labels", split, symbol + '_' + label_file)
            if os.path.exists(src_label_path):
                shutil.copy(src_label_path, dst_label_path)
                logging.info(f"Copied label {src_label_path} to {dst_label_path}")
            else:
                logging.warning(f"Label file {src_label_path} does not exist")


# Set up paths
base_dir = r"C:\Users\Elijah\PycharmProjects\edgar_backend\sec-edgar-filings"
output_dir = r"C:\Users\Elijah\PycharmProjects\edgar_backend\yolo_dataset"
prepare_yolo_dataset(base_dir, output_dir)

# Create data.yaml file
data_yaml_content = f"""
train: {output_dir}/images/train
val: {output_dir}/images/val
test: {output_dir}/images/test

nc: 4
names: ["Data Table", "Period", "Unit", "Date"]
"""

with open(os.path.join(output_dir, "data.yaml"), 'w') as f:
    f.write(data_yaml_content)

logging.info("Dataset preparation complete.")
logging.info(f"data.yaml content:\n{data_yaml_content}")

# Training YOLOv8

logging.info("Loading pre-trained YOLOv8 model...")
model = YOLO('yolov8n.pt')

logging.info("Starting training...")
model.train(data=os.path.join(output_dir, "data.yaml"), epochs=100)
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
    r"C:\Users\Elijah\PycharmProjects\edgar_backend\tables\COST\0000909832-13-000011_table_page3_table1.png",
    model,
    r"C:\Users\Elijah\PycharmProjects\edgar_backend\tables\COST\0000909832-13-000011_table_page3_table1_labeled.png"
)
