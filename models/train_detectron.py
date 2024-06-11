import os
import random
from detectron2.engine import DefaultTrainer, DefaultPredictor
from detectron2.config import get_cfg
from detectron2.data import MetadataCatalog
from detectron2 import model_zoo
from detectron2.data.datasets import register_coco_instances
from detectron2.utils.visualizer import Visualizer
import cv2

# Define the correct thing_classes
thing_classes = [
    "Date", "Period", "Data Table", "Unit"]

# Function to register datasets from multiple directories
def register_datasets(base_dir):
    valid_datasets = []
    for root, dirs, files in os.walk(base_dir):
        for dir_name in dirs:
            if dir_name in ['tables', 'augmented_tables']:
                dataset_name = f"{root.replace(os.sep, '_')}_dataset"
                json_file = os.path.join(root, 'annotations', 'instances_default.json')
                image_root = os.path.join(root, dir_name)
                if os.path.exists(json_file):
                    register_coco_instances(dataset_name, {}, json_file, image_root)
                    valid_datasets.append(dataset_name)
                if dir_name == 'augmented_tables':
                    augmented_dataset_name = f"{root.replace(os.sep, '_')}_augmented_dataset"
                    augmented_json_file = os.path.join(root, 'augmented_tables', 'augmented_instances_default.json')
                    if os.path.exists(augmented_json_file):
                        register_coco_instances(augmented_dataset_name, {}, augmented_json_file, image_root)
                        valid_datasets.append(augmented_dataset_name)
    return valid_datasets

# Register datasets
base_dir = r"C:\Users\Elijah\PycharmProjects\edgar_backend\sec-edgar-filings"
valid_datasets = register_datasets(base_dir)

# Reserve a subset of datasets for validation
random.shuffle(valid_datasets)
validation_split = 0.1
split_idx = int(len(valid_datasets) * validation_split)
validation_datasets = valid_datasets[:split_idx]
training_datasets = valid_datasets[split_idx:]

# Verify registered datasets
print("Training Datasets:")
for dataset in training_datasets:
    print(dataset)

print("Validation Datasets:")
for dataset in validation_datasets:
    print(dataset)

# Configure the model
cfg = get_cfg()
cfg.merge_from_file(model_zoo.get_config_file("COCO-Detection/faster_rcnn_R_50_FPN_3x.yaml"))
cfg.DATASETS.TRAIN = tuple(training_datasets)  # Use training datasets
cfg.DATASETS.TEST = tuple(validation_datasets)  # Use validation datasets
cfg.DATALOADER.NUM_WORKERS = 2
cfg.MODEL.WEIGHTS = ''  # Use a pre-trained model

cfg.SOLVER.IMS_PER_BATCH = 4
cfg.SOLVER.BASE_LR = 0.00001
cfg.SOLVER.MAX_ITER = 2500  # Increase the number of iterations
cfg.MODEL.ROI_HEADS.BATCH_SIZE_PER_IMAGE = 256
cfg.MODEL.ROI_HEADS.NUM_CLASSES = len(thing_classes)  # Number of classes

# Train the model
trainer = DefaultTrainer(cfg)
trainer.resume_or_load(resume=False)
trainer.train()

# Perform inference on a new image
def perform_inference(image_path, cfg, output_path):
    predictor = DefaultPredictor(cfg)
    im = cv2.imread(image_path)
    outputs = predictor(im)

    # Print classes and class_names for debugging
    classes = outputs["instances"].pred_classes.to("cpu")
    print(f"Predicted classes: {classes}")
    print(f"Class names: {thing_classes}")

    v = Visualizer(im[:, :, ::-1], metadata=MetadataCatalog.get(cfg.DATASETS.TRAIN[0]), scale=1.2)
    out = v.draw_instance_predictions(outputs["instances"].to("cpu"))

    # Save the output image
    result_image = out.get_image()[:, :, ::-1]
    cv2.imwrite(output_path, result_image)
    print(f"Result saved to {output_path}")

# Example usage for inference
perform_inference(
    r"C:\Users\Elijah\PycharmProjects\edgar_backend\sec-edgar-filings\AAPL\10-Q\0000320193-20-000010\tables\table_page4_table1.png",
    cfg,
    r"C:\Users\Elijah\PycharmProjects\edgar_backend\sec-edgar-filings\AAPL\10-Q\0000320193-20-000010\tables\output.png"
)
