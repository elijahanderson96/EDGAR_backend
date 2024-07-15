import albumentations as A
import cv2
import os
import json
from glob import glob

# Define the augmentation pipeline with specific transformations
transform = A.Compose([
    A.RandomBrightnessContrast(p=0.3),
    A.RGBShift(r_shift_limit=30, g_shift_limit=30, b_shift_limit=30, p=1),
], bbox_params=A.BboxParams(format='coco', label_fields=['category_ids']))


# Helper function to load JSON annotations


def load_json(json_file):
    with open(json_file) as f:
        return json.load(f)


# Helper function to visualize bounding boxes
def visualize_bboxes(image, bboxes, category_ids, category_names, output_path):
    for bbox, category_id in zip(bboxes, category_ids):
        x, y, w, h = map(int, bbox)
        cv2.rectangle(image, (x, y), (x + w, y + h), (0, 255, 0), 2)
        cv2.putText(image, category_names[category_id], (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
    cv2.imwrite(output_path, image)


# Load the dataset and apply augmentation
def augment_dataset(base_dir):
    datasets = glob(os.path.join(base_dir, '*'))
    for dataset in datasets:
        tables_dir = os.path.join(dataset, 'tables')
        annotation_file = os.path.join(dataset, 'instances_default.json')
        if not os.path.exists(tables_dir) or not os.path.exists(annotation_file):
            continue

        # Load annotations
        annotations = load_json(annotation_file)
        images = annotations['images']
        categories = annotations['categories']
        category_names = {cat['id']: cat['name'] for cat in categories}
        annotations_list = annotations['annotations']

        augmented_dir = os.path.join(dataset, 'augmented_tables')
        os.makedirs(augmented_dir, exist_ok=True)

        augmented_images = []
        augmented_annotations = []

        # Load images
        for img in images:
            image_path = os.path.join(tables_dir, img['file_name'])
            if 'augmented_' in img['file_name']:
                continue  # Skip already augmented images

            image = cv2.imread(image_path)
            image_height, image_width = image.shape[:2]

            # Extract corresponding annotations
            bboxes = []
            category_ids = []
            for ann in annotations_list:
                if ann['image_id'] == img['id']:
                    bbox = ann['bbox']
                    bboxes.append(bbox)
                    category_ids.append(ann['category_id'])

            # Apply augmentation
            transformed = transform(image=image, bboxes=bboxes, category_ids=category_ids)
            transformed_image = transformed['image']
            transformed_bboxes = transformed['bboxes']
            transformed_category_ids = transformed['category_ids']

            # Save augmented image without bounding boxes in the augmented_tables directory
            augmented_image_path = os.path.join(augmented_dir, 'augmented_' + img['file_name'])
            cv2.imwrite(augmented_image_path, transformed_image)

            # Save augmented image with bounding boxes in the parent directory for QA
            qa_image_path = os.path.join(dataset, 'augmented_' + img['file_name'])
            visualize_bboxes(transformed_image.copy(), transformed_bboxes, transformed_category_ids, category_names,
                             qa_image_path)

            # Update JSON annotations for augmented image
            new_img_id = len(augmented_images) + 1
            new_img_entry = {
                "id": new_img_id,
                "width": transformed_image.shape[1],
                "height": transformed_image.shape[0],
                "file_name": 'augmented_' + img['file_name']
            }
            augmented_images.append(new_img_entry)

            for i, bbox in enumerate(transformed_bboxes):
                x_min, y_min, width, height = bbox
                if transformed_image.shape[0] != image_height or transformed_image.shape[1] != image_width:
                    scale_x = transformed_image.shape[1] / image_width
                    scale_y = transformed_image.shape[0] / image_height
                    x_min *= scale_x
                    y_min *= scale_y
                    width *= scale_x
                    height *= scale_y
                new_ann_entry = {
                    "id": len(augmented_annotations) + 1,
                    "image_id": new_img_id,
                    "category_id": transformed_category_ids[i],
                    "bbox": [x_min, y_min, width, height],
                    "area": width * height,
                    "iscrowd": 0
                }
                augmented_annotations.append(new_ann_entry)

        # Save the updated annotations
        new_annotation_file = os.path.join(augmented_dir, 'augmented_instances_default.json')
        with open(new_annotation_file, 'w') as f:
            json.dump({"images": augmented_images, "annotations": augmented_annotations, "categories": categories}, f)


# Path to the dataset directory
base_dir = r"C:\Users\Elijah\PycharmProjects\edgar_backend\sec-edgar-filings\AAPL\10-Q"
augment_dataset(base_dir)
