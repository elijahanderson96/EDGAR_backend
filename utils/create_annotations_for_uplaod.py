import os

# Directories for images and labels
images_train_dir = os.path.join("yolo_dataset", "images", "train")
labels_train_dir = os.path.join("yolo_dataset", "labels", "train")

# Function to find unmatched files
def find_unmatched_files(images_dir, labels_dir):
    image_files = set([os.path.splitext(f)[0] for f in os.listdir(images_dir) if f.endswith('.png')])
    label_files = set([os.path.splitext(f)[0] for f in os.listdir(labels_dir) if f.endswith('.txt')])

    # Images without labels
    images_without_labels = image_files - label_files
    # Labels without images
    labels_without_images = label_files - image_files

    return images_without_labels, labels_without_images

# Check for unmatched files in training data
train_images_without_labels, train_labels_without_images = find_unmatched_files(images_train_dir, labels_train_dir)

# Display results
if train_images_without_labels:
    print("Images without corresponding labels:", train_images_without_labels)
else:
    print("All images have corresponding labels.")

if train_labels_without_images:
    print("Labels without corresponding images:", train_labels_without_images)
else:
    print("All labels have corresponding images.")
