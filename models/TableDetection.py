import argparse
import os
from ultralytics import YOLO
from PIL import Image, ImageOps
import sys
sys.path.append("..")


class FinancialTableDetector:
    def __init__(self, model_path='best.pt', symbol_dir=None, padding_color=(255, 255, 255)):
        """
        Initializes the FinancialTableDetector with the specified model and symbol directory.

        :param model_path: Path to the YOLOv8 model weights file.
        :param symbol_dir: Directory containing the symbol folder with the 10-Q filings.
        :param padding_color: Color to use for padding. Default is white (255, 255, 255).
        """
        self.model = YOLO(model_path, verbose=False)
        self.symbol_dir = symbol_dir
        self.padding_color = padding_color

    def add_padding(self, img, target_size):
        """
        Adds padding (white space) around the image to match the target size while keeping the original aspect ratio.

        :param img: The cropped image.
        :param target_size: The target size (width, height) to pad the image to.
        :return: The padded image.
        """
        target_width, target_height = target_size
        img_width, img_height = img.size

        # Calculate padding
        delta_w = target_width - img_width
        delta_h = target_height - img_height
        padding = (delta_w // 2, delta_h // 2, delta_w - (delta_w // 2), delta_h - (delta_h // 2))

        # Add padding with the specified color (white by default)
        return ImageOps.expand(img, padding, fill=self.padding_color)

    def detect_and_crop_tables(self):
        """
        Detects tables in all images within the tables subdirectories of the symbol directory,
        crops them with 10% padding, optionally resizes them or adds padding, and saves the
        processed images under the label name in the same directory only if the confidence
        score is 0.95 or higher. Files not moved to label directories will be deleted.
        """
        # Supported image file extensions
        supported_extensions = ('.png', '.jpg', '.jpeg', '.bmp', '.tiff')

        # Walk through all subdirectories under the symbol directory
        for root, dirs, files in os.walk(self.symbol_dir):
            if 'tables' in root:  # Only process directories that contain 'tables'
                moved_files = set()  # Track files that are successfully processed and moved

                for filename in files:
                    if filename.lower().endswith(supported_extensions):
                        image_path = os.path.join(root, filename)

                        # Perform inference with the model
                        results = self.model(image_path, verbose=False)

                        # Open the original image
                        with Image.open(image_path) as img:
                            img_width, img_height = img.size  # Get the dimensions of the image

                            # Process each detection result
                            for result in results:
                                # Check if any boxes (detections) are present
                                if result.boxes:
                                    for idx, box in enumerate(result.boxes):
                                        # Extract bounding box coordinates and convert to int
                                        x1, y1, x2, y2 = box.xyxy[0]  # These are tensors
                                        x1 = int(x1.item())
                                        y1 = int(y1.item())
                                        x2 = int(x2.item())
                                        y2 = int(y2.item())

                                        # Get the confidence score for the detection
                                        confidence_score = box.conf.item()  # Confidence score as a float
                                        print(f"Detection {idx + 1}: {confidence_score:.2f} confidence")

                                        # Only process and save images with a confidence score of 0.95 or higher
                                        if confidence_score >= 0.95:
                                            # Calculate 10% padding
                                            width_padding = int(0.10 * (x2 - x1))  # 10% of the width
                                            height_padding = int(0.10 * (y2 - y1))  # 10% of the height

                                            # Apply padding and ensure coordinates are within image bounds
                                            x1 = max(0, x1 - width_padding)
                                            y1 = max(0, y1 - height_padding)
                                            x2 = min(img_width, x2 + width_padding)
                                            y2 = min(img_height, y2 + height_padding)

                                            # Crop the detected table from the image
                                            cropped_img = img.crop((x1, y1, x2, y2))

                                            cropped_img = self.add_padding(cropped_img, (img_width, img_height))

                                            # Save the cropped image under the label name
                                            label_name = result.names[int(box.cls[0])]
                                            save_dir = os.path.join(root, label_name.replace(" ", "_").lower())
                                            os.makedirs(save_dir, exist_ok=True)

                                            # Construct the save path
                                            base_filename = os.path.splitext(filename)[0]
                                            save_path = os.path.join(save_dir, f'{base_filename}_{idx}.png')

                                            # Save the cropped image
                                            cropped_img.save(save_path, optimize=True)

                                            # Add the successfully saved file path to the moved_files set
                                            moved_files.add(image_path)

                                        else:
                                            print(f"Skipping detection {idx + 1} due to low confidence ({confidence_score:.2f})")

                # After processing, delete any files in the root directory that were not moved
                for filename in files:
                    file_path = os.path.join(root, filename)
                    if file_path not in moved_files:
                        # print(f"Deleting unprocessed file: {file_path}")
                        os.remove(file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Table Detection for a given SYMBOL')
    parser.add_argument('symbol', type=str, help='The symbol for which to detect tables')

    args = parser.parse_args()

    # Generate the symbol directory path dynamically based on the SYMBOL argument
    symbol_dir = os.path.join(os.getcwd(), 'sec-edgar-filings', args.symbol)

    print(symbol_dir)
    model_path = r'C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train20\weights\best.pt'

    detector = FinancialTableDetector(
        model_path=model_path,
        symbol_dir=symbol_dir,
    )

    detector.detect_and_crop_tables()
