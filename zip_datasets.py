import zipfile
import os

def zip_directory(directory_path, output_filename):
    with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                zip_file.write(file_path, os.path.relpath(file_path, os.path.dirname(directory_path)))
    print(f'{output_filename} created successfully!')

# Define directories and output zip file names
directories_to_zip = {
    'yolo_dataset': 'yolo_dataset.zip',
    'yolo_table_detection': 'yolo_table_detection.zip'
}

# Create separate zip files for each directory
for directory, output_zip in directories_to_zip.items():
    zip_directory(directory, output_zip)
