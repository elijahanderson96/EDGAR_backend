import os
from typing import List
import numpy as np
import PIL.Image
from cvat_sdk import make_client
import cvat_sdk.models as models
import cvat_sdk.auto_annotation as cvataa
from ultralytics import YOLO
import cv2
from config.configs import AA_PASSWORD, AA_USERNAME


class YOLOV8DetectionFunction:
    def __init__(self, model_path: str, labels: List[str], confidence_threshold: float = .621) -> None:
        # Load the YOLOv8 model
        self._model = YOLO(model_path)
        self._labels = labels
        self._confidence_threshold = confidence_threshold

    @property
    def spec(self) -> cvataa.DetectionFunctionSpec:
        # Describe the annotations
        return cvataa.DetectionFunctionSpec(
            labels=[
                cvataa.label_spec(cat, i)
                for i, cat in enumerate(self._labels)
            ]
        )

    def letterbox(self, image, new_shape=(1280, 1280), color=(114, 114, 114), auto=True, scaleFill=False, scaleup=True):
        # Resize image to a 32-pixel-multiple rectangle
        shape = image.shape[:2]  # current shape [height, width]
        if isinstance(new_shape, int):
            new_shape = (new_shape, new_shape)

        # Scale ratio (new / old) and new unpadded shape
        r = min(new_shape[0] / shape[0], new_shape[1] / shape[1])
        if not scaleup:  # only scale down, do not scale up (for better val mAP)
            r = min(r, 1.0)

        ratio = r, r  # width, height ratios
        new_unpad = int(round(shape[1] * r)), int(round(shape[0] * r))

        # Compute padding
        dw, dh = new_shape[1] - new_unpad[0], new_shape[0] - new_unpad[1]  # wh padding
        if auto:  # minimum rectangle
            dw, dh = np.mod(dw, 32), np.mod(dh, 32)  # wh padding
        elif scaleFill:  # stretch
            dw, dh = 0.0, 0.0
            new_unpad = new_shape
            ratio = new_shape[1] / shape[1], new_shape[0] / shape[0]  # width, height ratios

        dw /= 2  # divide padding into 2 sides
        dh /= 2

        if shape[::-1] != new_unpad:  # resize
            image = cv2.resize(image, new_unpad, interpolation=cv2.INTER_LINEAR)
        top, bottom = int(round(dh - 0.1)), int(round(dh + 0.1))
        left, right = int(round(dw - 0.1)), int(round(dw + 0.1))
        image = cv2.copyMakeBorder(image, top, bottom, left, right, cv2.BORDER_CONSTANT, value=color)  # add border
        return image, ratio, (dw, dh)

    def detect(self, context, image: PIL.Image.Image) -> List[models.LabeledShapeRequest]:
        # Convert PIL image to OpenCV format
        cv_image = np.array(image)
        cv_image = cv_image[:, :, ::-1].copy()  # Convert RGB to BGR

        # Letterbox the image to maintain aspect ratio
        padded_image, ratio, (dw, dh) = self.letterbox(cv_image)

        # Run the ML model
        results = self._model(padded_image)[0]

        # Convert the results into a form CVAT can understand
        shapes = []
        for box, score, label in zip(results.boxes.xyxy, results.boxes.conf, results.boxes.cls):
            if score >= self._confidence_threshold:
                box = box.cpu().numpy()
                label_id = int(label)
                shapes.append(cvataa.rectangle(
                    label_id=label_id,
                    points=[
                        float((box[0] - dw) / ratio[0]),  # x1
                        float((box[1] - dh) / ratio[1]),  # y1
                        float((box[2] - dw) / ratio[0]),  # x2
                        float((box[3] - dh) / ratio[1]),  # y2
                    ]
                ))
        return shapes


model_path = r"C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train41\weights\best.pt" # extract
# model_path = r'C:\Users\Elijah\PycharmProjects\edgar_backend\runs\detect\train16\weights\best.pt'
# Log into the CVAT server
# with make_client(host="http://localhost:8080",
#                  credentials=(AA_USERNAME, AA_PASSWORD)) as client:
#     # Annotate task 12345 using the custom model
#     cvataa.annotate_task(client, 65,
#                          YOLOV8DetectionFunction(model_path=model_path,
#                                                  labels=["Unit", "Data", "Column Title", "Column Group Title"]),
#                          )

print(AA_USERNAME, AA_PASSWORD)

# Log into the CVAT server
with make_client(host="https://app.cvat.ai",
                 credentials=(AA_USERNAME, AA_PASSWORD)) as client:
    # Annotate task 12345 using the custom model
    cvataa.annotate_task(client, 915973,
                         YOLOV8DetectionFunction(model_path=model_path,
                                                 labels=["Unit", "Data", "Column Title", "Column Group Title"]),
                         )


# # Log into the CVAT server
# with make_client(host="https://app.cvat.ai",
#                  credentials=(AA_USERNAME, AA_PASSWORD)) as client:
#     # Annotate task 12345 using the custom model
#     cvataa.annotate_task(client, 903806,
#                          YOLOV8DetectionFunction(model_path=model_path,
#                                                  labels=["Income", "Cash Flow", "Balance Sheet"]),
#                          )
