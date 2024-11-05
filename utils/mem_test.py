import torch
from ultralytics import YOLO

# Load the model
model_path = r'/runs/detect/train5/weights/best.pt'
model = YOLO(model_path)

# Check the model size
model_size = sum(p.numel() for p in model.parameters() if p.requires_grad)
print(f"Model size (parameters): {model_size}")

# Perform inference on a sample image
image = torch.randn(1, 3, 640, 640)  # Example input image
with torch.no_grad():
    outputs = model(image)

# Check memory usage
memory_allocated = torch.cuda.memory_allocated()
memory_reserved = torch.cuda.memory_reserved()
print(f"Memory allocated: {memory_allocated} bytes")
print(f"Memory reserved: {memory_reserved} bytes")
