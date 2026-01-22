from ultralytics import YOLO
from pathlib import Path
import csv

# -----------------------------
# Configuration
# -----------------------------
IMAGE_ROOT = Path("data/raw/images")
OUTPUT_DIR = Path("data/yolo")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "detections.csv"

# Limit images for learning/debugging
MAX_IMAGES = 30  # change to None later to process all

# YOLO model (lightweight)
model = YOLO("yolov8n.pt")

# Objects we treat as "products"
PRODUCT_OBJECTS = {
    "bottle",
    "cup",
    "box",
    "jar",
    "tube",
    "container"
}

# -----------------------------
# Helper: classify image
# -----------------------------
def classify_image(detected_objects: set) -> str:
    has_person = "person" in detected_objects
    has_product = any(obj in PRODUCT_OBJECTS for obj in detected_objects)

    if has_person and has_product:
        return "promotional"
    if has_product:
        return "product_display"
    if has_person:
        return "lifestyle"
    return "other"

# -----------------------------
# Main detection loop
# -----------------------------
rows = []
processed = 0

for channel_dir in IMAGE_ROOT.iterdir():
    if not channel_dir.is_dir():
        continue

    channel_name = channel_dir.name

    for image_path in channel_dir.glob("*.jpg"):
        message_id = image_path.stem  # filename = message_id

        results = model(image_path, verbose=False)

        detected_objects = set()

        for r in results:
            for box in r.boxes:
                class_id = int(box.cls[0])
                object_name = model.names[class_id]
                confidence = float(box.conf[0])

                detected_objects.add(object_name)

                rows.append({
                    "message_id": message_id,
                    "channel_name": channel_name,
                    "object_name": object_name,
                    "confidence": round(confidence, 3)
                })

        image_category = classify_image(detected_objects)

        # add category row
        rows.append({
            "message_id": message_id,
            "channel_name": channel_name,
            "object_name": "image_category",
            "confidence": None
        })

        processed += 1
        if MAX_IMAGES and processed >= MAX_IMAGES:
            break

    if MAX_IMAGES and processed >= MAX_IMAGES:
        break

# -----------------------------
# Write CSV
# -----------------------------
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=["message_id", "channel_name", "object_name", "confidence"]
    )
    writer.writeheader()
    writer.writerows(rows)

print(f"YOLO detection completed. Rows written: {len(rows)}")
print(f"Output file: {OUTPUT_FILE}")
