import os
from PIL import Image
from collections import defaultdict

ROOT_DIR = "./out/cpzp/sums"  # Adjust as needed
OUTPUT_DIR = "./out/cpzp/sums_joined"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Sort folders numerically (30, 60, 90, ...)
subfolders = sorted(
    [f for f in os.listdir(ROOT_DIR) if os.path.isdir(os.path.join(ROOT_DIR, f))],
    key=lambda x: int(x),
)

# Group image paths by cohort image filename
cohort_images = defaultdict(list)

for folder in subfolders:
    folder_path = os.path.join(ROOT_DIR, folder)
    for file in os.listdir(folder_path):
        if file.endswith(".png"):
            cohort_images[file].append(os.path.join(folder_path, file))

# Stitch each set of images vertically
for filename, image_paths in cohort_images.items():
    images = [Image.open(path) for path in image_paths]

    widths, heights = zip(*(img.size for img in images))
    common_width = max(widths)
    total_height = sum(heights)

    stitched_image = Image.new("RGB", (common_width, total_height))

    y_offset = 0
    for img in images:
        stitched_image.paste(img, (0, y_offset))
        y_offset += img.height

    stitched_image.save(os.path.join(OUTPUT_DIR, filename))

print("✅ Done! Vertical stacks sorted by folder number saved in:", OUTPUT_DIR)
