"""Refresh the microsite's WebP copies after recapturing product screenshots.

Requires Pillow. The build uses committed assets and does not require Python.
"""

from pathlib import Path

from PIL import Image

root = Path(__file__).resolve().parents[2]
destination = root / "website/public/screenshots"
destination.mkdir(parents=True, exist_ok=True)
for source in sorted((root / "docs/frontend/screenshots/current").glob("*.png")):
    with Image.open(source) as image:
        image.save(destination / f"{source.stem}.webp", "WEBP", quality=86, method=6)
