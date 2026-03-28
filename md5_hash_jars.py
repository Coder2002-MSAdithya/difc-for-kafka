#!/usr/bin/env python3
from pathlib import Path
import hashlib

LIB_DIR = Path("build/libs")

def md5_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()

def main():
    if not LIB_DIR.exists() or not LIB_DIR.is_dir():
        raise SystemExit(f"Directory not found: {LIB_DIR}")

    file_hashes = []
    distinct = set()

    for p in sorted(LIB_DIR.iterdir()):
        if p.is_file():
            h = md5_file(p)
            file_hashes.append((p.name, h))
            distinct.add(h)

    # Output 1: file -> hash mapping
    print("=== FILE_HASHES ===")
    for name, h in file_hashes:
        print(f"{name},{h}")

    # Output 2: distinct comma-separated list
    print("\n=== DISTINCT_MD5_CSV ===")
    print(",".join(sorted(distinct)))

    # Optional: Java Set.of(...) ready output
    print("\n=== JAVA_SET_OF ===")
    print("Set.of(" + ", ".join(f"\"{h}\"" for h in sorted(distinct)) + ");")

if __name__ == "__main__":
    main()
