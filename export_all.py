import os

root = os.path.dirname(os.path.abspath(__file__))
output = os.path.join(root, "all.txt")
exclude_dirs = {".git", "__pycache__", "venv"}
exclude_files = {"all.txt", "export_all.py", ".env"}

with open(output, "w", encoding="utf-8") as out:
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in exclude_dirs]
        for f in filenames:
            if f in exclude_files:
                continue
            path = os.path.join(dirpath, f)
            rel  = os.path.relpath(path, root)
            out.write(f"{'='*60}\n")
            out.write(f"FICHIER : {rel}\n")
            out.write(f"{'='*60}\n")
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    out.write(fh.read())
            except Exception:
                out.write(f"[impossible de lire {rel}]\n")
            out.write("\n\n")

print(f"all.txt genere dans {root}")
