"""Wrapper to run smoke_e2e and capture clean output."""
import subprocess, sys, os, re

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
result = subprocess.run(
    [sys.executable, os.path.join(root, "scripts", "smoke_e2e.py")],
    capture_output=True, cwd=root
)
out_path = os.path.join(root, "data", "smoke_result.txt")
raw = result.stdout.decode("utf-8", errors="replace")
# Process each \n-delimited line: take only the LAST \r-segment
# (requests progress bars write \r within a single line)
clean_lines = []
for line in raw.split("\n"):
    parts = line.split("\r")
    clean_lines.append(parts[-1])
clean = "\n".join(clean_lines)
with open(out_path, "w", encoding="utf-8") as f:
    f.write(clean)
    if result.stderr:
        f.write("\n--- STDERR ---\n")
        stderr_text = result.stderr.decode("utf-8", errors="replace")
        f.write(stderr_text)
print(f"rc={result.returncode}")
