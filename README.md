# Codespaces 터미널에서, repo 루트에서 실행
python - <<'PY'
import os, re, secrets, pathlib, sys
env = pathlib.Path(".env")
val = secrets.token_urlsafe(64)
txt = env.read_text(encoding="utf-8") if env.exists() else ""
lines = [ln for ln in txt.splitlines() if not ln.startswith("APP_SECRET_KEY=")]
lines.append(f"APP_SECRET_KEY={val}")
env.write_text("\n".join(lines) + "\n", encoding="utf-8")
ok = len(val) >= 64 and re.fullmatch(r"[A-Za-z0-9_\-]+", val) is not None
print("APP_SECRET_KEY:", "OK" if ok else "INVALID")
sys.exit(0 if ok else 1)
PY