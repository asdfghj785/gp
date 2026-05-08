#!/bin/zsh
set -euo pipefail

ROOT="/Users/eudis/ths"
LOG="$ROOT/logs/ollama_ensure.log"
mkdir -p "$ROOT/logs"

ts() { date '+%Y-%m-%d %H:%M:%S'; }

if /usr/bin/curl -sS --max-time 3 http://127.0.0.1:11434/api/tags >/dev/null 2>&1; then
  echo "[$(ts)] Ollama already ready" >> "$LOG"
  exit 0
fi

echo "[$(ts)] Ollama not responding, opening Ollama.app" >> "$LOG"
/usr/bin/open -gja Ollama >/dev/null 2>&1 || true

for _ in {1..30}; do
  if /usr/bin/curl -sS --max-time 3 http://127.0.0.1:11434/api/tags >/dev/null 2>&1; then
    echo "[$(ts)] Ollama ready after ensure" >> "$LOG"
    exit 0
  fi
  /bin/sleep 2
done

echo "[$(ts)] Ollama ensure failed after waiting" >> "$LOG"
exit 1
