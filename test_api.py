"""Quick test: does the llm_review module load the API key and model correctly?"""
import os, sys, pathlib

# Simulate what llm_review does
api_key = None
resolved_api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
print(f"1. env var value: {repr(resolved_api_key[:20]) if resolved_api_key else repr(resolved_api_key)}")

if not resolved_api_key:
    env_path = pathlib.Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("ANTHROPIC_API_KEY="):
                resolved_api_key = line.split("=", 1)[1].strip().strip("'\"")
                break
    print(f"2. from .env: {repr(resolved_api_key[:20]) if resolved_api_key else 'EMPTY'}")

if not resolved_api_key:
    print("FAIL: no API key found")
    sys.exit(1)

import anthropic
client = anthropic.Anthropic(api_key=resolved_api_key)
model = "claude-haiku-4-5-20251001"
print(f"3. Using model: {model}")

try:
    msg = client.messages.create(
        model=model,
        max_tokens=10,
        messages=[{"role": "user", "content": "say hello"}],
    )
    print(f"OK: {msg.content[0].text}")
    print(f"Model used: {msg.model}")
except Exception as e:
    print(f"ERROR: {e}")
