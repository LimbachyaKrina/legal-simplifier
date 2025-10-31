# llm_adapter.py
import os
import time
import json
from dotenv import load_dotenv

load_dotenv()

# Try to import google genai SDK; if not available we will fail gracefully
try:
    from google import genai
    from google.genai.errors import ServerError
    SDK_AVAILABLE = True
except Exception:
    SDK_AVAILABLE = False

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PRIMARY_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
FALLBACK_MODELS = ["gemini-1.5", "gemini-1.5-flash-8b", "gemini-1.5-pro"]  # try a few if available
MAX_RETRIES = 3
RETRY_BACKOFF = 1.0  # seconds (exponential backoff multiplier)

def _local_fallback_summary(prompt_text: str) -> str:
    """
    Deterministic simple fallback summary generator used when the LLM isn't available.
    This prevents the UI from crashing and still returns a readable summary.
    The function is intentionally conservative and must not invent numbers.
    """
    # Try to extract numbers and small table-like facts from the prompt string (safe)
    try:
        # If prompt contains a JSON-like 'facts' variable (as in our streamlit), we can return the first rows
        if "facts =" in prompt_text:
            idx = prompt_text.find("facts =")
            snippet = prompt_text[idx: idx + 2000]
            # return a tiny canned message
            return "LLM unavailable. Here is a short factual summary:\n" + snippet[:600]
    except Exception:
        pass
    # Generic fallback
    return "LLM unavailable — a short deterministic summary cannot be generated. Numerical results are shown above and the executed SQL is available for provenance."

def call_gemini_sdk(prompt: str, model: str, max_tokens: int = 512, temperature: float = 0.1):
    """
    Call the google-genai SDK and return textual output. Raises on hard failures.
    Retries on ServerError (503/overload).
    """
    if not SDK_AVAILABLE:
        raise RuntimeError("google-genai SDK not installed (pip install google-genai).")

    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set in environment or .env")

    client = genai.Client(api_key=GEMINI_API_KEY)
    # Attempt with retries and fallbacks
    models_to_try = [model] + [m for m in FALLBACK_MODELS if m != model]
    last_exc = None
    for m in models_to_try:
        for attempt in range(MAX_RETRIES):
            try:
                resp = client.models.generate_content(
                    model=m,
                    contents=prompt,
                    # older SDK uses config objects; generate_content accepts contents and config
                    # Some versions return resp.text; some keep structured response. We normalize below.
                    # You can adjust generation config if needed in your SDK version.
                    # Example: config=genai.types.GenerateContentConfig(temperature=temperature, max_output_tokens=max_tokens)
                )
                # Many SDK versions provide .text attribute
                text = getattr(resp, "text", None)
                if text:
                    return text
                # else try structured
                if isinstance(resp, dict):
                    # try common keys
                    if "candidates" in resp and len(resp["candidates"])>0:
                        # some endpoints return candidates -> content -> parts
                        try:
                            return resp["candidates"][0]["content"]["parts"][0]["text"]
                        except Exception:
                            return json.dumps(resp)
                    if "output" in resp:
                        return json.dumps(resp["output"])
                    return str(resp)
                # fallback to string
                return str(resp)
            except Exception as e:
                last_exc = e
                # If server error with overload, wait and retry
                msg = str(e).lower()
                if ("503" in msg) or ("overload" in msg) or ("temporarily unavailable" in msg) or ("servererror" in msg) or ("server error" in msg):
                    wait = (2 ** attempt) * RETRY_BACKOFF
                    print(f"Model {m} overloaded/server error. Retrying in {wait}s (attempt {attempt+1}/{MAX_RETRIES})...")
                    time.sleep(wait)
                    continue
                else:
                    # If some other error (auth, 404), break and try next model
                    print(f"Model {m} raised non-retryable error: {e}")
                    break
    # All attempts failed
    raise RuntimeError(f"All model attempts failed. Last exception: {repr(last_exc)}")

def llm_generate_short(prompt: str) -> str:
    """
    Safe wrapper for Streamlit. Attempts Gemini SDK; on failure returns deterministic fallback text.
    """
    # Respect offline mode to avoid any external calls
    if os.getenv("OFFLINE", "0") == "1":
        return _local_fallback_summary(prompt)
    try:
        # prefer primary model
        return call_gemini_sdk(prompt, model=PRIMARY_MODEL, max_tokens=512, temperature=0.1)
    except Exception as e:
        # print helpful debug info to console
        print("LLM call failed:", repr(e))
        # return a deterministic fallback summary rather than crashing
        return _local_fallback_summary(prompt)
