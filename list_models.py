import google.generativeai as genai
import yaml
import os

def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def list_models():
    print("Listing Available Models...")
    try:
        config = load_config()
        nudge_conf = config.get('nudge', {})
        api_key = nudge_conf.get('gemini_api_key')
        
        if not api_key or "YOUR_GEMINI_API_KEY" in api_key:
            print("❌ API Key not configured.")
            return

        genai.configure(api_key=api_key)
        
        for m in genai.list_models():
            if 'generateContent' in m.supported_generation_methods:
                print(m.name)
                
    except Exception as e:
        print(f"❌ Error listing models: {e}")

if __name__ == "__main__":
    list_models()
