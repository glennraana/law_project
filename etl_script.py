import json
import requests
import zipfile
import io
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime, timezone
from transformers import pipeline

# --- Konfigurasjon og tilkobling ---
client = MongoClient("mongodb+srv://Cluster80101:VXJYYkR6bFpL@cluster80101.oa4vk.mongodb.net/Laws?retryWrites=true&w=majority")
db = client["Laws"]
lovdata_collection = db.lovdata_documents

REGISTRY_FILE = r"/Users/glenn/als_project/als/registry.json"
LOVDATA_API_KEY = "5zlxtcndwj7ac1wg"

# --- Sett opp summarizer ---
summarizer = pipeline(
    "summarization",
    model="facebook/bart-large-cnn",
    tokenizer="facebook/bart-large-cnn",
    framework="pt",
    max_length=300
)

def summarize_text(text, max_length=300, min_length=100):
    """
    Oppsummerer teksten dersom den er for lang.
    Hvis summarizeren feiler for et gitt chunk, returneres chunken uendret.
    """
    try:
        summary = summarizer(text, max_length=max_length, min_length=min_length, truncation=True)
        return summary[0]['summary_text']
    except Exception as e:
        print(f"[Summarization Error] {e}")
        return text

def chunk_text(text, chunk_size=10000):
    """
    Deler opp en tekst i biter på ca. chunk_size tegn.
    """
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

def fetch_data_from_url(url):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "X-API-Key": LOVDATA_API_KEY
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        if url.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                texts = []
                for filename in z.namelist():
                    try:
                        with z.open(filename) as file:
                            text = file.read().decode("utf-8", errors="replace")
                            # Hvis filen er veldig stor, del den opp i mindre biter og oppsummer hver bit
                            if len(text) > 500000:
                                print(f"Fil {filename} er veldig stor, deler opp i biter for oppsummering...")
                                chunks = chunk_text(text, chunk_size=10000)
                                chunk_summaries = []
                                for chunk in chunks:
                                    chunk_summaries.append(summarize_text(chunk))
                                text = "\n\n".join(chunk_summaries)
                            else:
                                soup = BeautifulSoup(text, "html.parser")
                                text = soup.get_text(separator="\n", strip=True)
                            texts.append(text)
                    except Exception as e:
                        print(f"Feil ved lesing av {filename} i {url}: {e}")
                combined_text = "\n\n".join(texts)
                if len(combined_text) > 1000000:
                    print(f"Samlet innhold fra {url} er for stort, deler opp og oppsummerer...")
                    chunks = chunk_text(combined_text, chunk_size=10000)
                    chunk_summaries = [summarize_text(chunk) for chunk in chunks]
                    combined_text = "\n\n".join(chunk_summaries)
                return combined_text
        else:
            # For API-endepunkt som returnerer JSON
            try:
                data = response.json()
                return json.dumps(data, ensure_ascii=False)
            except Exception:
                soup = BeautifulSoup(response.text, "html.parser")
                text = soup.get_text(separator="\n", strip=True)
                return text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def lovdata_etl_pipeline():
    try:
        with open(REGISTRY_FILE, "r") as f:
            registry = json.load(f)
    except Exception as e:
        print(f"Feil ved lasting av registry.json: {e}")
        return
    
    for source in registry["data_sources"]:
        if "Lovdata" in source.get("name", ""):
            url = source["url"]
            print(f"Behandler Lovdata-kilde: {url} ...")
            content = fetch_data_from_url(url)
            if content:
                document = {
                    "source": url,
                    "data_type": source.get("data_type", []),
                    "retrieved_at": datetime.now(timezone.utc).isoformat(),
                    "content": content
                }
                try:
                    result = lovdata_collection.insert_one(document)
                    print(f"Dokument fra {url} lagret med ID: {result.inserted_id}")
                except Exception as e:
                    print(f"Feil ved lagring av dokument fra {url}: {e}")
            else:
                print(f"Kunne ikke hente data fra {url}")
        else:
            print(f"Hopper over ikke-Lovdata kilde: {source.get('name', 'Ukjent')}")

if __name__ == "__main__":
    lovdata_etl_pipeline()
