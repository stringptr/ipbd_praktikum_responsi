from fastapi import APIRouter, HTTPException
from typing import List, Optional
import json
from pathlib import Path

router = APIRouter()

DATA_FILE = Path(__file__).parent.parent.parent.parent / "data" / "wired_articles.json"


def load_articles():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Data file not found: {DATA_FILE}")
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("")
def get_all_articles():
    try:
        data = load_articles()
        return data
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading data: {str(e)}")


@router.get("/count")
def get_articles_count():
    try:
        data = load_articles()
        return {"count": data.get("articles_count", 0)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/session")
def get_session_info():
    try:
        data = load_articles()
        return {
            "session_id": data.get("session_id"),
            "timestamp": data.get("timestamp"),
            "articles_count": data.get("articles_count"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))