from fastapi import FastAPI
from app.api.v1.main import api_router

app = FastAPI()

app.include_router(api_router, prefix="/api/v1")


@app.get("/")
def home():
    return "Welcome to Wired Articles API"


@app.get("/api/health")
def health():
    return {"status": "healthy"}