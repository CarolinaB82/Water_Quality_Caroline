import os
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

app = FastAPI(
    title="Water Quality Deployment API",
    description="API permettant de déclencher le workflow Databricks du projet Water Quality.",
    version="1.0.0"
)


DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID")


@app.get("/")
def home():
    return {
        "message": "Water Quality Deployment API is running"
    }


@app.post("/deploy")
def deploy_pipeline():
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN or not DATABRICKS_JOB_ID:
        raise HTTPException(
            status_code=500,
            detail="Variables d'environnement Databricks manquantes"
        )

    url = f"{DATABRICKS_HOST}/api/2.2/jobs/run-now"

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "job_id": int(DATABRICKS_JOB_ID)
    }

    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=30
    )

    if response.status_code not in [200, 201]:
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text
        )

    return {
        "status": "started",
        "databricks_response": response.json()
    }