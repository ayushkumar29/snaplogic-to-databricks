from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import uvicorn
from pathlib import Path

from app.api.routes import router as api_router

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(
    title="SnapLogic to Databricks Converter",
    description="Convert SnapLogic pipelines to Databricks PySpark code",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
app.include_router(api_router, prefix="/api")

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = BASE_DIR / "static" / "index.html"
    return FileResponse(html_path)

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "healthy", "message": "SnapLogic Converter API is running"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
