"""
API Routes for SnapLogic Converter
"""
from fastapi import APIRouter, File, UploadFile, HTTPException, Form
from fastapi.responses import JSONResponse
from typing import List, Optional
import json
import os
from pathlib import Path

from app.engine.parser import SLPParser
from app.engine.graph import DependencyGraph
from app.engine.generator import SparkGenerator
from app.llm.agent import LLMAgent

router = APIRouter()

# Temporary storage for uploaded files
UPLOAD_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Initialize components
parser = SLPParser()
graph = DependencyGraph()
generator = SparkGenerator()
llm_agent = LLMAgent()

@router.post("/upload")
async def upload_files(files: List[UploadFile] = File(...)):
    """
    Upload one or more SLP files for conversion.
    Returns the parsed structure and any missing dependencies.
    """
    uploaded_pipelines = []
    missing_dependencies = []
    
    for file in files:
        if not file.filename.endswith(('.slp', '.json', '.zip')):
            continue
            
        content = await file.read()
        file_path = UPLOAD_DIR / file.filename
        
        # Save file
        with open(file_path, 'wb') as f:
            f.write(content)
        
        # Parse the pipeline
        try:
            pipeline_data = parser.parse(content.decode('utf-8'))
            uploaded_pipelines.append({
                "filename": file.filename,
                "pipeline_name": pipeline_data.get("name", "Unknown"),
                "snap_count": len(pipeline_data.get("snaps", [])),
                "data": pipeline_data
            })
            
            # Check for child pipeline references
            child_refs = parser.find_child_references(pipeline_data)
            for ref in child_refs:
                if not any(p["filename"] == ref for p in uploaded_pipelines):
                    missing_dependencies.append(ref)
                    
        except Exception as e:
            uploaded_pipelines.append({
                "filename": file.filename,
                "error": str(e)
            })
    
    return {
        "success": True,
        "pipelines": uploaded_pipelines,
        "missing_dependencies": list(set(missing_dependencies)),
        "message": f"Uploaded {len(uploaded_pipelines)} pipeline(s)"
    }

@router.post("/convert")
async def convert_pipelines(
    pipeline_names: List[str] = Form(...),
    use_ai: bool = Form(True)
):
    """
    Convert uploaded pipelines to Databricks code.
    """
    results = []
    unknown_snaps = []
    
    for pipeline_name in pipeline_names:
        file_path = UPLOAD_DIR / pipeline_name
        
        if not file_path.exists():
            results.append({
                "pipeline": pipeline_name,
                "error": "File not found. Please upload first."
            })
            continue
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            pipeline_data = parser.parse(content)
            
            # Build dependency graph
            graph.add_pipeline(pipeline_data)
            
            # Generate PySpark code
            code, unknowns = generator.generate(pipeline_data)
            
            # If there are unknown snaps and AI is enabled, ask LLM
            if unknowns and use_ai:
                for snap in unknowns:
                    ai_code = await llm_agent.resolve_snap(snap)
                    if ai_code:
                        code = code.replace(f"# TODO: Unknown snap - {snap['type']}", ai_code)
                    else:
                        unknown_snaps.append(snap)
            
            results.append({
                "pipeline": pipeline_name,
                "code": code,
                "success": True
            })
            
        except Exception as e:
            results.append({
                "pipeline": pipeline_name,
                "error": str(e),
                "success": False
            })
    
    return {
        "results": results,
        "unknown_snaps": unknown_snaps,
        "dependency_order": graph.get_execution_order()
    }

@router.post("/ask-ai")
async def ask_ai_for_help(
    snap_data: dict,
    question: Optional[str] = None
):
    """
    Ask the AI for help with a specific snap or question.
    """
    response = await llm_agent.ask(snap_data, question)
    return {"response": response}

@router.get("/pipelines")
async def list_uploaded_pipelines():
    """List all uploaded pipeline files."""
    files = list(UPLOAD_DIR.glob("*.slp")) + list(UPLOAD_DIR.glob("*.json"))
    return {
        "pipelines": [f.name for f in files]
    }

@router.delete("/pipelines/{filename}")
async def delete_pipeline(filename: str):
    """Delete an uploaded pipeline file."""
    file_path = UPLOAD_DIR / filename
    if file_path.exists():
        os.remove(file_path)
        return {"message": f"Deleted {filename}"}
    raise HTTPException(status_code=404, detail="File not found")
