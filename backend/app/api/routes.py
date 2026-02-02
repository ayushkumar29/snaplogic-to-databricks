"""
API Routes for SnapLogic Converter
Enhanced with credential detection and custom snap handling
"""
from fastapi import APIRouter, File, UploadFile, HTTPException, Form, Body
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
import json
import os
from pathlib import Path

from app.engine.parser import SLPParser
from app.engine.graph import DependencyGraph
from app.engine.generator import SparkGenerator
from app.engine.credentials import CredentialDetector
from app.engine.custom_snap import CustomSnapHandler
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
credential_detector = CredentialDetector()
custom_snap_handler = CustomSnapHandler(llm_agent)

@router.post("/upload")
async def upload_files(files: List[UploadFile] = File(...)):
    """
    Upload one or more SLP files for conversion.
    Returns parsed structure, missing dependencies, and detected credentials.
    """
    uploaded_pipelines = []
    missing_dependencies = []
    all_credentials = {
        "accounts": [],
        "connections": [],
        "credentials": [],
        "paths": [],
        "custom_snaps": [],
        "parameters": []
    }
    
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
            
            # Detect credentials and accounts
            detected = credential_detector.detect(pipeline_data)
            
            # Merge detected items
            for key in all_credentials:
                all_credentials[key].extend(detected.get(key, []))
            
            uploaded_pipelines.append({
                "filename": file.filename,
                "pipeline_name": pipeline_data.get("name", "Unknown"),
                "snap_count": len(pipeline_data.get("snaps", [])),
                "has_custom_snaps": len(detected.get("custom_snaps", [])) > 0,
                "requires_credentials": len(detected.get("credentials", [])) > 0,
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
    
    # Generate config template if credentials detected
    config_template = None
    if any(all_credentials[k] for k in all_credentials):
        config_template = credential_detector.generate_config_template(all_credentials)
    
    return {
        "success": True,
        "pipelines": uploaded_pipelines,
        "missing_dependencies": list(set(missing_dependencies)),
        "detected_credentials": all_credentials,
        "config_template": config_template,
        "message": f"Uploaded {len(uploaded_pipelines)} pipeline(s)"
    }

@router.post("/analyze-credentials")
async def analyze_credentials(pipeline_name: str = Form(...)):
    """
    Analyze a specific pipeline for credentials and accounts.
    """
    file_path = UPLOAD_DIR / pipeline_name
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Pipeline not found")
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        pipeline_data = parser.parse(content)
        detected = credential_detector.detect(pipeline_data)
        config_template = credential_detector.generate_config_template(detected)
        
        return {
            "pipeline": pipeline_name,
            "detected": detected,
            "config_template": config_template,
            "summary": {
                "accounts_count": len(detected["accounts"]),
                "connections_count": len(detected["connections"]),
                "credentials_count": len(detected["credentials"]),
                "paths_count": len(detected["paths"]),
                "custom_snaps_count": len(detected["custom_snaps"])
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/analyze-custom-snap")
async def analyze_custom_snap(
    snap_data: Dict[str, Any] = Body(...),
    user_answers: Optional[Dict[str, str]] = Body(None)
):
    """
    Analyze a custom/enterprise snap and get conversion suggestions.
    """
    # First, get basic analysis
    analysis = custom_snap_handler.analyze_custom_snap(snap_data)
    
    # If AI is available and user provided answers, generate code
    if user_answers or analysis["is_known_enterprise"]:
        ai_code = await custom_snap_handler.generate_with_ai(snap_data, user_answers)
        analysis["generated_code"] = ai_code
    
    return analysis

@router.post("/convert")
async def convert_pipelines(
    pipeline_names: List[str] = Form(...),
    use_ai: bool = Form(True),
    credential_config: Optional[str] = Form(None)
):
    """
    Convert uploaded pipelines to Databricks code.
    Optionally apply credential configuration.
    """
    results = []
    unknown_snaps = []
    custom_snaps_info = []
    
    # Parse credential config if provided
    cred_map = {}
    if credential_config:
        try:
            for line in credential_config.strip().split('\n'):
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.split('=', 1)
                    cred_map[key.strip()] = value.strip()
        except:
            pass
    
    for pipeline_name in pipeline_names:
        file_path = UPLOAD_DIR / pipeline_name
        
        if not file_path.exists():
            results.append({
                "pipeline": pipeline_name,
                "error": "File not found. Please upload first.",
                "success": False
            })
            continue
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            pipeline_data = parser.parse(content)
            
            # Build dependency graph
            graph.add_pipeline(pipeline_data)
            
            # Detect custom snaps first
            detected = credential_detector.detect(pipeline_data)
            
            # Generate PySpark code
            code, unknowns = generator.generate(pipeline_data)
            
            # Handle unknown/custom snaps
            for snap in unknowns:
                if use_ai:
                    # Check if it's a known enterprise type
                    analysis = custom_snap_handler.analyze_custom_snap(snap)
                    
                    if analysis["is_known_enterprise"]:
                        # Use template
                        ai_code = analysis.get("generated_code", "")
                    else:
                        # Try AI generation
                        ai_code = await llm_agent.resolve_snap(snap)
                    
                    if ai_code:
                        code = code.replace(
                            f"# TODO: Unknown snap - {snap['type']}", 
                            ai_code
                        )
                    else:
                        unknown_snaps.append(snap)
                        custom_snaps_info.append({
                            "snap": snap,
                            "questions": analysis.get("questions_for_user", [])
                        })
                else:
                    unknown_snaps.append(snap)
            
            # Apply credential replacements
            for key, value in cred_map.items():
                code = code.replace(f"{{{key}}}", value)
                code = code.replace(f"${{{key}}}", value)
            
            results.append({
                "pipeline": pipeline_name,
                "code": code,
                "success": True,
                "custom_snaps_count": len(detected.get("custom_snaps", []))
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
        "custom_snaps_requiring_input": custom_snaps_info,
        "dependency_order": graph.get_execution_order()
    }

@router.post("/ask-ai")
async def ask_ai_for_help(
    snap_data: Dict[str, Any] = Body(default={}),
    question: Optional[str] = Body(default=None)
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

@router.get("/llm-status")
async def get_llm_status():
    """Get the current LLM provider status."""
    return llm_agent.get_provider_info()
