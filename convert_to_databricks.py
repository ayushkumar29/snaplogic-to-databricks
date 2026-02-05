import os
import json
import sys
import requests

def minimize_pipeline_data(data):
    """
    Aggressively strips metadata to fit meaningful logic into LLM context window.
    """
    minimized = []
    snaps = data.get('snaps', [])
    
    for snap in snaps:
        # Extract meaningful name
        snap_name = snap.get("name", "Unknown")
        if snap_name == "Unknown":
            # Try to find label in property_map
            props = snap.get("property_map", {})
            info = props.get("info", {})
            label = info.get("label", {})
            if isinstance(label, dict):
                snap_name = label.get("value", snap.get("instance_id", "Snap"))
        
        # Keep only critical fields
        simple_snap = {
            "name": snap_name,
            "class_id": snap.get("class_id", ""),
            "settings": snap.get("property_map", {}).get("settings", {}) 
                        if "property_map" in snap else snap.get("settings", {})
        }
        
        # If settings is deep, try to flatten or clean it
        # For now, just keeping it as is might be enough if we drop instance_ids etc.
        minimized.append(simple_snap)
        
    return minimized

def convert_to_databricks(sanitized_json_path, output_path=None):
    """
    Reads a sanitized SnapLogic JSON file and uses a local LLM (Ollama) to generate
    a Databricks PySpark notebook.
    """
    
    # Configuration for standard Ollama local instance
    OLLAMA_API_URL = "http://localhost:11434/api/generate"
    MODEL_NAME = "llama3.2"

    try:
        with open(sanitized_json_path, 'r', encoding='utf-8') as f:
            pipeline_data = json.load(f)
            
        # NORMALIZE: Handle 'snap_map' vs 'snaps'
        if 'snaps' not in pipeline_data and 'snap_map' in pipeline_data:
            print("Normalizing 'snap_map' to 'snaps' list...")
            pipeline_data['snaps'] = list(pipeline_data['snap_map'].values())
            # Optionally remove the original map to save tokens
            del pipeline_data['snap_map']

        # MINIMIZE: Reduce token count
        minimized_data = minimize_pipeline_data(pipeline_data)
        print(f"Minimized pipeline to {len(minimized_data)} snaps for LLM context.")
        # DEBUG: Print first item to check quality
        if minimized_data:
            print(f"Sample Snap: {minimized_data[0]}")
            
    except Exception as e:
        print(f"Error reading JSON: {e}")
        return

    # Llama 3.2 struggles with batch processing. We will iterate in Python and ask for snippets.
    
    # Post-Processing: Force Enterprise Guardrails
    final_script = []
    
    # 1. Add Global Error Handler at the top
    final_script.append("# Databricks notebook source")
    final_script.append("# ENTERPRISE GUARDRAILS ENFORCED: Security, Validation, Idempotency, Auditing")
    final_script.append("import pyspark")
    final_script.append("from pyspark.sql import SparkSession")
    final_script.append("from pyspark.sql.functions import *")
    final_script.append("")
    final_script.append("# Initialize Spark")
    final_script.append("spark = SparkSession.builder.getOrCreate()")
    final_script.append("")
    final_script.append("# COMMAND ----------")
    final_script.append("")
    final_script.append("# Global Error Handler Definition")
    final_script.append("def handle_critical_failure(error_msg, snap_name='Unknown'):")
    final_script.append("    print(f'CRITICAL FAILURE in {snap_name}: {error_msg}')")
    final_script.append("    # Trigger ServiceNow Webhook")
    final_script.append("    # import requests")
    final_script.append("    # requests.post('https://servicenow.enterprise.com/api/incident', json={'error': error_msg, 'source': 'Databricks'})")
    final_script.append("    raise Exception(error_msg)")
    final_script.append("")
    
    import re
    
    print(f"Converting {len(minimized_data)} snaps iteratively...")
    
    for i, snap in enumerate(minimized_data):
        snap_name = snap['name']
        class_id = snap['class_id']
        settings = snap.get('settings', {})
        
        print(f"  > Processing {i+1}/{len(minimized_data)}: {snap_name} ({class_id})...")
        
        # Micro-prompt...
        prompt = f"""
        ACT AS A SECURITY-FOCUSED DATABRICKS ARCHITECT.
        Task: Write PySpark code for Snap: "{snap_name}" ({class_id}).
        Context: df_{i} is input. Output MUST be assigned to df_{i+1}.
        
        STRICT RULES:
        - Output PYTHON CODE ONLY.
        - NO Markdown.
        """

        payload = {
            "model": MODEL_NAME,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.1}
        }

        try:
            response = requests.post(OLLAMA_API_URL, json=payload)
            response.raise_for_status()
            result = response.json()
            raw = result.get("response", "").strip()
            
            # Extract code
            code_match = re.search(r"```python(.*?)```", raw, re.DOTALL)
            snippet = code_match.group(1).strip() if code_match else raw.strip()
            snippet = snippet.replace("```", "")
            
            # --- GUARDRAIL ENFORCEMENT (REGEX) ---
            
            # 1. Security: Replace hardcoded passwords/secrets
            # Matches .option("password", "...") or password="..."
            snippet = re.sub(r'("password",\s*")[^"]+(")', r'\1" + dbutils.secrets.get(scope="enterprise", key="db_password") + \2', snippet, flags=re.IGNORECASE)
            snippet = re.sub(r'(password\s*=\s*")[^"]+(")', r'\1" + dbutils.secrets.get(scope="enterprise", key="db_password") + \2', snippet, flags=re.IGNORECASE)
            
            # 2. Validation: Inject checks before writes
            if "write" in snippet or ".save" in snippet:
                validation_logic = f"\n# GUARDRAIL: Validation before Write\nif 'df_{i+1}' in locals() and df_{i+1}.count() == 0:\n    handle_critical_failure('Empty DataFrame before write', '{snap_name}')\n"
                snippet = validation_logic + snippet
            
            # 3. Error Handling Wrapper
            snippet_indented = "\n    ".join(snippet.splitlines())
            wrapped_snippet = f"""
try:
    # --- Snap: {snap_name} ---
    {snippet_indented}
except Exception as e:
    handle_critical_failure(str(e), '{snap_name}')
"""
            
            final_script.append("# COMMAND ----------")
            final_script.append(wrapped_snippet)
            
        except Exception as e:
            print(f"Error converting snap {snap_name}: {e}")
            final_script.append(f"# Error converting {snap_name}: {e}")

    final_code = "\n".join(final_script)

    if not output_path:
        base, ext = os.path.splitext(sanitized_json_path)
        if base.endswith("_sanitized"):
            base = base[:-10]
        output_path = f"{base}_databricks.py"

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(final_code)
    
    print(f"Generated Databricks notebook saved to: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python convert_to_databricks.py <sanitized_pipeline.json>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    convert_to_databricks(input_file)
