import os
import json
import sys
import requests
import re

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

    # --- TEMPLATES FROM LOAD_TO_STAGE REFERENCE ---
    
    TEMPLATE_HEADER = """# Databricks notebook source
# MAGIC %sh nc -zv 10.114.0.46 1433

# COMMAND ----------

# MAGIC %pip install pysmb chardet msal requests

# COMMAND ----------

from smb.SMBConnection import SMBConnection
import io
import chardet
from pyspark.sql.types import StructType, StructField, StringType
from io import BytesIO
import json
import os
import msal
import requests

# COMMAND ----------

# DBTITLE 1,Running vault client
# MAGIC %run /Workspace/webdruginfo_adp_comp_dev/pbm_webdruginfo_adp_comp/src/utilities/vault-client

# COMMAND ----------

# DBTITLE 1,Set up the vault confriguration
# Secret Scope variables
adb_secret_scope_name = "inspire_appsvc031919_dev" 
adb_clientid_key_name = "client_id"
adb_clientsecret_key_name = "client_secret"
adb_tenantid_key_name = "tenant_id"

# udap teams hashicorp settings
hsv_url = "https://preprod.vault.humana.net"
hsv_namespace = "nsudap"
hsv_team_folder = "inspire.appsvc031919" 
hsv_team_subfolder = "test"
hsv_password_mft = "password_mft"
hsv_username_mft = "username_mft"
hsv_db_username = "db_username"
hsv_db_password = "db_password"

# COMMAND ----------

vault_client = HashiCorpVaultClient(hsv_url, hsv_namespace, hsv_team_folder, hsv_team_subfolder)

sp_client_id = vault_client.fetch_secret(adb_secret_scope_name, adb_clientid_key_name)
sp_client_secret = vault_client.fetch_secret(adb_secret_scope_name, adb_clientsecret_key_name)
tenant_id = vault_client.fetch_secret(adb_secret_scope_name, adb_tenantid_key_name)

sp_authority_url = f"https://login.microsoftonline.com/{tenant_id}"
sp_scope = ["https://management.azure.com//.default"]
hsv_login_endpoint = f"{hsv_url}/v1/auth/jwt/azuread/inspirewellness/login"

spn_jwt = vault_client.get_spn_jwt_token(sp_client_id, sp_client_secret, sp_authority_url, sp_scope)
hsv_vault_token = vault_client.get_hsv_vault_token(spn_jwt, hsv_login_endpoint, hsv_team_folder)

# Fetch Secrets
username_mft_value = vault_client.get_hashicorp_key_value(hsv_vault_token, hsv_username_mft)
password_mft_value = vault_client.get_hashicorp_key_value(hsv_vault_token, hsv_password_mft)
db_username_value = vault_client.get_hashicorp_key_value(hsv_vault_token, hsv_db_username)
db_password_value = vault_client.get_hashicorp_key_value(hsv_vault_token, hsv_db_password)

# COMMAND ----------

# Configuration
env = 'dev'
# TODO: Update path to match your deployment
with open('/Workspace/Users/user@humana.com/config.json', 'r') as f:
    current_config = json.load(f)

# Get config for current environment
config_key = list(current_config.values())[0] if current_config else {} # fallback
# typically: config = current_config.get(env, {})

# Extract common variables (Mocked for generation, user must verify)
client_machine_name = config.get("client_machine_name", "Unknown")
server_name = config.get("server_name", "Unknown")
domain = config.get("domain", "Unknown")
share_name = config.get("share_name", "Unknown")
path = config.get("path", "Unknown")
filename = config.get("filename", "Unknown")
jdbc_url = config.get("jdbc_url", "jdbc:sqlserver://...")
jdbc_properties = config.get("jdbc_driver", {})

# COMMAND ----------
"""

    # --- FEW-SHOT EXAMPLES ---
    FEW_SHOT_EXAMPLES = {
        "simpleread": """
def file_reader():
    conn = SMBConnection(username_mft_value, password_mft_value, client_machine_name, server_name, domain=domain, use_ntlm_v2=True)
    assert conn.connect(server_name, 445)

    files = conn.listPath(share_name, path)
    file_info = next((f for f in files if f.filename == filename), None)
    if not file_info:
        raise FileNotFoundError(f"{filename} not found in {path}")

    # Read file content as binary data
    file_buffer = BytesIO()
    conn.retrieveFile(share_name, f"{path}/{filename}", file_buffer)
    raw_file_bytes = file_buffer.getvalue()

    return {"raw_file_bytes": raw_file_bytes}
""",
        "csvparser": """
def csv_parser(input_data):
    raw_file_bytes = input_data["raw_file_bytes"]
    # Auto-detect encoding
    detection = chardet.detect(raw_file_bytes)
    encoding = detection['encoding'] if detection['encoding'] else 'utf-8'
    decoded_str = raw_file_bytes.decode(encoding)
    
    # Prepare a file-like object for Spark ingestion
    rdd = sc.parallelize(decoded_str.splitlines())
    # Define Schema based on expected columns
    schema = StructType([StructField("Col1", StringType(), True)])
    df = spark.read.csv(rdd, schema=schema, sep='^', quote='"', header=False, encoding=encoding)
    
    # Create one document per CSV row
    documents = df.rdd.map(lambda row: {"Col1": row.Col1})
    
    # Yield documents
    for doc in documents.collect():
        yield doc
""",
        "sqlserver-insert": """
def Insert_to_Table(documents_stream):
    # Prepare data for bulk insert
    rows = [(doc["Col1"],) for doc in documents_stream]
    if rows:
        df = spark.createDataFrame(rows, schema=StructType([StructField("Col1", StringType(), True)]))
        df.write.jdbc(jdbc_url, "dbo.TargetTable", mode="append", properties=jdbc_properties)
""",
        "mapper": """
def mapper_function(documents_stream):
    for doc in documents_stream:
        # Transformation logic
        doc["new_col"] = doc["old_col"].upper()
        yield doc
"""
    }

    try:
        with open(sanitized_json_path, 'r', encoding='utf-8') as f:
            pipeline_data = json.load(f)
            
        # NORMALIZE: Handle 'snap_map' vs 'snaps'
        if 'snaps' not in pipeline_data and 'snap_map' in pipeline_data:
            print("Normalizing 'snap_map' to 'snaps' list...")
            pipeline_data['snaps'] = list(pipeline_data['snap_map'].values())
            del pipeline_data['snap_map']

        # MINIMIZE
        minimized_data = minimize_pipeline_data(pipeline_data)
        print(f"Minimized pipeline to {len(minimized_data)} snaps.")
            
    except Exception as e:
        print(f"Error reading JSON: {e}")
        return

    # Start Building Script
    final_script = []
    final_script.append(TEMPLATE_HEADER)
    
    function_names = []
    
    print(f"Converting {len(minimized_data)} snaps iteratively...")
    
    for i, snap in enumerate(minimized_data):
        snap_name = snap['name']
        class_id = snap['class_id']
        
        # Sanitize function name
        safe_func_name = re.sub(r'[^a-zA-Z0-9_]', '_', snap_name)
        function_names.append(safe_func_name)
        
        print(f"  > Processing {i+1}/{len(minimized_data)}: {snap_name}...")
        
        # Detect Few-Shot
        example_code = ""
        lower_key = snap_name.lower() + class_id.lower()
        for key, code in FEW_SHOT_EXAMPLES.items():
            if key in lower_key:
                example_code = f"\nREFERENCE PATTERN:\n{code}\n"
                break

        # Prompt for Function Generation
        prompt = f"""
        ACT AS A DATABRICKS ARCHITECT for Humana.
        Task: Write a Python FUNCTION for Snap: "{snap_name}" ({class_id}).
        Function Name: def {safe_func_name}(input_data):
        
        Context:
        - Input: 'input_data' (Dict, Bytes, or Generator of Dicts)
        - Output: Return the processed data or Yield if it's a stream/mapper.
        - Use 'vault_client' secrets where needed.
        {example_code}
        
        STRICT RULES:
        - Output PYTHON CODE ONLY.
        - NO Markdown.
        - NO global code execution, ONLY the function definition.
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
            
            final_script.append(f"# --- Snap: {snap_name} ---")
            final_script.append(snippet)
            final_script.append("")

        except Exception as e:
            print(f"Error converting snap {snap_name}: {e}")
            final_script.append(f"# Error generating {snap_name}")

    # Generate Main Execution Block
    final_script.append("# COMMAND ----------")
    final_script.append("# Main Execution Flow")
    final_script.append("try:")
    
    # Linear chaining logic (naive)
    prev_var = None
    for idx, func_name in enumerate(function_names):
        if idx == 0:
            final_script.append(f"    step_{idx}_out = {func_name}()")
        else:
            final_script.append(f"    step_{idx}_out = {func_name}(step_{idx-1}_out)")
        prev_var = f"step_{idx}_out"
        
    final_script.append("except Exception as e:")
    final_script.append("    print(f'Pipeline execution failed: {str(e)}')")
    final_script.append("    raise e")

    # --- Convert to IPYNB ---
    def py_to_ipynb(py_content):
        lines = py_content.splitlines()
        cells = []
        current_cell_lines = []
        
        def add_cell(lines_list):
            if not lines_list: return
            # Clean up
            while lines_list and not lines_list[0].strip(): lines_list.pop(0)
            while lines_list and not lines_list[-1].strip(): lines_list.pop()
            if not lines_list: return

            source = [line + "\n" for line in lines_list]
            if source: source[-1] = source[-1].rstrip("\n")

            cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": source
            })

        for line in lines:
            if line.strip() == "# COMMAND ----------":
                add_cell(current_cell_lines)
                current_cell_lines = []
            else:
                current_cell_lines.append(line)
        add_cell(current_cell_lines)

        return {
            "cells": cells,
            "metadata": {
                "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                "language_info": {"name": "python", "version": "3.8"}
            },
            "nbformat": 4, "nbformat_minor": 4
        }

    final_code = "\n".join(final_script)
    notebook_json = py_to_ipynb(final_code)

    if not output_path:
        # Determine Output Filename
        pipeline_name = "UnknownPipeline"
        try:
             pipeline_name = pipeline_data.get('property_map', {}).get('info', {}).get('label', {}).get('value', "")
        except: pass
            
        if not pipeline_name:
            pipeline_name = pipeline_data.get('info', {}).get('title', "")
        if not pipeline_name:
             pipeline_name = pipeline_data.get('info', {}).get('label', {}).get('value', "")
             
        if pipeline_name:
            safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pipeline_name)
            safe_name = re.sub(r'_+', '_', safe_name)
            output_path = f"result/{safe_name}_databricks.ipynb"
        else:
            base, _ = os.path.splitext(sanitized_json_path)
            if base.endswith("_sanitized"): base = base[:-10]
            base_name = os.path.basename(base)
            output_path = f"result/{base_name}_databricks.ipynb"

    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(notebook_json, f, indent=2)
    
    print(f"Generated Databricks notebook saved to: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python convert_to_databricks.py <sanitized_pipeline.json>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    convert_to_databricks(input_file)
