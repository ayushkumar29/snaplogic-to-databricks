import os
import json
import shutil
import glob
from sanitize_pipeline import sanitize_snaplogic_pipeline
from convert_to_databricks import convert_to_databricks
from py_to_ipynb import py_to_ipynb

# Configuration
INPUT_DIR = "input_pipelines"
RESULT_DIR = "result"
JUNK_DIR = "junk"

def setup_directories():
    """Ensure all necessary directories exist."""
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(RESULT_DIR, exist_ok=True)
    os.makedirs(JUNK_DIR, exist_ok=True)
    print(f"Directories initialized: {INPUT_DIR}, {RESULT_DIR}, {JUNK_DIR}")

def is_valid_snaplogic_file(filepath):
    """
    Validates if a file is a valid SnapLogic pipeline file.
    Critieria:
    1. Must be valid JSON.
    2. Must contain 'snaps' or 'snap_map' keys.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if not isinstance(data, dict):
            return False
            
        if 'snaps' in data or 'snap_map' in data:
            return True
        
        return False
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False
    except Exception as e:
        print(f"Unexpected error validating {filepath}: {e}")
        return False

def process_file(filepath):
    """
    Process a single file: Validate -> (Move to Junk OR Convert).
    """
    filename = os.path.basename(filepath)
    print(f"Processing: {filename}")
    
    if is_valid_snaplogic_file(filepath):
        print(f"  [VALID] - Starting conversion...")
        
        # 1. Sanitize (to a temp location or memory - here using temp file approach as per existing module)
        # We'll use a temp filename in the same dir to avoid cross-device move issues, or just distinct name
        temp_sanitized_name = f"temp_sanitized_{filename}"
        if not temp_sanitized_name.endswith('.json'):
             temp_sanitized_name += '.json'
             
        temp_sanitized_path = os.path.join(INPUT_DIR, temp_sanitized_name)
        
        try:
            # Step A: Sanitize
            sanitize_snaplogic_pipeline(filepath, output_path=temp_sanitized_path)
            
            # Step B: Convert
            # Determine output filename
            base_name = os.path.splitext(filename)[0]
            output_filename = f"{base_name}_databricks.py"
            output_path = os.path.join(RESULT_DIR, output_filename)
            
            convert_to_databricks(temp_sanitized_path, output_path=output_path)
            
            # Step C: Convert to Notebook (.ipynb)
            with open(output_path, 'r', encoding='utf-8') as f:
                py_content = f.read()
            
            ipynb_filename = f"{base_name}_databricks.ipynb"
            ipynb_path = os.path.join(RESULT_DIR, ipynb_filename)
            py_to_ipynb(py_content, ipynb_path)

            print(f"  [SUCCESS] - Converted to {output_path}")
            print(f"  [SUCCESS] - Notebook saved to {ipynb_path}")
            
        except Exception as e:
            print(f"  [ERROR] - Failed during conversion: {e}")
        finally:
            # Cleanup temp file
            if os.path.exists(temp_sanitized_path):
                os.remove(temp_sanitized_path)
                
    else:
        print(f"  [INVALID] - Moving to junk...")
        destination = os.path.join(JUNK_DIR, filename)
        # Handle duplicate names in junk
        if os.path.exists(destination):
            base, ext = os.path.splitext(filename)
            counter = 1
            while os.path.exists(os.path.join(JUNK_DIR, f"{base}_{counter}{ext}")):
                counter += 1
            destination = os.path.join(JUNK_DIR, f"{base}_{counter}{ext}")
            
        shutil.move(filepath, destination)
        print(f"  Moved to {destination}")

def main():
    setup_directories()
    
    # Get list of all files in input directory
    # Supporting .json and .slp extensions mainly, but let's grab all to catch junk too
    files = [f for f in os.listdir(INPUT_DIR) if os.path.isfile(os.path.join(INPUT_DIR, f))]
    
    if not files:
        print(f"No files found in '{INPUT_DIR}'. Please place .slp or .json files there and run again.")
        return

    print(f"Found {len(files)} files to process...")
    
    for file in files:
        # Skip our own temp files if they got left over
        if file.startswith("temp_sanitized_"):
            continue
            
        filepath = os.path.join(INPUT_DIR, file)
        process_file(filepath)

if __name__ == "__main__":
    main()
