import sys
import os
import sanitize_pipeline
import convert_to_databricks

def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <pipeline_file.slp>")
        sys.exit(1)

    input_path = sys.argv[1]
    
    # 1. Sanitize
    print(f"--- Step 1: Sanitizing {input_path} ---")
    base, _ = os.path.splitext(input_path)
    sanitized_path = f"{base}_sanitized.json"
    
    sanitize_pipeline.sanitize_snaplogic_pipeline(input_path, sanitized_path)
    
    if not os.path.exists(sanitized_path):
        print("Sanitization failed, aborting.")
        sys.exit(1)

    # 2. Convert
    print(f"--- Step 2: Converting {sanitized_path} to Databricks PySpark ---")
    convert_to_databricks.convert_to_databricks(sanitized_path)

if __name__ == "__main__":
    main()
