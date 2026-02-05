# SnapLogic to Databricks Converter

A robust utility for converting SnapLogic pipelines (`.slp` or `.json`) into Databricks PySpark notebooks. This tool prioritizes enterprise security, validation, and automated batch processing.

## Key Features

- **Batch Processing:** Automatically processes multiple pipeline files at once.
- **Intelligent Validation:** Detects and segregates invalid or "junk" files.
- **Security Guardrails:** Replaces hardcoded credentials with Databricks Secrets.
- **Sanitization:** Strips GUI metadata to optimize processing.
- **Enterprise Ready:** Adds error handling, logging, and validation checks to generated code.

## Directory Structure

- **`input_pipelines/`**: Drop your `.slp` or `.json` files here.
- **`result/`**: Contains the generated Databricks PySpark notebooks (`.py`).
- **`junk/`**: Invalid or corrupt files are automatically moved here.
- **`process_uploads.py`**: The main script for batch processing.
- **`convert_to_databricks.py`**: Core logic for LLM-based code generation.
- **`sanitize_pipeline.py`**: Utility to clean SnapLogic metadata.

## Usage

### 1. Setup
Ensure you have Python installed and the required dependencies:
```bash
pip install requests
```
*(Make sure you have a local LLM running, e.g., Ollama, if utilizing the LLM conversion features.)*

### 2. Run Batch Processor
1.  Place your SnapLogic export files in the `input_pipelines` folder.
2.  Run the processor script:
    ```bash
    python process_uploads.py
    ```

### 3. Review Output
- **Success:** Check `result/` for your converted Python scripts.
- **Failures:** Check console output for errors. Invalid files will be in `junk/`.

## Generated Code Features
The converted notebooks include:
- **Global Error Handling:** `try/except` blocks around each Snap's logic.
- **Validation:** Checks for empty DataFrames before write operations.
- **Secrets Management:** `dbutils.secrets.get` allows for secure credential retrieval.
