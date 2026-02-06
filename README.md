# SnapLogic to Databricks Converter 🚀

**An Enterprise-Grade utility to automate the migration of SnapLogic pipelines to Databricks PySpark notebooks.**

This tool parses SnapLogic `.slp` or JSON exports, analyzes their logic, and generates robust, ready-to-deploy PySpark code. It utilizes AI (LLM) for intelligent logic translation while enforcing strict engineering guardrails.

## ✨ Key Features

-   **Intelligent Conversion**: Uses AI to understand complex Snap logic (Mapper, Router, Union, etc.) and generate equivalent PySpark transformations.
-   **Enterprise Architecture**:
    -   **Vault Integration**: Automatically injects HashiCorp Vault client for secure credential management.
    -   **Dynamic Configuration**: Supports environment-based configuration (Dev/QA/Prod) via JSON.
    -   **Modular Design**: generated code is structured into functional blocks (e.g., `def File_Reader()`), improving maintainability and testability.
-   **Security First**:
    -   Secrets are never hardcoded.
    -   Uses `pysmb` and `BytesIO` for secure, memory-safe file handling.
-   **Native Notebook Support**: Outputs directly to `.ipynb` format for seamless import into Databricks Workspaces.

## 🛠️ Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/your-org/snaplogic-to-databricks.git
    cd snaplogic-to-databricks
    ```

2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Local LLM Setup**:
    Ensure you have [Ollama](https://ollama.ai/) running locally with the `llama3.2` model (or configure the script to point to your enterprise LLM endpoint).
    ```bash
    ollama run llama3.2
    ```

## 🚀 Usage

Run the converter on a single SnapLogic pipeline file:

```bash
python main.py <path_to_pipeline.json>
```

**Example:**
```bash
python main.py input_pipelines/load_to_staging.json
```

The tool will:
1.  **Sanitize** the input JSON to remove GUI metadata.
2.  **Analyze** the pipeline structure.
3.  **Generate** a `.ipynb` file in the `result/` directory (e.g., `result/load_to_staging_databricks.ipynb`).

## 🏗️ Project Structure

-   `main.py`: Entry point for the converter.
-   `convert_to_databricks.py`: Core logic for LLM interaction and code generation.
-   `sanitize_pipeline.py`: Utility to clean input JSONs.
-   `py_to_ipynb.py`: Helper to format Python code as Jupyter Notebooks.
-   `input_pipelines/`: Place your source SnapLogic JSON files here.
-   `result/`: Generated Databricks notebooks will appear here.

## 🛡️ Guardrails & Patterns

The converter enforces the **"LoadToStage"** architectural standard:
-   **Functions over Scripts**: Every Snap becomes a discrete Python function.
-   **Explicit I/O**: Inputs and Outputs are explicitly passed between steps.
-   **Vault-first Auth**: All credentials are fetched via the Vault Client.

---
*Built for the Modern Data Platform.*
