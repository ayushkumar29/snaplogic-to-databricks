import gradio as gr
import os
import shutil
import time
from sanitize_pipeline import sanitize_snaplogic_pipeline
from convert_to_databricks import convert_to_databricks

def process_pipeline(file_obj, model_name):
    """
    Main processing function for the Gradio UI.
    1. Sanitizes the uploaded SLP file.
    2. Converts it to a Databricks notebook using the specified LLM.
    """
    if file_obj is None:
        return "Please upload a .slp file.", None

    # Working with the uploaded file path
    input_path = file_obj.name
    
    # Create a temporary output directory to avoid clutter
    output_dir = "converted_output"
    os.makedirs(output_dir, exist_ok=True)
    
    base_name = os.path.splitext(os.path.basename(input_path))[0]
    sanitized_path = os.path.join(output_dir, f"{base_name}_sanitized.json")
    final_output_path = os.path.join(output_dir, f"{base_name}_databricks.py")

    status_log = []

    # Step 1: Sanitize
    try:
        status_log.append(f"Sanitizing {os.path.basename(input_path)}...")
        sanitize_snaplogic_pipeline(input_path, sanitized_path)
        status_log.append("Sanitization complete.")
    except Exception as e:
        return f"Error during sanitization: {str(e)}", None

    # Step 2: Convert
    try:
        status_log.append(f"Converting using model: {model_name}...")
        # Note: We might want to pass model_name if the convert script supported it dynamically.
        # Currently convert_to_databricks.py uses a hardcoded default or env var, 
        # but for now we rely on its internal default (llama3.2).
        # To support dynamic model selection, we would need to slightly refactor convert_to_databricks.py
        # For now, we assume the user is happy with the default local LLM.
        
        convert_to_databricks(sanitized_path, final_output_path)
        status_log.append("Conversion complete.")
    except Exception as e:
         return f"Error during conversion: {str(e)}\n\nLogs:\n" + "\n".join(status_log), None

    # Read the generated code to display
    try:
        with open(final_output_path, "r", encoding="utf-8") as f:
            generated_code = f.read()
    except Exception as e:
        return f"Error reading output file: {str(e)}", None

    return generated_code, final_output_path

# UI Layout
with gr.Blocks(title="SnapLogic to Databricks Converter") as demo:
    gr.Markdown("# 🔄 SnapLogic to Databricks Converter")
    gr.Markdown("Upload your `.slp` pipeline file to sanitize it and generate a PySpark notebook.")
    
    with gr.Row():
        with gr.Column():
            file_input = gr.File(label="Upload SnapLogic Pipeline (.slp)", file_types=[".slp", ".json"])
            model_input = gr.Dropdown(
                choices=["llama3.2", "gpt-4", "gemini-1.5-pro"], 
                value="llama3.2", 
                label="LLM Model (Currently uses generic local LLM logic)"
            )
            convert_btn = gr.Button("Convert Pipeline", variant="primary")
        
        with gr.Column():
            code_output = gr.Code(label="Generated PySpark Code", language="python", lines=20)
            file_output = gr.File(label="Download Notebook (.py)")

    convert_btn.click(
        fn=process_pipeline,
        inputs=[file_input, model_input],
        outputs=[code_output, file_output]
    )

if __name__ == "__main__":
    demo.launch(inbrowser=True)
