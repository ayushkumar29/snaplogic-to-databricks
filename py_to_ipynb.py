import json
import os

def py_to_ipynb(py_content, output_path=None):
    """
    Converts a Databricks-style Python script (separated by # COMMAND ----------)
    into a Jupyter Notebook dictionary.
    
    Args:
        py_content (str): The raw Python source code.
        output_path (str, optional): Path to save the .ipynb file.

    Returns:
        dict: The notebook structure.
    """
    lines = py_content.splitlines()
    cells = []
    current_cell_lines = []
    
    def add_cell(lines_list):
        if not lines_list:
            return
        
        # Clean up leading empty lines
        while lines_list and not lines_list[0].strip():
            lines_list.pop(0)
        # Clean up trailing empty lines
        while lines_list and not lines_list[-1].strip():
            lines_list.pop()
            
        if not lines_list:
            return

        source = [line + "\n" for line in lines_list]
        # Remove last newline from last item
        if source:
            source[-1] = source[-1].rstrip("\n")

        cell = {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": source
        }
        cells.append(cell)

    for line in lines:
        if line.strip() == "# COMMAND ----------":
            add_cell(current_cell_lines)
            current_cell_lines = []
        else:
            current_cell_lines.append(line)
            
    # Add valid remaining lines
    add_cell(current_cell_lines)

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.5"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(notebook, f, indent=2)
            
    return notebook

if __name__ == "__main__":
    # Test
    sample_code = """
import pyspark
# COMMAND ----------
print("Hello World")
# COMMAND ----------
df = spark.createDataFrame([])
"""
    print(json.dumps(py_to_ipynb(sample_code), indent=2))
