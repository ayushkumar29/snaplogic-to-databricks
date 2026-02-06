import json
import html

def ipynb_to_html(notebook_dict):
    """
    Converts a notebook dictionary to a standalone HTML string.
    """
    
    html_content = ["""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Notebook View</title>
    <!-- PrismJS for Syntax Highlighting -->
    <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.24.1/themes/prism-coy.min.css" rel="stylesheet" />
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; padding: 20px; background: #f5f5f5; color: #333; }
        .notebook-container { max-width: 900px; margin: 0 auto; background: white; color: #333; padding: 30px; border-radius: 8px; box_shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .notebook-container * { color: #333; } /* Force all children to respect dark text color */
        .cell { margin-bottom: 20px; border: 1px solid #e0e0e0; border-radius: 4px; overflow: hidden; }
        .cell-input { position: relative; }
        .prompt { position: absolute; left: 0; top: 0; width: 60px; padding: 10px; text-align: right; color: #888; font-family: monospace; font-size: 12px; background: #fafafa; border-right: 1px solid #eee; height: 100%; box-sizing: border-box; }
        .code-area { margin-left: 60px; padding: 10px; background: #fcfcfc; overflow-x: auto; }
        pre { margin: 0; }
        code { font-family: 'Consolas', 'Monaco', 'Andale Mono', 'Ubuntu Mono', monospace; font-size: 14px; }
        
        .cell-markdown { padding: 10px 20px; color: #333; }
        
        /* Scrollbar styling */
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-thumb { background: #ccc; border-radius: 4px; }
        ::-webkit-scrollbar-track { background: #f1f1f1; }
    </style>
</head>
<body>
    <div class="notebook-container">
    """]
    
    cells = notebook_dict.get('cells', [])
    
    for i, cell in enumerate(cells):
        cell_type = cell.get('cell_type')
        source_list = cell.get('source', [])
        source_text = "".join(source_list)
        
        if cell_type == 'code':
            count = cell.get('execution_count')
            prompt_text = f"In [{count if count is not None else ' '}]:"
            
            # Escape HTML in code
            escaped_code = html.escape(source_text)
            
            html_content.append(f"""
            <div class="cell">
                <div class="cell-input">
                    <div class="prompt">{prompt_text}</div>
                    <div class="code-area">
                        <pre><code class="language-python">{escaped_code}</code></pre>
                    </div>
                </div>
            </div>
            """)
            
        elif cell_type == 'markdown':
            # Basic markdown handling - for now just wrap in div, 
            # ideally would use markdown library but keeping deps low.
            # We'll just pre-wrap it to preserve format or simple p-wrap.
            escaped_md = html.escape(source_text).replace('\n', '<br>')
            html_content.append(f"""
            <div class="cell cell-markdown">
                <div>{escaped_md}</div>
            </div>
            """)
            
    html_content.append("""
    </div>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.24.1/prism.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.24.1/components/prism-python.min.js"></script>
</body>
</html>
    """)
    
    return "".join(html_content)
