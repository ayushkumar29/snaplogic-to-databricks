import json
import sys
import os

def sanitize_snaplogic_pipeline(input_path, output_path=None):
    """
    Reads a SnapLogic .slp (JSON) file, removes GUI layout information,
    and saves the sanitized JSON.
    """
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {input_path}")
        return
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {input_path}")
        return

    # Helper function to recursively remove specific keys
    def remove_keys(obj, keys_to_remove):
        if isinstance(obj, dict):
            # Remove keys at this level
            for key in keys_to_remove:
                if key in obj:
                    del obj[key]
            # Recurse into values
            for key, value in obj.items():
                remove_keys(value, keys_to_remove)
        elif isinstance(obj, list):
            # Recurse into list items
            for item in obj:
                remove_keys(item, keys_to_remove)

    # Keys commonly associated with GUI layout in SnapLogic
    # 'view' often contains x, y coordinates and other visual metadata
    keys_to_strip = ['view', 'x', 'y', 'height', 'width', 'color', 'render_map', 'detail_map']
    
    # Explicitly remove top-level render_map which is common in some exports
    if 'render_map' in data:
        del data['render_map']

    # Safely strip from snaps list if exists
    if 'snaps' in data:
        for snap in data['snaps']:
            if 'view' in snap:
                del snap['view']
                
    # Handle snap_map if exists
    if 'snap_map' in data:
        for snap_id, snap in data['snap_map'].items():
            if 'view' in snap:
                del snap['view']
            # Recursive cleanup for properties inside snaps
            remove_keys(snap, keys_to_strip)
    
    # Also unrelated metadata at root level if exists
    if 'job_stats' in data:
         del data['job_stats'] # Runtime stats not needed for conversion logic

    # If the user wants to strip *all* x,y coords specifically (heuristic), 
    # we can do that, but 'view' removal is usually safer and sufficient.
    # Let's stick to 'view' for now as per standard SnapLogic export structure.

    if not output_path:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_sanitized.json"

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    print(f"Sanitized pipeline saved to: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python sanitize_pipeline.py <pipeline_file.slp>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    sanitize_snaplogic_pipeline(input_file)
