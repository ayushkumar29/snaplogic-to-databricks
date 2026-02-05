import json
import sys

def inspect(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print("Top level keys:", list(data.keys()))
        
        if 'snaps' in data:
            print(f"Found 'snaps' array with {len(data['snaps'])} items.")
            print("First snap keys:", data['snaps'][0].keys() if data['snaps'] else "Empty")
        elif 'snap_map' in data:
            print(f"Found 'snap_map' dictionary with {len(data['snap_map'])} items.")
            first_key = list(data['snap_map'].keys())[0]
            print("First snap_map item keys:", data['snap_map'][first_key].keys())
        else:
            print("'snaps' key NOT found at root.")
            
        # Check property_map if snaps not at root
        if 'property_map' in data:
            print("property_map keys:", list(data['property_map'].keys()))
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect(sys.argv[1])
