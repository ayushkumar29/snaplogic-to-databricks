"""
SnapLogic Pipeline Parser
Parses .slp files (JSON format) and extracts pipeline structure.
"""
import json
from typing import Dict, List, Any, Optional

class SLPParser:
    """Parser for SnapLogic pipeline files."""
    
    # Known snap types that reference child pipelines
    PIPELINE_EXECUTE_SNAPS = [
        "com-snaplogic-snaps-transform-pipelineexecutesnap",
        "com.snaplogic.snaps.transform.PipelineExecuteSnap",
        "PipelineExecute"
    ]
    
    def parse(self, content: str) -> Dict[str, Any]:
        """
        Parse SLP file content and extract pipeline structure.
        
        Args:
            content: JSON string content of the .slp file
            
        Returns:
            Parsed pipeline data with snaps, links, and metadata
        """
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in pipeline file: {e}")
        
        # Handle different SLP formats
        if "snap_map" in data:
            return self._parse_v2_format(data)
        elif "snaps" in data:
            return self._parse_v1_format(data)
        elif "instance_id" in data:
            return self._parse_export_format(data)
        else:
            # Try to auto-detect
            return self._auto_parse(data)
    
    def _parse_v2_format(self, data: Dict) -> Dict[str, Any]:
        """Parse newer SnapLogic format with snap_map."""
        snaps = []
        
        snap_map = data.get("snap_map", {})
        for snap_id, snap_data in snap_map.items():
            snaps.append(self._normalize_snap(snap_id, snap_data))
        
        return {
            "name": data.get("name", data.get("pipeline_name", "Unknown Pipeline")),
            "description": data.get("description", ""),
            "snaps": snaps,
            "links": self._extract_links(data),
            "parameters": data.get("parameters", {}),
            "raw": data
        }
    
    def _parse_v1_format(self, data: Dict) -> Dict[str, Any]:
        """Parse older SnapLogic format with snaps array."""
        snaps = []
        
        for snap in data.get("snaps", []):
            snap_id = snap.get("instance_id", snap.get("id", "unknown"))
            snaps.append(self._normalize_snap(snap_id, snap))
        
        return {
            "name": data.get("name", data.get("pipeline_name", "Unknown Pipeline")),
            "description": data.get("description", ""),
            "snaps": snaps,
            "links": data.get("links", []),
            "parameters": data.get("parameters", {}),
            "raw": data
        }
    
    def _parse_export_format(self, data: Dict) -> Dict[str, Any]:
        """Parse SnapLogic export format."""
        snaps = []
        
        # Check for nested structure
        if "node" in data:
            node = data["node"]
            if isinstance(node, dict):
                for snap_id, snap_data in node.items():
                    if isinstance(snap_data, dict):
                        snaps.append(self._normalize_snap(snap_id, snap_data))
        
        return {
            "name": data.get("instance_id", "Unknown Pipeline"),
            "description": data.get("description", ""),
            "snaps": snaps,
            "links": self._extract_links(data),
            "parameters": data.get("properties", {}).get("parameters", {}),
            "raw": data
        }
    
    def _auto_parse(self, data: Dict) -> Dict[str, Any]:
        """Attempt to auto-detect and parse unknown format."""
        snaps = []
        
        # Recursively search for snap-like objects
        self._find_snaps_recursive(data, snaps)
        
        return {
            "name": self._find_name(data),
            "description": "",
            "snaps": snaps,
            "links": [],
            "parameters": {},
            "raw": data
        }
    
    def _normalize_snap(self, snap_id: str, snap_data: Dict) -> Dict[str, Any]:
        """Normalize snap data to a consistent format."""
        # Extract snap type
        snap_type = (
            snap_data.get("class_id") or
            snap_data.get("snap_type") or
            snap_data.get("type") or
            snap_data.get("class_fqid", "").split("_")[-1] or
            "Unknown"
        )
        
        # Extract properties/settings
        properties = (
            snap_data.get("property_map") or
            snap_data.get("properties") or
            snap_data.get("settings") or
            {}
        )
        
        # Extract input/output views
        input_views = snap_data.get("input_views", snap_data.get("inputs", []))
        output_views = snap_data.get("output_views", snap_data.get("outputs", []))
        
        return {
            "id": snap_id,
            "type": snap_type,
            "name": snap_data.get("instance_name", snap_data.get("name", snap_id)),
            "properties": properties,
            "input_views": input_views if isinstance(input_views, list) else [],
            "output_views": output_views if isinstance(output_views, list) else [],
            "raw": snap_data
        }
    
    def _extract_links(self, data: Dict) -> List[Dict]:
        """Extract links/connections between snaps."""
        links = []
        
        if "link_map" in data:
            for link_id, link_data in data["link_map"].items():
                links.append({
                    "id": link_id,
                    "from_snap": link_data.get("from_id"),
                    "from_port": link_data.get("from_port"),
                    "to_snap": link_data.get("to_id"),
                    "to_port": link_data.get("to_port")
                })
        elif "links" in data and isinstance(data["links"], list):
            links = data["links"]
        
        return links
    
    def _find_snaps_recursive(self, data: Any, snaps: List, depth: int = 0):
        """Recursively search for snap objects in nested structure."""
        if depth > 10:  # Prevent infinite recursion
            return
            
        if isinstance(data, dict):
            # Check if this looks like a snap
            if any(key in data for key in ["class_id", "snap_type", "property_map"]):
                snap_id = data.get("instance_id", f"snap_{len(snaps)}")
                snaps.append(self._normalize_snap(snap_id, data))
            else:
                for value in data.values():
                    self._find_snaps_recursive(value, snaps, depth + 1)
        elif isinstance(data, list):
            for item in data:
                self._find_snaps_recursive(item, snaps, depth + 1)
    
    def _find_name(self, data: Dict) -> str:
        """Try to find the pipeline name from various locations."""
        candidates = [
            data.get("name"),
            data.get("pipeline_name"),
            data.get("instance_id"),
            data.get("properties", {}).get("name"),
        ]
        return next((c for c in candidates if c), "Unknown Pipeline")
    
    def find_child_references(self, pipeline_data: Dict) -> List[str]:
        """
        Find references to child pipelines (Pipeline Execute snaps).
        
        Returns:
            List of referenced pipeline paths/names
        """
        child_refs = []
        
        for snap in pipeline_data.get("snaps", []):
            snap_type = snap.get("type", "").lower()
            
            # Check if this is a Pipeline Execute snap
            if any(pe.lower() in snap_type for pe in self.PIPELINE_EXECUTE_SNAPS):
                props = snap.get("properties", {})
                
                # Look for pipeline path in various property names
                pipeline_path = (
                    props.get("pipeline_path") or
                    props.get("pipelinePath") or
                    props.get("settings", {}).get("pipeline_path") or
                    self._find_nested_value(props, "pipeline")
                )
                
                if pipeline_path:
                    child_refs.append(pipeline_path)
        
        return child_refs
    
    def _find_nested_value(self, data: Any, key: str) -> Optional[str]:
        """Find a value by key in nested structure."""
        if isinstance(data, dict):
            if key in data:
                return data[key]
            for value in data.values():
                result = self._find_nested_value(value, key)
                if result:
                    return result
        elif isinstance(data, list):
            for item in data:
                result = self._find_nested_value(item, key)
                if result:
                    return result
        return None
