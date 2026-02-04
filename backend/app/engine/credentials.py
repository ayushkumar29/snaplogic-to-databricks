from typing import Dict, List, Any
import re


class CredentialDetector:
    
    CREDENTIAL_PATTERNS = [
        r"account[_\-]?(ref|id|name)?",
        r"connection[_\-]?(ref|id|name)?",
        r"auth[_\-]?(ref|id|name)?",
        r"jdbc[_\-]?url",
        r"connection[_\-]?string",
        r"host[_\-]?name?",
        r"database[_\-]?(name|url)?",
        r"schema[_\-]?name?",
        r"catalog",
        r"username",
        r"password",
        r"api[_\-]?key",
        r"secret[_\-]?key",
        r"access[_\-]?key",
        r"token",
        r"bearer",
        r"bucket[_\-]?(name)?",
        r"container[_\-]?(name)?",
        r"storage[_\-]?account",
        r"s3[_\-]?path",
        r"azure[_\-]?path",
        r"gcs[_\-]?path",
        r"file[_\-]?path",
        r"folder[_\-]?path",
        r"output[_\-]?path",
        r"input[_\-]?path",
    ]
    
    ACCOUNT_SNAPS = [
        "jdbc", "database", "oracle", "sqlserver", "postgres", "mysql", "snowflake",
        "s3", "azure", "gcs", "blob", "adls",
        "salesforce", "workday", "servicenow", "sap",
        "rest", "soap", "http", "api",
        "sftp", "ftp", "email", "smtp",
        "kafka", "rabbitmq", "jms",
        "aws", "gcp", "azure"
    ]
    
    def detect(self, pipeline_data: Dict[str, Any]) -> Dict[str, Any]:
        result = {
            "accounts": [],
            "connections": [],
            "credentials": [],
            "paths": [],
            "custom_snaps": [],
            "parameters": [],
        }
        
        snaps = pipeline_data.get("snaps", [])
        
        for snap in snaps:
            self._analyze_snap(snap, result)
        
        params = pipeline_data.get("parameters", {})
        for param_name, param_value in params.items():
            result["parameters"].append({
                "name": param_name,
                "value": param_value if not self._is_sensitive(param_name) else "***",
                "type": "pipeline_parameter"
            })
        
        result = self._deduplicate(result)
        
        return result
    
    def _analyze_snap(self, snap: Dict, result: Dict):
        snap_type = snap.get("type", "").lower()
        snap_name = snap.get("name", snap.get("id", "Unknown"))
        properties = snap.get("properties", {})
        
        if self._is_custom_snap(snap_type):
            result["custom_snaps"].append({
                "snap_name": snap_name,
                "snap_type": snap.get("type", "Unknown"),
                "properties": properties,
                "requires_manual_review": True
            })
        
        needs_account = any(acc in snap_type for acc in self.ACCOUNT_SNAPS)
        self._scan_properties(properties, snap_name, result, needs_account)
    
    def _scan_properties(self, properties: Any, snap_name: str, result: Dict, needs_account: bool, path: str = ""):
        if isinstance(properties, dict):
            for key, value in properties.items():
                current_path = f"{path}.{key}" if path else key
                key_lower = key.lower()
                
                for pattern in self.CREDENTIAL_PATTERNS:
                    if re.search(pattern, key_lower, re.IGNORECASE):
                        category = self._categorize_property(key_lower)
                        
                        result[category].append({
                            "snap_name": snap_name,
                            "property": key,
                            "path": current_path,
                            "current_value": self._mask_value(value) if self._is_sensitive(key_lower) else value,
                            "needs_update": True
                        })
                        break
                
                if isinstance(value, (dict, list)):
                    self._scan_properties(value, snap_name, result, needs_account, current_path)
                    
        elif isinstance(properties, list):
            for i, item in enumerate(properties):
                self._scan_properties(item, snap_name, result, needs_account, f"{path}[{i}]")
    
    def _categorize_property(self, key: str) -> str:
        if any(p in key for p in ["account", "connection_ref", "auth_ref"]):
            return "accounts"
        elif any(p in key for p in ["jdbc", "url", "connection_string", "host"]):
            return "connections"
        elif any(p in key for p in ["user", "pass", "key", "secret", "token"]):
            return "credentials"
        elif any(p in key for p in ["path", "folder", "bucket", "container"]):
            return "paths"
        return "credentials"
    
    def _is_custom_snap(self, snap_type: str) -> bool:
        snap_type_lower = snap_type.lower()
        
        standard_prefixes = [
            "com-snaplogic-snaps",
            "com.snaplogic.snaps",
            "com-snaplogic-snap",
        ]
        
        is_standard = any(prefix in snap_type_lower for prefix in standard_prefixes)
        
        enterprise_patterns = [
            "custom", "enterprise", "internal", "corp", "company",
            "private", "proprietary"
        ]
        is_explicitly_custom = any(p in snap_type_lower for p in enterprise_patterns)
        
        return is_explicitly_custom or (not is_standard and len(snap_type) > 5)
    
    def _is_sensitive(self, key: str) -> bool:
        sensitive_words = ["password", "secret", "key", "token", "auth", "credential"]
        return any(word in key.lower() for word in sensitive_words)
    
    def _mask_value(self, value: Any) -> str:
        if value is None:
            return None
        str_val = str(value)
        if len(str_val) <= 4:
            return "****"
        return str_val[:2] + "****" + str_val[-2:] if len(str_val) > 4 else "****"
    
    def _deduplicate(self, result: Dict) -> Dict:
        for category in result:
            if isinstance(result[category], list):
                seen = set()
                unique = []
                for item in result[category]:
                    if isinstance(item, dict):
                        key = f"{item.get('snap_name', '')}-{item.get('property', item.get('name', ''))}"
                        if key not in seen:
                            seen.add(key)
                            unique.append(item)
                    else:
                        unique.append(item)
                result[category] = unique
        return result
    
    def generate_config_template(self, detected: Dict) -> str:
        lines = ["# SnapLogic to Databricks - Configuration Template", ""]
        
        if detected["accounts"]:
            lines.append("# === ACCOUNTS ===")
            for item in detected["accounts"]:
                lines.append(f"# Snap: {item['snap_name']}")
                lines.append(f"{item['property'].upper()}=")
            lines.append("")
        
        if detected["connections"]:
            lines.append("# === DATABASE CONNECTIONS ===")
            for item in detected["connections"]:
                lines.append(f"# Snap: {item['snap_name']}")
                lines.append(f"{item['property'].upper()}=")
            lines.append("")
        
        if detected["credentials"]:
            lines.append("# === CREDENTIALS (use Databricks Secrets) ===")
            for item in detected["credentials"]:
                lines.append(f"# Snap: {item['snap_name']}")
                lines.append(f"# {item['property'].upper()}=<use dbutils.secrets.get()>")
            lines.append("")
        
        if detected["paths"]:
            lines.append("# === FILE PATHS (update for DBFS/Unity Catalog) ===")
            for item in detected["paths"]:
                lines.append(f"# Snap: {item['snap_name']}")
                lines.append(f"# Original: {item.get('current_value', 'N/A')}")
                lines.append(f"{item['property'].upper()}=")
            lines.append("")
        
        return "\n".join(lines)
