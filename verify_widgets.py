
import re

# Mocking the function from convert_to_databricks.py for testing
def inject_enterprise_secrets(code, service_type):
    """
    Injects specific secrets based on the detected service type.
    Handlers: Usernames, Passwords, JDBC URIs, Kafka Configs.
    """
    scope = "enterprise"
    
    # --- WIDGET INJECTION FOR CONFIG (Server/Port/Host) ---
    # Look for hardcoded servers or ports and replace with Widgets
    
    # Helper to inject widget replacement
    def inject_widget(pattern, widget_name):
        nonlocal code
        # Pattern MUST capture the key assignment in group 1 (e.g. 'server=')
        # And consume the value + quotes without capturing them for preservation
        # Replacement is just group 1 + the widget call
        replacement = f'\\1dbutils.widgets.get("{widget_name}")'
        code = re.sub(pattern, replacement, code, flags=re.IGNORECASE)
        
    if service_type == 'email':
        # server="smtp..." -> server=dbutils...
        # Matches: server= "..." or '...'
        inject_widget(r'(server\s*=\s*)(["\'])[^"\']+?\2', "Email_Server")
        # port=587 or port="587"
        # Matches: port= (optional quote) digits (optional quote)
        inject_widget(r'(port\s*=\s*)(["\']?)\d+\2', "Email_Port")
        
    if service_type in ['smb', 'ftp']:
        # host="192..."
        inject_widget(r'(host\s*=\s*)(["\'])[^"\']+?\2', f"{service_type.upper()}_Host")
        # share="shared"
        if service_type == 'smb':
             inject_widget(r'(share\s*=\s*)(["\'])[^"\']+?\2', "SMB_Share")
        # port
        inject_widget(r'(port\s*=\s*)(["\']?)\d+\2', f"{service_type.upper()}_Port")

    # --- SECRET INJECTION FOR AUTH (User/Pass) ---
    pwd_key = f"{service_type}_password"
    
    # Spark .option("password", "value") -> .option("password", dbutils.secrets.get(...))
    code = re.sub(
        r'(\.option\s*\(\s*"password"\s*,\s*)"([^"]+)"(\s*\))', 
        f'\\1dbutils.secrets.get(scope="{scope}", key="{pwd_key}")\\3', 
        code, flags=re.IGNORECASE
    )
    
    # Generic password="value" assignment
    code = re.sub(
        r'(password\s*=\s*)"([^"]+)"', 
        f'\\1dbutils.secrets.get(scope="{scope}", key="{pwd_key}")', 
        code, flags=re.IGNORECASE
    )

    user_key = f"{service_type}_user"
    
    code = re.sub(
        r'(\.option\s*\(\s*"user"\s*,\s*)"([^"]+)"(\s*\))', 
        f'\\1dbutils.secrets.get(scope="{scope}", key="{user_key}")\\3', 
        code, flags=re.IGNORECASE
    )
    
    code = re.sub(
        r'(user\s*=\s*)"([^"]+)"', 
        f'\\1dbutils.secrets.get(scope="{scope}", key="{user_key}")', 
        code, flags=re.IGNORECASE
    )
        
    return code

# Test Cases
def test_email_injection():
    input_code = 'email_snap.send(server="smtp.gmail.com", port=587, user="admin@example.com", password="secret_password")'
    expected_fragment = 'server=dbutils.widgets.get("Email_Server")'
    output = inject_enterprise_secrets(input_code, 'email')
    
    print(f"--- Email Injection Test ---")
    print(f"Input: {input_code}")
    print(f"Output: {output}")
    
    if 'dbutils.widgets.get("Email_Server")' in output and 'dbutils.widgets.get("Email_Port")' in output:
        print("✅ Widget Injection PASS")
    else:
        print("❌ Widget Injection FAIL")

    if 'dbutils.secrets.get(scope="enterprise", key="email_password")' in output:
         print("✅ Secret Injection PASS")
    else:
         print("❌ Secret Injection FAIL")

def test_smb_injection():
    input_code = 'smb_client.connect(host="192.168.1.100", share="finance_data", port=445, user="admin", password="password123")'
    output = inject_enterprise_secrets(input_code, 'smb')
    
    print(f"\n--- SMB Injection Test ---")
    print(f"Input: {input_code}")
    print(f"Output: {output}")
    
    if 'dbutils.widgets.get("SMB_Host")' in output and 'dbutils.widgets.get("SMB_Share")' in output:
        print("✅ Widget Injection PASS")
    else:
        print("❌ Widget Injection FAIL")

if __name__ == "__main__":
    test_email_injection()
    test_smb_injection()
