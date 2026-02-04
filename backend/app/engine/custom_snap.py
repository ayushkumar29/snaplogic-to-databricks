from typing import Dict, Any, Optional, List
import json


class CustomSnapHandler:
    
    def __init__(self, llm_agent=None):
        self.llm_agent = llm_agent
        
        self.enterprise_hints = {
            "salesforce": {
                "databricks_equivalent": "spark-salesforce connector or REST API",
                "import": "# Install: databricks-connect, simple-salesforce",
                "template": '''from simple_salesforce import Salesforce

sf = Salesforce(
    username=dbutils.secrets.get("salesforce", "username"),
    password=dbutils.secrets.get("salesforce", "password"),
    security_token=dbutils.secrets.get("salesforce", "token")
)

records = sf.query("{query}")
df = spark.createDataFrame(records["records"])'''
            },
            "workday": {
                "databricks_equivalent": "Workday REST API",
                "template": '''import requests

api_url = "https://your-tenant.workday.com/api/v1/{endpoint}"
headers = {"Authorization": "Bearer " + dbutils.secrets.get("workday", "token")}

response = requests.get(api_url, headers=headers)
data = response.json()
df = spark.createDataFrame(data["results"])'''
            },
            "servicenow": {
                "databricks_equivalent": "ServiceNow REST API",
                "template": '''import requests

instance = "your-instance"
api_url = f"https://{instance}.service-now.com/api/now/table/{table}"
auth = (dbutils.secrets.get("servicenow", "user"), dbutils.secrets.get("servicenow", "pass"))

response = requests.get(api_url, auth=auth)
df = spark.createDataFrame(response.json()["result"])'''
            },
            "sap": {
                "databricks_equivalent": "SAP HANA Spark Connector or RFC",
                "template": '''df = spark.read.format("com.sap.spark.hana") \\
    .option("url", dbutils.secrets.get("sap", "url")) \\
    .option("user", dbutils.secrets.get("sap", "user")) \\
    .option("password", dbutils.secrets.get("sap", "pass")) \\
    .option("dbtable", "{table}") \\
    .load()'''
            },
            "kafka": {
                "databricks_equivalent": "Spark Structured Streaming",
                "template": '''df = spark.readStream.format("kafka") \\
    .option("kafka.bootstrap.servers", "{bootstrap_servers}") \\
    .option("subscribe", "{topic}") \\
    .option("startingOffsets", "earliest") \\
    .load()

from pyspark.sql.functions import from_json, col
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))'''
            }
        }
    
    def analyze_custom_snap(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        snap_type = snap.get("type", "").lower()
        snap_name = snap.get("name", "Unknown")
        properties = snap.get("properties", {})
        
        result = {
            "snap_name": snap_name,
            "snap_type": snap.get("type"),
            "is_known_enterprise": False,
            "suggested_approach": None,
            "generated_code": None,
            "questions_for_user": [],
            "documentation_hints": []
        }
        
        for enterprise_type, hints in self.enterprise_hints.items():
            if enterprise_type in snap_type:
                result["is_known_enterprise"] = True
                result["suggested_approach"] = hints.get("databricks_equivalent")
                result["generated_code"] = self._apply_template(
                    hints.get("template", ""), 
                    properties
                )
                result["documentation_hints"].append(
                    f"See Databricks documentation for {enterprise_type} integration"
                )
                break
        
        if not result["is_known_enterprise"]:
            result["questions_for_user"] = self._generate_questions(snap)
            result["suggested_approach"] = "Manual conversion with AI assistance"
        
        return result
    
    def _apply_template(self, template: str, properties: Dict) -> str:
        code = template
        for key, value in properties.items():
            placeholder = "{" + key + "}"
            if placeholder in code:
                code = code.replace(placeholder, str(value))
        return code
    
    def _generate_questions(self, snap: Dict) -> List[Dict]:
        questions = []
        
        questions.append({
            "id": "purpose",
            "question": f"What does the snap '{snap.get('name', 'Unknown')}' do?",
            "hint": "e.g., Reads from SAP, Calls internal API, Transforms data"
        })
        
        questions.append({
            "id": "inputs_outputs",
            "question": "What are the inputs and outputs?",
            "hint": "e.g., Takes customer data, outputs enriched records"
        })
        
        questions.append({
            "id": "external_system",
            "question": "Does it connect to an external system? If so, which one?",
            "hint": "e.g., Internal CRM, SFTP server, Custom API"
        })
        
        questions.append({
            "id": "documentation",
            "question": "Is there any documentation or sample code available?",
            "hint": "You can paste a link or description"
        })
        
        return questions
    
    async def generate_with_ai(self, snap: Dict, user_answers: Dict = None) -> str:
        if not self.llm_agent or not self.llm_agent.is_available():
            return self._generate_placeholder(snap)
        
        prompt = f"""You are an expert in migrating SnapLogic to Databricks.

I have a CUSTOM/ENTERPRISE SnapLogic snap that needs to be converted to PySpark code for Databricks.

**Snap Type:** {snap.get('type', 'Unknown')}
**Snap Name:** {snap.get('name', 'Unknown')}
**Properties:** 
```json
{json.dumps(snap.get('properties', {}), indent=2)}
```

**Input Views:** {snap.get('input_views', [])}
**Output Views:** {snap.get('output_views', [])}

"""
        if user_answers:
            prompt += f"""
**User-provided context:**
- Purpose: {user_answers.get('purpose', 'Not provided')}
- Inputs/Outputs: {user_answers.get('inputs_outputs', 'Not provided')}  
- External System: {user_answers.get('external_system', 'Not provided')}
- Documentation: {user_answers.get('documentation', 'Not provided')}

"""
        
        prompt += """Please generate:
1. Equivalent PySpark/Python code for Databricks
2. Include proper credential handling using dbutils.secrets
3. Add comments explaining what each section does
4. Handle errors gracefully

Return ONLY the Python code."""

        try:
            response = await self.llm_agent.llm.ainvoke(prompt)
            return response.content if hasattr(response, 'content') else str(response)
        except Exception as e:
            return self._generate_placeholder(snap, str(e))
    
    def _generate_placeholder(self, snap: Dict, error: str = None) -> str:
        snap_type = snap.get('type', 'Unknown')
        snap_name = snap.get('name', 'Unknown')
        properties = snap.get('properties', {})
        
        code = f'''# CUSTOM ENTERPRISE SNAP - Requires Manual Implementation
# Snap Type: {snap_type}
# Snap Name: {snap_name}
#
# Original Properties:
# {json.dumps(properties, indent=2)[:500]}...
#
def process_{snap_name.replace(" ", "_").replace("-", "_").lower()}(df):
    result_df = df
    return result_df

df_result = process_{snap_name.replace(" ", "_").replace("-", "_").lower()}(df)
'''
        
        if error:
            code += f"\n# Note: AI generation failed - {error}"
        
        return code
