import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

class LLMAgent:
    
    def __init__(self):
        self.llm = None
        self.provider = None
        self._initialize_llm()
    
    def _initialize_llm(self):
        if os.getenv("GROQ_API_KEY"):
            try:
                from langchain_groq import ChatGroq
                self.llm = ChatGroq(
                    model="llama-3.3-70b-versatile",
                    temperature=0.2,
                    api_key=os.getenv("GROQ_API_KEY")
                )
                self.provider = "groq"
                print("Using Groq (Llama 3.3 70B)")
                return
            except Exception as e:
                print(f"Groq init failed: {e}")
        
        if os.getenv("OLLAMA_MODEL") or self._check_ollama():
            try:
                from langchain_ollama import ChatOllama
                model = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")
                self.llm = ChatOllama(
                    model=model,
                    temperature=0.2,
                    base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
                )
                self.provider = "ollama"
                print(f"Using Ollama ({model})")
                return
            except Exception as e:
                print(f"Ollama init failed: {e}")
        
        if os.getenv("OPENAI_API_KEY"):
            try:
                from langchain_openai import ChatOpenAI
                self.llm = ChatOpenAI(
                    model="gpt-4o",
                    temperature=0.2,
                    api_key=os.getenv("OPENAI_API_KEY")
                )
                self.provider = "openai"
                print("Using OpenAI (GPT-4o)")
                return
            except Exception as e:
                print(f"OpenAI init failed: {e}")
        
        if os.getenv("ANTHROPIC_API_KEY"):
            try:
                from langchain_anthropic import ChatAnthropic
                self.llm = ChatAnthropic(
                    model="claude-3-5-sonnet-20241022",
                    temperature=0.2,
                    api_key=os.getenv("ANTHROPIC_API_KEY")
                )
                self.provider = "anthropic"
                print("Using Anthropic (Claude 3.5)")
                return
            except Exception as e:
                print(f"Anthropic init failed: {e}")
        
        print("No LLM configured. AI features disabled.")
        print("Set GROQ_API_KEY (free) or run Ollama locally.")
    
    def _check_ollama(self) -> bool:
        try:
            import httpx
            response = httpx.get("http://localhost:11434/api/tags", timeout=2)
            return response.status_code == 200
        except:
            return False
    
    async def resolve_snap(self, snap: Dict[str, Any]) -> Optional[str]:
        if not self.llm:
            return None
        
        prompt = f"""Convert this SnapLogic snap to Databricks PySpark code:

**Snap Type:** {snap.get('type', 'Unknown')}
**Snap Name:** {snap.get('name', 'Unknown')}
**Properties:** {snap.get('properties', {})}

Generate equivalent PySpark code. Use `df` as input, create output variable.
Return ONLY Python code, no explanations."""

        try:
            response = await self.llm.ainvoke(prompt)
            return response.content if hasattr(response, 'content') else str(response)
        except Exception as e:
            print(f"LLM Error: {e}")
            return None
    
    async def ask(self, snap_data: Dict[str, Any], question: Optional[str] = None) -> str:
        if not self.llm:
            return "AI disabled. Set GROQ_API_KEY (free at console.groq.com) or run Ollama."
        
        prompt = f"""Context: {snap_data}
Question: {question or "How to convert this to PySpark?"}"""

        try:
            response = await self.llm.ainvoke(prompt)
            return response.content if hasattr(response, 'content') else str(response)
        except Exception as e:
            return f"Error: {e}"
    
    async def optimize_code(self, code: str) -> str:
        if not self.llm:
            return code
        
        prompt = f"""Optimize this PySpark code for Databricks:
```python
{code}
```
Return optimized code only."""

        try:
            response = await self.llm.ainvoke(prompt)
            return response.content if hasattr(response, 'content') else code
        except:
            return code
    
    def is_available(self) -> bool:
        return self.llm is not None
    
    def get_provider_info(self) -> Dict[str, Any]:
        return {
            "available": self.is_available(),
            "provider": self.provider,
            "message": f"Using {self.provider}" if self.llm else "No LLM configured"
        }
