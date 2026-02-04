# SnapLogic to Databricks Converter

A web-based tool that converts SnapLogic pipelines (.slp files) into Databricks PySpark code, with AI assistance for complex transformations.

## Features

- Multi-format SLP parsing for various SnapLogic export formats
- Dependency graph resolution for parent/child pipeline relationships
- Built-in converters for 40+ common snap types
- AI-powered conversion for unknown snaps (OpenAI, Anthropic, Groq, Ollama)
- Modern drag-and-drop interface with syntax highlighting
- AI chat assistant for conversion questions

## Quick Start

### 1. Install Dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 2. Configure API Key (Optional - for AI features)

```bash
cp .env.example .env
# Edit .env and add your API key
# GROQ_API_KEY=your_key (recommended - free)
# OPENAI_API_KEY=your_key
# ANTHROPIC_API_KEY=your_key
```

### 3. Run the Server

```bash
cd backend
python main.py
```

### 4. Open the Web UI

Navigate to: **http://localhost:8000**

## Usage

1. **Upload** - Drag and drop your `.slp` files into the upload zone
2. **Review** - Check for any missing child pipeline dependencies
3. **Convert** - Click "Convert to Databricks" to generate PySpark code
4. **Download** - Copy or download your converted code

## Project Structure

```
snaplogic-to-databricks/
├── backend/
│   ├── app/
│   │   ├── api/              # FastAPI routes
│   │   ├── engine/           # Core conversion logic
│   │   │   ├── parser.py     # SLP file parser
│   │   │   ├── graph.py      # Dependency resolver
│   │   │   └── generator.py  # PySpark generator
│   │   └── llm/              # AI integration
│   │       └── agent.py      # LLM client
│   ├── static/               # Frontend files
│   ├── main.py               # FastAPI app
│   └── requirements.txt
└── data/                     # Sample files & uploads
```

## Supported Snap Types

| Category | Snaps |
|----------|-------|
| Read | CSV, JSON, Parquet, XML, JDBC, S3, Azure Blob |
| Write | CSV, JSON, Parquet, JDBC, S3, Azure Blob |
| Transform | Mapper, Filter, Sort, Aggregate, Join, Union |
| Flow | Pipeline Execute, Router, Gate, Merge |

## AI Features

When the converter encounters an unknown snap type:
1. It marks the snap for AI resolution
2. The AI assistant generates equivalent PySpark code
3. You can ask follow-up questions in the chat panel

## License

MIT
