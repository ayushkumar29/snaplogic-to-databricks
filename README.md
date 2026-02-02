# SnapLogic to Databricks Converter

A powerful web-based tool that converts SnapLogic pipelines (.slp files) into Databricks PySpark code, with AI assistance for complex transformations.

## Features

- 🔄 **Multi-format SLP Parsing** - Handles various SnapLogic export formats
- 📊 **Dependency Graph** - Automatically resolves parent/child pipeline relationships
- ⚡ **40+ Snap Handlers** - Built-in converters for common snap types
- 🤖 **AI-Powered** - Uses OpenAI or Anthropic for unknown snaps
- 🎨 **Modern UI** - Drag-and-drop interface with syntax highlighting
- 💬 **AI Chat Assistant** - Ask questions about your conversion

## Quick Start

### 1. Install Dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 2. Configure API Key (Optional - for AI features)

```bash
# Copy the example env file
cp .env.example .env

# Edit .env and add your API key
# OPENAI_API_KEY=sk-...
# OR
# ANTHROPIC_API_KEY=sk-ant-...
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
│   │   ├── api/          # FastAPI routes
│   │   ├── engine/       # Core conversion logic
│   │   │   ├── parser.py     # SLP file parser
│   │   │   ├── graph.py      # Dependency resolver
│   │   │   └── generator.py  # PySpark generator
│   │   └── llm/          # AI integration
│   │       └── agent.py      # LLM client
│   ├── static/           # Frontend files
│   │   ├── index.html
│   │   ├── styles.css
│   │   └── app.js
│   ├── main.py           # FastAPI app
│   └── requirements.txt
└── data/                 # Sample files & uploads
```

## Supported Snap Types

| Category | Snaps |
|----------|-------|
| **Read** | CSV, JSON, Parquet, XML, JDBC, S3, Azure Blob |
| **Write** | CSV, JSON, Parquet, JDBC, S3, Azure Blob |
| **Transform** | Mapper, Filter, Sort, Aggregate, Join, Union |
| **Flow** | Pipeline Execute, Router, Gate, Merge |

## AI Features

When the converter encounters an unknown snap type:
1. It marks the snap for AI resolution
2. The AI assistant generates equivalent PySpark code
3. You can ask follow-up questions in the chat panel

## License

MIT
