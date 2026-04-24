# 🔄 Zero-Code ETL Framework

> A metadata-driven ETL framework where **pipelines are configuration, not code**.  
> Built for reuse across the entire data team — no Python knowledge required to create a new pipeline.

---

## 🗣️ In plain English — what does this actually do?

Imagine you work at a store and every day you have to do the same tedious task: grab the sales data from a spreadsheet, calculate totals, remove duplicate entries, and save everything in a tidy report. Today, someone has to open a terminal, run a script, and hope nothing breaks — and if the boss wants to change anything, you need to call a developer again.

**This project lets anyone on the team — even without knowing how to code — create that process themselves**, just by writing a simple text file, like a recipe:

> *"Grab data from here, remove duplicates, convert this field to a number, and save it there."*

The program reads the recipe and runs everything automatically.

Two details make it stand out: it keeps a diary of every run — how many records were processed, how long it took, whether anything went wrong — so if something breaks, you know exactly where and when. And it was built to be reused: the whole team shares the same tool for completely different processes, just swapping the recipe.

> **Is it like an AI agent?** Not quite. An AI agent understands natural language and improvises. This tool does exactly what the recipe says, every time, without interpreting anything — like a washing machine. You pick the programme, press the button, and it always does the same thing the same way. That predictability is the point: when processing financial data daily, you need the process to be identical every run, auditable, and reliable.

---

## 🗣️ Em português — o que esse projeto faz, sem enrolação?

Imagina que você trabalha numa loja e precisa, todo dia, fazer a mesma tarefa chata: pegar os dados de vendas de uma planilha, calcular o total de cada produto, remover os itens duplicados e salvar tudo num relatório organizado. Hoje, alguém precisa abrir o terminal, rodar um script, torcer pra não dar erro — e se o chefe quiser mudar qualquer coisa, precisa chamar o programador de novo.

**Esse projeto deixa qualquer pessoa da equipe — mesmo sem saber programar — criar esse processo sozinha**, só escrevendo um arquivo de texto simples, tipo uma receita de bolo:

> *"Busca os dados daqui, remove os duplicados, converte esse campo pra número e salva lá."*

O programa lê essa receita e executa tudo automaticamente.

Dois detalhes fazem diferença: ele anota tudo que acontece — quantos registros foram processados, quanto tempo levou, se deu algum erro — então se algo quebrar, dá pra saber exatamente onde e quando. E foi construído pra ser reutilizado: a equipe inteira usa a mesma ferramenta pra processos completamente diferentes, só trocando a receita.

> **É como um agente de IA?** Não exatamente. Um agente de IA entende linguagem natural e improvisa. Essa ferramenta faz exatamente o que a receita manda, sempre, sem interpretar nada — como uma máquina de lavar. Você escolhe o programa, aperta o botão, e ela faz sempre a mesma coisa do mesmo jeito. Essa previsibilidade é justamente o ponto: quando você processa dados financeiros todo dia, precisa que o processo seja idêntico em toda execução, auditável e confiável.

---

## ✨ Highlights

| Feature | Details |
|---|---|
| **Zero-code pipelines** | Define extract → transform → load in YAML or JSON |
| **Pluggable architecture** | Add custom sources/destinations in < 15 lines |
| **Metadata tracking** | Every run logged to DuckDB (rows, timing, status) |
| **Chainable transforms** | 9 built-in transformations, composable in any order |
| **Multiple sources** | REST API (with pagination), CSV, JSON, SQLite, DuckDB, PostgreSQL |
| **Multiple destinations** | DuckDB (Data Lake), SQLite, CSV, JSON, PostgreSQL |

---

## 🚀 Quickstart

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the sample pipeline (pulls from a public REST API)
python run_pipeline.py configs/api_users.yaml

# 3. Check run history
python run_pipeline.py --history
```

---

## 📁 Project Structure

```
etl_framework/
├── etl_core/
│   ├── engine.py         # Orchestrator: reads config → extract → transform → load
│   ├── extractors.py     # Source adapters (REST API, CSV, SQL, …)
│   ├── transformers.py   # Transformation pipeline (rename, cast, filter, …)
│   ├── loaders.py        # Destination adapters (DuckDB, SQLite, CSV, …)
│   └── metadata.py       # Run tracking & audit log
├── configs/
│   ├── api_users.yaml    # Example: REST API → DuckDB
│   ├── sales_csv.yaml    # Example: CSV → DuckDB
│   └── posts_to_sqlite.json  # Example: REST API → SQLite (JSON config)
├── examples/
│   └── sample_sales.csv
├── tests/
│   └── test_etl.py       # Full test suite (pytest)
├── run_pipeline.py        # CLI entry point
└── requirements.txt
```

---

## ⚙️ Pipeline Config Reference

A pipeline config is a YAML or JSON file with three sections:

```yaml
pipeline_name: my_pipeline   # unique identifier

source:        # WHERE to extract from
  type: rest_api
  ...

transformations:  # WHAT to transform (optional, ordered)
  - type: rename_columns
    ...

destination:   # WHERE to load to
  type: duckdb
  ...
```

---

## 📥 Sources

### `rest_api`

```yaml
source:
  type: rest_api
  url: https://api.example.com/orders
  method: GET                          # GET | POST
  headers:
    Authorization: "Bearer TOKEN"
  params:
    status: active
  root_key: data.orders               # dotted path into response
  pagination:
    type: offset
    page_param: page
    limit_param: per_page
    limit: 100
    max_pages: 50
```

### `csv`

```yaml
source:
  type: csv
  path: data/sales.csv
  delimiter: ","
  encoding: utf-8
```

### `sqlite`

```yaml
source:
  type: sqlite
  database: warehouse.db
  query: "SELECT * FROM orders WHERE status = 'completed'"
```

### `duckdb`

```yaml
source:
  type: duckdb
  database: data_lake.duckdb
  query: "SELECT * FROM raw.events WHERE dt >= '2024-01-01'"
```

### `postgres`

```yaml
source:
  type: postgres
  connection_string: "postgresql://user:pass@host:5432/db"
  query: "SELECT id, name, amount FROM public.transactions"
```

---

## 🔄 Transformations

Transformations are applied **in order**. Chain as many as needed.

### `rename_columns`
```yaml
- type: rename_columns
  mapping:
    old_col_name: new_col_name
    userId: user_id
```

### `cast_types`
```yaml
- type: cast_types
  columns:
    price: float
    quantity: int
    is_active: bool
```

### `filter_rows`
```yaml
- type: filter_rows
  condition: "float(price) > 0 and str(status) == 'active'"
```

### `add_column`
```yaml
- type: add_column
  name: total
  expression: "round(float(price) * int(quantity), 2)"
```

### `drop_columns`
```yaml
- type: drop_columns
  columns: [internal_id, debug_flag, temp_col]
```

### `deduplicate`
```yaml
- type: deduplicate
  subset: [email]   # omit to deduplicate on all columns
```

### `fill_nulls`
```yaml
- type: fill_nulls
  columns:
    discount: 0.0
    region: "unknown"
```

### `normalize_text`
```yaml
- type: normalize_text
  columns: [name, city, category]
  operations: [strip, lower]   # strip | lower | upper | title
```

### `custom_python`
```yaml
- type: custom_python
  code: |
    for row in data:
        row['margin'] = round((row['revenue'] - row['cost']) / row['revenue'], 4)
```

---

## 📤 Destinations

### `duckdb` (recommended — Data Lake)

```yaml
destination:
  type: duckdb
  database: data_lake.duckdb
  table: orders
  schema: refined          # namespace/schema (default: main)
  write_mode: append       # append | replace
```

### `sqlite`

```yaml
destination:
  type: sqlite
  database: output/local.db
  table: orders
  write_mode: replace
```

### `csv`

```yaml
destination:
  type: csv
  path: output/orders_processed.csv
  write_mode: append
```

### `postgres`

```yaml
destination:
  type: postgres
  connection_string: "postgresql://user:pass@host:5432/db"
  table: public.orders
  write_mode: append
```

---

## 🔌 Extending the Framework

The framework is designed to be extended without modifying core code.

### Custom extractor

```python
from etl_core.extractors import BaseExtractor, ExtractorFactory

class S3Extractor(BaseExtractor):
    def extract(self) -> list[dict]:
        # your logic here
        return [...]

# Register once at startup
ExtractorFactory.register("s3", S3Extractor)
```

Then use it in any config:
```yaml
source:
  type: s3
  bucket: my-bucket
  key: raw/events.parquet
```

### Custom transformer / loader — same pattern.

---

## 📊 Metadata & Run History

Every run is automatically logged to a DuckDB metadata store:

```bash
# View all runs
python run_pipeline.py --history

# Filter by pipeline
python run_pipeline.py --history --pipeline sales_csv_pipeline
```

Output example:
```
Run    Pipeline                       Status     Rows     Time(s)   Started
-------------------------------------------------------------------------------------
3      sales_csv_pipeline             success    10       0.21      2024-11-15T14:32:01
2      jsonplaceholder_users          success    10       1.03      2024-11-15T14:30:45
1      jsonplaceholder_users          failed     -        0.05      2024-11-15T14:29:12
```

Query the metadata directly:
```python
import duckdb
conn = duckdb.connect("metadata.duckdb")
conn.execute("SELECT * FROM pipeline_runs ORDER BY run_id DESC").df()
```

---

## 🧪 Running Tests

```bash
pytest tests/ -v
pytest tests/ -v --cov=etl_core --cov-report=term-missing
```

---

## 🏛️ Design Decisions

**Why YAML/JSON configs?**  
Any team member (analyst, BI engineer, data engineer) can create a pipeline without writing Python. The framework reduces pipeline creation from hours to minutes.

**Why DuckDB as the Data Lake?**  
DuckDB is embedded (no server needed), supports SQL, and reads Parquet/CSV natively — ideal for a portable, team-shareable local Data Lake.

**Why the Factory pattern?**  
`ExtractorFactory`, `TransformerFactory`, and `LoaderFactory` decouple the config parser from the implementation. Adding a new connector never touches existing code (Open/Closed Principle).

**Why metadata tracking?**  
Observability is a production requirement. Every run captures timing, row counts, and errors — enabling data quality monitoring and SLA tracking without external tooling.

---

## 📄 License

MIT
