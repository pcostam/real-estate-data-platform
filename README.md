# real-estate-data-platform

An MCP server exposing Portuguese housing and economic indicators from
[INE](https://www.ine.pt) (Instituto Nacional de Estatistica), backed by a
PySpark ingestion layer.

![Architecture overview](assets/architecture_diagram.svg)

## What's included

### Verified indicators

| Name | varcd | Notes |
| --- | --- | --- |
| `median_price_per_m2` | `0012239` | Median sale price, EUR/m2, quarterly, by NUTS region |
| `number_of_sales` | `0014363` | Housing sales, trailing 12 months, quarterly, by NUTS region |
| `housing_price_index` (IPHab) | `0014765` | Base 2025, quarterly, Portugal only (no regional breakdown); `dim_3='H1'` for the headline Total |
| `consumer_price_index` (IPC) | `0014640` | Base 2025, monthly level index (for deflating nominal prices); defaults to `dim_3='T'` (headline), pass `dim3='001'` for Total exceto habitacao |

See [ingestion/indicators.py](ingestion/indicators.py) for the full registry
(descriptions, frequency, dimension notes).

Three of the four have a `dim_3` breakdown dimension (buyer tax residence,
dwelling category, consumption aggregate); `number_of_sales` has none. Tools
filter to each indicator's registered "Total" `dim_3` by default so results
aren't silently mixed across categories - pass `dim3` explicitly to see a
specific breakdown instead. See
[Dimensions and categories](#dimensions-and-categories) for what each code
means.

### Tools

| Tool | Purpose |
| --- | --- |
| `list_known_indicators` | See the registry above, with verified flags |
| `get_indicator(varcd, region, dim3, start_year, end_year)` | Generic fetch for any INE indicator by code |
| `get_indicator_raw(varcd)` | Debug: raw unparsed JSON from INE |
| `get_median_price_per_m2(region, start_year, end_year)` | Price data, filterable |
| `get_number_of_sales(region, start_year, end_year)` | Sales data, filterable |
| `get_yoy_change(indicator, region)` | Year-over-year % change |
| `compare_regions(indicator, regions, period)` | Side-by-side region comparison |
| `compute_price_to_income(price_varcd_or_name, income_varcd, region, dwelling_size_m2)` | Affordability ratio - you supply an income indicator's varcd |
| `get_buyer_origin_premium(region, start_year, end_year)` | % premium foreign buyers pay over national buyers on median price per m2, per period. A price-gap proxy, not a foreign-buyer share - INE publishes no sales volume split by buyer origin |
| `get_listing_price_context(html_path, start_year, end_year)` | Parse a saved idealista listing page and return its location plus the municipality's median price per m2 (INE has no parish-level prices) |

### Resources

- `ine://indicators` - the known-indicator registry as JSON
- `ine://indicator/{varcd}` - any indicator's full parsed series as JSON

### A note on INE's API and historical data

INE's `pindica.jsp` endpoint returns exactly one period per call - either
the latest, or a specific one selected via a `Dim1` period code. There's no
single call that returns a full time series. So:

- `get_indicator`/`get_median_price_per_m2`/`get_number_of_sales` with no
  `start_year`/`end_year` do one fetch (latest period only).
- With a year range, `ine_client.py` looks up the matching period codes via
  INE's metadata endpoint and fetches each one individually (capped at 60
  periods per call to keep latency bounded - narrow the range if you hit
  that).
- `get_yoy_change` does two fetches: the latest period, then the matching
  period exactly one year earlier (resolved the same way).

### Dimensions and categories

Every INE indicator is a grid: each value sits at one category of each
dimension. Pick one category per dimension and you get exactly one number.

- `dim 1` - period (quarter or month)
- `dim 2` - geography
- `dim 3` - an indicator-specific breakdown (not every indicator has one)

| varcd | dim 1 | dim 2 | dim 3 |
| --- | --- | --- | --- |
| `0012239` median price/m2 | Quarter, 2019 Q1 onwards | NUTS 2024, 97 areas (`PT` Portugal -> `1` Continente -> `11` Norte -> `112` Cavado -> `1120303` Braga) | Buyer's tax residence (domicilio fiscal): `T` Total, `1` Territorio nacional, `2` Estrangeiro (`21` Uniao Europeia, `22` Restantes paises) |
| `0014363` number of sales | Quarter, 2019 Q4 onwards | NUTS 2024, 931 areas (down to every municipality) | none |
| `0014765` IPHab | Quarter, 2009 Q1 onwards | `PT` only | Dwelling category: `H1` Total, `H11` Novos, `H12` Existentes |
| `0014640` IPC | **Month**, 1948 onwards | `PT` Portugal, `1` Continente | Special aggregates: `T` Total, `001` Total exceto habitacao, `002`-`007` exclusions/food/energy components, `008` Bens, `009` Servicos |

Things to watch when working with these:

- **Categories are hierarchical - don't sum across levels.** In `0012239`,
  `T` already contains `1` and `2`, and `2` contains `21` and `22`.
  Geography nests the same way (Cavado is inside Norte, inside Continente).
  The metadata's `categ_nivel` gives each category's level; compare places
  at the same level.
- **Codes are per indicator.** `T` is the total in `0012239` and `0014640`,
  but `H1` in `0014765`. Always check the metadata.
- **Buyer tax residence is not nationality.** A foreign national living in
  Portugal counts as `1`; a Portuguese emigrant buying from abroad counts as
  `2`.
- **A blank `valor` means not published**, not zero (e.g. foreign-buyer rows
  in smaller regions).
- **Coverage differs.** IPHab is national only and IPC is monthly, so neither
  lines up directly with a regional quarterly series.

#### Looking up dimensions yourself

The metadata endpoint `pindicaMeta.jsp` describes any indicator. In
PowerShell:

```powershell
$m = (Invoke-RestMethod 'https://www.ine.pt/ine/json_indicador/pindicaMeta.jsp?varcd=0012239&lang=PT')[0]
$m.Dimensoes.Descricao_Dim   # what each dimension is
$m.Dimensoes.Categoria_Dim[0].PSObject.Properties | Where-Object Name -like 'Dim_Num3_*' | ForEach-Object { $_.Value } | Select-Object cat_id, categ_dsg, categ_nivel   # categories of dim 3 (change the 3 for others)
```

`$m.UnidadeMedida`, `$m.PrimeiroPeriodo` and `$m.UltimoPeriodo` give the
unit and the available range.

Then pass one `cat_id` per dimension as `Dim1`/`Dim2`/`Dim3` to `pindica.jsp`
(omit one to get every category of it):

```powershell
(Invoke-RestMethod 'https://www.ine.pt/ine/json_indicador/pindica.jsp?op=2&varcd=0012239&Dim1=S5A20261&Dim2=112&Dim3=T&lang=PT')[0].Dados.PSObject.Properties.Value | Select-Object geodsg, dim_3, valor
```

Quote the URL - PowerShell treats an unquoted `&` as an operator. Results
are keyed by period label under `Dados` (e.g. `1.º Trimestre de 2026`),
hence the `.PSObject.Properties`.

## Prerequisites

- Python 3.11+
- A JDK (Java 17+) - PySpark runs on the JVM and won't start without one.
  Set `JAVA_HOME` to point at it.

### Windows-specific setup

PySpark's Hadoop layer expects a Windows-native `winutils.exe`/`hadoop.dll`
even though this project never touches HDFS. Without it, `SparkSession`
creation fails (or hangs) with errors like
`Could not locate executable null\bin\winutils.exe`.

1. Install a JDK (e.g. [Eclipse Temurin](https://adoptium.net/)) and set
   `JAVA_HOME` to its install directory, e.g.:

   ```powershell
   setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot"
   ```

2. Download a `winutils.exe` build matching your Hadoop version (the one
   bundled with the pinned `pyspark==4.2.0` - search
   `winutils <hadoop-version>`, e.g. from
   [cdarlint/winutils](https://github.com/cdarlint/winutils)) and place it
   (plus `hadoop.dll`) under `C:\hadoop\bin\`.
3. Set `HADOOP_HOME` to that folder and add it to `PATH`:

   ```powershell
   setx HADOOP_HOME "C:\hadoop"
   setx PATH "%PATH%;%HADOOP_HOME%\bin"
   ```

4. Open a **new** terminal (env vars set via `setx` only apply to new
   sessions) before running the server or tests.

## Running the server

```bash
pip install -r requirements.txt
python entrypoints/mcp_server.py
```

### Using it from Claude Code

This repo's [.mcp.json](.mcp.json) is checked in (project scope), so cloning
the repo is enough for Claude Code to pick up the `ine-housing-data` server
automatically - no manual `claude mcp add` needed. It points at
`.venv/Scripts/python.exe` (a relative path, resolved from the repo root), so
create the venv there first:

```bash
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

On macOS/Linux, create the venv the same way but update `.mcp.json`'s
`command` to `.venv/bin/python` instead (the `Scripts/python.exe` path is
Windows-specific).

Or, for local development with the MCP inspector:

```bash
mcp dev entrypoints/mcp_server.py
```

To use it from Claude Desktop, add to its MCP server config:

```json
{
  "mcpServers": {
    "ine-housing-data": {
      "command": "python",
      "args": ["entrypoints/mcp_server.py"],
      "cwd": "/absolute/path/to/real-estate-data-platform"
    }
  }
}
```

Note: the server reuses the PySpark `INEClient`, and each fetch produces a
Spark DataFrame that's collected to plain JSON at the tool boundary. A
`SparkSession` is started lazily on first use (not at import), but that
first call will still take a few seconds for the JVM to come up.

## Testing the API/MCP server locally

There's no HTTP endpoint to `curl` - this is a stdio-based MCP server - so
"calling it" means one of the following, in increasing order of how much of
the stack you exercise:

1. **Call `INEClient` directly** - skips Spark and the MCP layer entirely,
   fastest way to check the raw INE API response:

   ```bash
   python -c "from ingestion.ine_client import INEClient; print(INEClient().download('median_price_per_m2'))"
   ```

2. **Run an entrypoint script** - exercises the full `to_dataframe` path
   (Spark included) via [entrypoints/main.py](entrypoints/main.py):

   ```bash
   python entrypoints/main.py
   ```

   This needs the project importable as a package (see Setup below) since
   it does `from ingestion.ine_client import INEClient`.

3. **Call an MCP tool function directly in Python** - bypasses the MCP
   protocol but exercises the same code the server calls:

   ```bash
   python -c "from entrypoints.mcp_server import get_indicator_raw; print(get_indicator_raw('0012239'))"
   ```

4. **Run the real MCP server through the Inspector** - the closest thing to
   how Claude or another MCP client actually calls it, over the real stdio
   transport:

   ```bash
   mcp dev entrypoints/mcp_server.py
   ```

   Opens a local web UI where you can invoke any tool/resource by name.

### Setup for options 2 and 3

`ingestion`/`entrypoints` aren't on `sys.path` by default when a script is
run directly (Python only adds the script's own directory, not the project
root). Install the project in editable mode once so imports resolve
regardless of your working directory or how the script is invoked:

```bash
pip install -e . --no-deps
```

## Example prompts

Once the server is connected in Claude (Desktop or Code), here are prompts
that put these tools to use - e.g. pasting a saved listing page or URL
alongside a question:

**Listing evaluation**
- "Here's another listing [paste HTML/URL] - is the asking price reasonable
  for [neighborhood]?"
- "Compare these two listings I'm considering - which is the better deal
  per m²?"
- "This listing has been on the market for 6 months with no price drop - is
  that normal for this area?"

**Market trend queries**
- "How has the price per m² in [Cascais/Porto/Setubal] changed over the
  last 3 years?"
- "Which municipalities in the Lisbon metro area have seen the fastest
  price growth in the last year?"
- "Is Odivelas overheated compared to neighboring municipalities, or still
  catching up?"
- "Show me the number of sales trend for [region] - is demand rising or
  falling?"
- "Is [municipality] seeing strong demand from foreign buyers?" (answered
  via the foreign-vs-national price premium, not buyer counts; "foreign"
  means tax-resident abroad, not foreign nationality)

**Affordability / investment framing**
- "If I make €X/year, what price-to-income ratio would a €Y property in
  [region] represent?"
- "At current appreciation rates, what might median price/m² in [region]
  look like in 2 years?"

**Negotiation prep**
- "Given INE data, what's a defensible opening counter-offer for a
  property needing [light/full] renovation?"
- "How much has this specific building's neighborhood repriced since the
  property was likely bought by the current owner (assume ~2018
  purchase)?"

**Raw data / verification**
- "Show me the raw INE API response for [indicator] in [region], no
  formatting"
- "List all known indicators this tool can query"

## Adding more indicators

You don't need to touch code to pull a new INE series - just find its
`varcd`:

1. ine.pt -> Bases de Dados
2. Search for what you want (e.g. "rendas da habitacao", "custos de
   construcao", "ganho medio mensal de trabalho")
3. Open it -> Alterar condicoes de selecao -> switch "Arvore" to "Codigos"
   -> note the `varcd` (and `Dim1`/`Dim2`/`Dim3` codes if you want to filter
   further)
4. Call `get_indicator("that_varcd")` directly, or add a named entry to
   `KNOWN_INDICATORS` in [ingestion/indicators.py](ingestion/indicators.py)
   so it shows up in `list_known_indicators` and can be referenced by name
   in the other tools.

For a mortgage-rate or Euribor series, note that INE isn't the source -
that data comes from Banco de Portugal. A second client would be needed to
wire that up in the same server.
