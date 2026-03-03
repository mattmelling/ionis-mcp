# ionis-mcp

MCP server for IONIS HF propagation analytics — 175M signatures from WSPR, RBN, Contest, DXpedition, and PSK Reporter.

## What This Does

Wraps the [IONIS distributed datasets](https://sourceforge.net/p/ionis-ai) (SQLite) and exposes propagation analytics as [MCP](https://modelcontextprotocol.io) tools. Install this package, point it at your downloaded data, and Claude (Desktop or Code) can answer propagation questions directly.

**Example questions Claude can answer:**
- "When is 20m open from Idaho to Europe?"
- "How does SFI affect 15m propagation?"
- "Show me 10m paths at 03z where both stations are in the dark"
- "Compare WSPR and RBN observations on 20m FN31 to JO51"

## Install

```bash
pip install ionis-mcp
```

## Download Data

Download SQLite files from [SourceForge](https://sourceforge.net/p/ionis-ai). Minimum useful set (~430 MB):

- `contest_signatures.sqlite` — 25 years of CQ contest data
- `grid_lookup.sqlite` — Maidenhead grid coordinates
- `solar_indices.sqlite` — SFI, SSN, Kp from 2000-2026

Full download is ~15 GB across 9 SQLite files covering 175M+ signatures.

## Configure

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "ionis": {
      "command": "ionis-mcp",
      "env": {
        "IONIS_DATA_DIR": "/path/to/ionis-ai-datasets/v1.0"
      }
    }
  }
}
```

### Claude Code

Add to `.claude/settings.json`:

```json
{
  "mcpServers": {
    "ionis": {
      "command": "ionis-mcp",
      "env": {
        "IONIS_DATA_DIR": "/path/to/ionis-ai-datasets/v1.0"
      }
    }
  }
}
```

Restart Claude. Tools appear automatically.

## Tools (10)

| Tool | Purpose |
|------|---------|
| `list_datasets` | Show available datasets and stats |
| `query_signatures` | Flexible signature lookup with filters |
| `band_openings` | When does a band open for a specific path? |
| `path_analysis` | Complete analysis across all bands and hours |
| `solar_correlation` | How does SFI affect a band or path? |
| `grid_info` | Decode grid to lat/lon, compute solar elevation |
| `compare_sources` | Cross-dataset comparison for same path |
| `dark_hour_analysis` | Classify paths by solar geometry |
| `solar_conditions` | Historical solar indices (SFI, SSN, Kp) |
| `band_summary` | Overview of a band across all data |

## Data Directory Layout

```
$IONIS_DATA_DIR/
├── propagation/
│   ├── wspr-signatures/wspr_signatures_v2.sqlite
│   ├── rbn-signatures/rbn_signatures.sqlite
│   ├── contest-signatures/contest_signatures.sqlite
│   ├── dxpedition-signatures/dxpedition_signatures.sqlite
│   └── pskr-signatures/pskr_signatures.sqlite
├── solar/
│   ├── solar-indices/solar_indices.sqlite
│   └── dscovr/dscovr_l1.sqlite
└── tools/
    ├── grid-lookup/grid_lookup.sqlite
    └── balloon-callsigns/balloon_callsigns_v2.sqlite
```

The server works with whatever datasets are present. Missing datasets degrade gracefully.

## Testing with MCP Inspector

```bash
ionis-mcp --transport streamable-http --port 8000
# Open http://localhost:8000/mcp in browser
```

## License

GPL-3.0-or-later
