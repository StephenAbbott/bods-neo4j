# BODS-Neo4j

Bidirectional converter between the [Beneficial Ownership Data Standard (BODS) v0.4](https://standard.openownership.org/en/0.4.0/) and [Neo4j](https://neo4j.com/) graph database, with built-in graph analysis queries for UBO detection, corporate group mapping, and circular ownership detection.

Part of the [BODS Interoperability Toolkit](https://github.com/StephenAbbott/bods-interoperability-toolkit).

## Why?

**BODS** provides a universal, standardised format for beneficial ownership data — enabling interoperability across countries, registers, and data sources. **Neo4j** provides powerful graph traversal and analysis capabilities that are essential for understanding complex ownership structures.

This tool bridges the gap:

- **BODS → Neo4j**: Import any BODS v0.4 dataset into Neo4j for graph analysis, UBO detection, and visualisation
- **Neo4j → BODS**: Export graph data back to BODS format for standardised data exchange and sharing
- **Graph Queries**: Ready-made Cypher queries for common beneficial ownership analysis tasks

## Background

The [Beneficial Ownership Data Standard](https://standard.openownership.org/) was created by [Open Ownership](https://www.openownership.org/) as the world's leading open standard for beneficial ownership information on legal persons and legal arrangements.

BODS data is published by multiple sources including:
- [BODS Data Explorer](https://bods-data.openownership.org/) — GLEIF and other datasets in BODS format
- [UK PSC Pipeline](https://github.com/openownership/bods-uk-psc-pipeline) — UK Companies House data
- [GLEIF Pipeline](https://github.com/openownership/bods-gleif-pipeline) — Legal Entity Identifiers
- [OpenCorporates Pipeline](https://github.com/StephenAbbott/bods-opencorporates) — OpenCorporates relationship data
- [Kyckr Pipeline](https://github.com/StephenAbbott/bods-kyckr) — Kyckr UBO verification data
- [ICIJ Offshore Leaks Pipeline](https://github.com/StephenAbbott/bods-icij-offshoreleaks) — Offshore Leaks Database

## Installation

```bash
pip install -e .
```

For development:
```bash
pip install -e ".[dev]"
```

## Quick Start

### 1. Start Neo4j

```bash
docker compose up -d
```

This starts Neo4j Community Edition on `bolt://localhost:7687` with APOC plugin.

### 2. BODS → Neo4j (CSV export)

Generate Neo4j-importable CSV files and import scripts:

```bash
bods-neo4j to-csv examples/sample_data/sample_bods.json -o ./neo4j_export
```

This produces:
- `entities.csv`, `persons.csv`, `relationships.csv`
- `import.cypher` — for loading into a running Neo4j instance
- `import.sh` — for `neo4j-admin` bulk import

### 3. BODS → Neo4j (direct driver load)

Load BODS data directly into Neo4j:

```bash
bods-neo4j to-neo4j examples/sample_data/sample_bods.json \
    --uri bolt://localhost:7687 \
    --username neo4j \
    --password bodspassword
```

### 4. Neo4j → BODS

Export the graph back to BODS format:

```bash
bods-neo4j to-bods output.jsonl \
    --uri bolt://localhost:7687 \
    --username neo4j \
    --password bodspassword \
    --publisher-name "My Organisation"
```

### 5. Inspect data

```bash
# BODS file statistics
bods-neo4j info examples/sample_data/sample_bods.json

# Neo4j graph statistics
bods-neo4j graph-info --uri bolt://localhost:7687 --username neo4j --password bodspassword
```

## Neo4j Graph Schema

BODS statements map to Neo4j's property graph model as follows:

### Nodes

| BODS Statement | Neo4j Label(s) | Key Properties |
|---|---|---|
| Entity (registeredEntity) | `:Entity:RegisteredEntity` | name, recordId, jurisdictionCode, entityType |
| Entity (trust) | `:Entity:Arrangement:Trust` | name, recordId |
| Entity (stateBody) | `:Entity:StateBody` | name, recordId |
| Entity (nomination) | `:Entity:Arrangement:Nomination` | name, recordId |
| Person (knownPerson) | `:Person` | name, recordId, birthDate, nationalityCode |

### Relationships

| BODS Concept | Neo4j Relationship | Direction | Key Properties |
|---|---|---|---|
| Ownership/Control interest | `[:HAS_INTEREST]` | `(interestedParty)-[:HAS_INTEREST]->(subject)` | interestTypes, shareMinimum, shareMaximum, isBeneficialOwnership |

### Round-Trip Fidelity

All BODS metadata is preserved as node/relationship properties:
- Complex nested structures (identifiers, addresses, interests arrays) are stored as `*_json` properties
- Statement IDs, record IDs, publication details, and source information are preserved
- Data can be converted back to valid BODS v0.4 format without loss

## Graph Analysis Queries

### Python API

```python
from bods_neo4j.config import Neo4jConfig
from bods_neo4j.graph_queries.ubo_detection import find_owners, find_all_ubos
from bods_neo4j.graph_queries.corporate_groups import find_corporate_group, find_top_level_parents
from bods_neo4j.graph_queries.circular_ownership import find_circular_ownership

config = Neo4jConfig(uri="bolt://localhost:7687", username="neo4j", password="bodspassword")

# Find all owners of an entity
owners = find_owners("rec-entity-alpha", config)

# Find all UBOs with >= 25% effective ownership
ubos = find_all_ubos(config, threshold=25.0)

# Map a corporate group
group = find_corporate_group("rec-entity-alpha", config)

# Find top-level parent entities
parents = find_top_level_parents(config, limit=50)

# Detect circular ownership
cycles = find_circular_ownership(config)
```

### Key Cypher Queries

**Find all owners of an entity:**
```cypher
MATCH path = (owner)-[:HAS_INTEREST*1..10]->(target:Entity {recordId: "rec-entity-alpha"})
WHERE owner:Person OR (owner:Entity AND NOT EXISTS {
    MATCH (upstream)-[:HAS_INTEREST]->(owner)
})
RETURN owner.name, length(path) AS depth
```

**Calculate effective ownership through chains:**
```cypher
MATCH path = (person:Person)-[:HAS_INTEREST*1..10]->(entity:Entity)
WITH person, entity, path,
     reduce(pct = 1.0, r IN relationships(path) |
         CASE WHEN r.shareMinimum IS NOT NULL
              THEN pct * (toFloat(r.shareMinimum) / 100.0)
              ELSE pct END) * 100.0 AS effectivePct
WHERE effectivePct >= 25
RETURN person.name, entity.name, effectivePct
```

**Detect circular ownership:**
```cypher
MATCH path = (e:Entity)-[:HAS_INTEREST*2..10]->(e)
RETURN e.name, length(path) AS cycleLength,
       [n IN nodes(path) | n.name] AS cycleNames
```

**Find entities without identified UBOs:**
```cypher
MATCH (e:Entity)
WHERE NOT EXISTS { MATCH (p:Person)-[:HAS_INTEREST*]->(e) }
AND EXISTS { MATCH ()-[:HAS_INTEREST]->(e) }
RETURN e.name, e.jurisdictionCode
```

## Comparison with Other Approaches

This tool was designed by comparing two approaches to modelling UK beneficial ownership data:

| Approach | Open Ownership ([bods-uk-psc-pipeline](https://github.com/openownership/bods-uk-psc-pipeline)) | Neo4j Team ([neo4j-company-house-demo](https://github.com/erikbijl/neo4j-company-house-demo)) |
|---|---|---|
| **Output** | Standardised BODS v0.4 JSON | Custom Neo4j property graph |
| **Strength** | Interoperability across data sources | Graph traversal and UBO analysis |
| **Weakness** | No graph analysis built in | Source-specific, not interoperable |

**BODS-Neo4j bridges this gap** — standardised data format with graph analysis capabilities.

## Testing

```bash
pytest
```

Tests cover:
- BODS file reading (JSON and JSONL)
- Statement mapping (entities, persons, relationships)
- CSV export with correct structure
- Round-trip fidelity (BODS → Neo4j → BODS)
- BODS schema utilities

### Conformance against the shared BODS v0.4 fixture pack

`tests/test_bods_fixtures_conformance.py` runs the mapper against every case in the canonical [**bods-v04-fixtures**](https://pypi.org/project/bods-v04-fixtures/) pack via the [**pytest-bods-v04-fixtures**](https://pypi.org/project/pytest-bods-v04-fixtures/) plugin. Tests are parametrized by fixture name so a failure like `[edge-cases/10-circular-ownership]` points straight at the offending case.

Graph-specific conformance checks include: every statement maps to a node or relationship (no silent `None` returns from shape divergence); circular ownership emits two distinct mirrored HAS_INTEREST edges; and declared-unknown UBOs (inline `unspecifiedReason`) don't crash the mapper and still leave the known subject entity as a usable node.

## Project Structure

```
src/bods_neo4j/
├── cli.py                          # Click CLI commands
├── config.py                       # Configuration dataclasses
├── bods_to_neo4j/
│   ├── reader.py                   # BODS JSON/JSONL streaming reader
│   ├── mapper.py                   # BODS statements → Neo4j nodes/relationships
│   ├── csv_exporter.py             # CSV export + Cypher/admin import scripts
│   └── driver_loader.py            # Direct Neo4j loading via Python driver
├── neo4j_to_bods/
│   ├── extractor.py                # Query Neo4j graph
│   ├── mapper.py                   # Neo4j nodes/rels → BODS statements
│   └── writer.py                   # Output BODS JSON/JSONL
├── graph_queries/
│   ├── ubo_detection.py            # Ultimate beneficial owner traversal
│   ├── corporate_groups.py         # Corporate group mapping and metrics
│   └── circular_ownership.py       # Cycle detection
└── utils/
    ├── bods_schema.py              # BODS v0.4 constants and helpers
    └── neo4j_helpers.py            # Neo4j connection and batch operations
```

## License

MIT
