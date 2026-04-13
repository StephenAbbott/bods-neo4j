"""Export BODS data to Neo4j-importable CSV files with Cypher and admin import scripts.

Produces:
    - entities.csv: Entity nodes
    - persons.csv: Person nodes
    - relationships.csv: HAS_INTEREST relationships
    - import.cypher: LOAD CSV script for loading into a running Neo4j instance
    - import.sh: neo4j-admin bulk import script for initial database creation
"""

import csv
import json
import logging
from pathlib import Path
from typing import Union

from .mapper import map_statement
from .reader import read_bods_file

logger = logging.getLogger(__name__)

# CSV column definitions - these determine the output schema
ENTITY_COLUMNS = [
    "statementId",
    "recordId",
    "recordStatus",
    "declarationSubject",
    "statementDate",
    "name",
    "entityType",
    "entitySubtype",
    "entityTypeDetails",
    "isComponent",
    "jurisdictionName",
    "jurisdictionCode",
    "foundingDate",
    "dissolutionDate",
    "uri",
    "primaryIdentifierId",
    "primaryIdentifierScheme",
    "registeredAddress",
    "registeredPostCode",
    "registeredCountry",
    "publisherName",
    "publicationDate",
    "bodsVersion",
    "identifiers_json",
    "addresses_json",
    "alternateNames_json",
    "source_json",
    "annotations_json",
    "publicListing_json",
    "formedByStatute_json",
]

PERSON_COLUMNS = [
    "statementId",
    "recordId",
    "recordStatus",
    "declarationSubject",
    "statementDate",
    "name",
    "personType",
    "isComponent",
    "familyName",
    "givenName",
    "birthDate",
    "deathDate",
    "nationalityCode",
    "pepStatus",
    "unspecifiedReason",
    "unspecifiedDescription",
    "publisherName",
    "publicationDate",
    "bodsVersion",
    "names_json",
    "identifiers_json",
    "addresses_json",
    "nationalities_json",
    "taxResidencies_json",
    "placeOfBirth_json",
    "politicalExposure_json",
    "source_json",
    "annotations_json",
]

RELATIONSHIP_COLUMNS = [
    "statementId",
    "recordId",
    "recordStatus",
    "declarationSubject",
    "statementDate",
    "sourceRecordId",
    "targetRecordId",
    "isComponent",
    "isBeneficialOwnership",
    "interestTypes",
    "directOrIndirect",
    "shareExact",
    "shareMinimum",
    "shareMaximum",
    "interestStartDate",
    "interestEndDate",
    "publisherName",
    "publicationDate",
    "bodsVersion",
    "interests_json",
    "componentRecords_json",
    "subjectUnspecified_json",
    "interestedPartyUnspecified_json",
    "source_json",
    "annotations_json",
]


def export_to_csv(
    bods_file: Union[str, Path],
    output_dir: Union[str, Path] = "./neo4j_export",
) -> dict:
    """Export BODS data to Neo4j CSV files with import scripts.

    Args:
        bods_file: Path to BODS JSON or JSONL file
        output_dir: Directory for output files

    Returns:
        Dictionary with counts of exported entities, persons, and relationships
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    counts = {"entities": 0, "persons": 0, "relationships": 0, "skipped": 0}

    entities_path = output_dir / "entities.csv"
    persons_path = output_dir / "persons.csv"
    relationships_path = output_dir / "relationships.csv"

    with (
        open(entities_path, "w", newline="", encoding="utf-8") as ef,
        open(persons_path, "w", newline="", encoding="utf-8") as pf,
        open(relationships_path, "w", newline="", encoding="utf-8") as rf,
    ):
        entity_writer = csv.DictWriter(ef, fieldnames=ENTITY_COLUMNS, extrasaction="ignore")
        person_writer = csv.DictWriter(pf, fieldnames=PERSON_COLUMNS, extrasaction="ignore")
        rel_writer = csv.DictWriter(rf, fieldnames=RELATIONSHIP_COLUMNS, extrasaction="ignore")

        entity_writer.writeheader()
        person_writer.writeheader()
        rel_writer.writeheader()

        for statement in read_bods_file(bods_file):
            mapped = map_statement(statement)
            if mapped is None:
                counts["skipped"] += 1
                continue

            if mapped["type"] == "node":
                props = mapped["properties"]
                # Convert list values to JSON strings for CSV
                for key, value in props.items():
                    if isinstance(value, list):
                        props[key] = json.dumps(value)

                if "Entity" in mapped["labels"]:
                    # Add labels as a property for import
                    props["_labels"] = ";".join(mapped["labels"])
                    entity_writer.writerow(props)
                    counts["entities"] += 1
                elif "Person" in mapped["labels"]:
                    person_writer.writerow(props)
                    counts["persons"] += 1

            elif mapped["type"] == "relationship":
                props = mapped["properties"]
                props["sourceRecordId"] = mapped.get("source_record_id", "")
                props["targetRecordId"] = mapped.get("target_record_id", "")
                # Convert list values to JSON strings for CSV
                for key, value in props.items():
                    if isinstance(value, list):
                        props[key] = json.dumps(value)
                rel_writer.writerow(props)
                counts["relationships"] += 1

            total = counts["entities"] + counts["persons"] + counts["relationships"]
            if total % 100_000 == 0 and total > 0:
                logger.info(
                    "Exported %d entities, %d persons, %d relationships",
                    counts["entities"], counts["persons"], counts["relationships"],
                )

    # Generate import scripts
    _generate_cypher_script(output_dir)
    _generate_admin_import_script(output_dir)

    logger.info(
        "Export complete: %d entities, %d persons, %d relationships (%d skipped)",
        counts["entities"], counts["persons"], counts["relationships"], counts["skipped"],
    )

    return counts


def _generate_cypher_script(output_dir: Path):
    """Generate a Cypher LOAD CSV script for importing into a running Neo4j instance."""
    script = """\
// =============================================================================
// BODS Neo4j Import Script (LOAD CSV)
// Generated by bods-neo4j
//
// Usage: Run this in Neo4j Browser or via cypher-shell against a running instance.
// Place the CSV files in the Neo4j import directory or serve them via HTTP.
// =============================================================================

// --- Step 1: Create constraints and indexes ---

CREATE CONSTRAINT constraint_entity_record_id IF NOT EXISTS
FOR (e:Entity) REQUIRE e.recordId IS UNIQUE;

CREATE CONSTRAINT constraint_person_record_id IF NOT EXISTS
FOR (p:Person) REQUIRE p.recordId IS UNIQUE;

CREATE CONSTRAINT constraint_entity_statement_id IF NOT EXISTS
FOR (e:Entity) REQUIRE e.statementId IS UNIQUE;

CREATE CONSTRAINT constraint_person_statement_id IF NOT EXISTS
FOR (p:Person) REQUIRE p.statementId IS UNIQUE;

CREATE INDEX idx_entity_name IF NOT EXISTS FOR (e:Entity) ON (e.name);
CREATE INDEX idx_person_name IF NOT EXISTS FOR (p:Person) ON (p.name);
CREATE INDEX idx_entity_jurisdiction IF NOT EXISTS FOR (e:Entity) ON (e.jurisdictionCode);
CREATE INDEX idx_entity_type IF NOT EXISTS FOR (e:Entity) ON (e.entityType);

// --- Step 2: Load Entity nodes ---

LOAD CSV WITH HEADERS FROM 'file:///entities.csv' AS row
CALL {
    WITH row
    MERGE (e:Entity {recordId: row.recordId})
    SET e.statementId = row.statementId,
        e.statementDate = row.statementDate,
        e.recordStatus = row.recordStatus,
        e.declarationSubject = row.declarationSubject,
        e.name = row.name,
        e.entityType = row.entityType,
        e.entitySubtype = row.entitySubtype,
        e.entityTypeDetails = row.entityTypeDetails,
        e.isComponent = CASE row.isComponent WHEN 'True' THEN true ELSE false END,
        e.jurisdictionName = row.jurisdictionName,
        e.jurisdictionCode = row.jurisdictionCode,
        e.foundingDate = row.foundingDate,
        e.dissolutionDate = row.dissolutionDate,
        e.uri = row.uri,
        e.primaryIdentifierId = row.primaryIdentifierId,
        e.primaryIdentifierScheme = row.primaryIdentifierScheme,
        e.registeredAddress = row.registeredAddress,
        e.registeredPostCode = row.registeredPostCode,
        e.registeredCountry = row.registeredCountry,
        e.publisherName = row.publisherName,
        e.publicationDate = row.publicationDate,
        e.bodsVersion = row.bodsVersion,
        e.identifiers_json = row.identifiers_json,
        e.addresses_json = row.addresses_json,
        e.alternateNames_json = row.alternateNames_json,
        e.source_json = row.source_json,
        e.annotations_json = row.annotations_json
} IN TRANSACTIONS OF 10000 ROWS;

// --- Step 3: Add subtype labels to entities ---

MATCH (e:Entity) WHERE e.entityType = 'registeredEntity'
SET e:RegisteredEntity;

MATCH (e:Entity) WHERE e.entityType = 'legalEntity'
SET e:LegalEntity;

MATCH (e:Entity) WHERE e.entityType = 'arrangement'
SET e:Arrangement;

MATCH (e:Entity) WHERE e.entitySubtype = 'trust'
SET e:Trust;

MATCH (e:Entity) WHERE e.entitySubtype = 'nomination'
SET e:Nomination;

MATCH (e:Entity) WHERE e.entityType = 'state'
SET e:State;

MATCH (e:Entity) WHERE e.entityType = 'stateBody'
SET e:StateBody;

MATCH (e:Entity) WHERE e.entitySubtype = 'governmentDepartment'
SET e:GovernmentDepartment;

MATCH (e:Entity) WHERE e.entitySubtype = 'stateAgency'
SET e:StateAgency;

// --- Step 4: Load Person nodes ---

LOAD CSV WITH HEADERS FROM 'file:///persons.csv' AS row
CALL {
    WITH row
    MERGE (p:Person {recordId: row.recordId})
    SET p.statementId = row.statementId,
        p.statementDate = row.statementDate,
        p.recordStatus = row.recordStatus,
        p.declarationSubject = row.declarationSubject,
        p.name = row.name,
        p.personType = row.personType,
        p.isComponent = CASE row.isComponent WHEN 'True' THEN true ELSE false END,
        p.familyName = row.familyName,
        p.givenName = row.givenName,
        p.birthDate = row.birthDate,
        p.deathDate = row.deathDate,
        p.nationalityCode = row.nationalityCode,
        p.pepStatus = row.pepStatus,
        p.unspecifiedReason = row.unspecifiedReason,
        p.publisherName = row.publisherName,
        p.publicationDate = row.publicationDate,
        p.bodsVersion = row.bodsVersion,
        p.names_json = row.names_json,
        p.identifiers_json = row.identifiers_json,
        p.addresses_json = row.addresses_json,
        p.nationalities_json = row.nationalities_json,
        p.source_json = row.source_json,
        p.annotations_json = row.annotations_json
} IN TRANSACTIONS OF 10000 ROWS;

// --- Step 5: Load relationships ---
// Direction: (interestedParty)-[:HAS_INTEREST]->(subject)

LOAD CSV WITH HEADERS FROM 'file:///relationships.csv' AS row
CALL {
    WITH row
    // Find source node (interested party) - could be Entity or Person
    OPTIONAL MATCH (source:Entity {recordId: row.sourceRecordId})
    OPTIONAL MATCH (sourcePerson:Person {recordId: row.sourceRecordId})
    WITH row, COALESCE(source, sourcePerson) AS sourceNode
    // Find target node (subject entity)
    MATCH (target:Entity {recordId: row.targetRecordId})
    WHERE sourceNode IS NOT NULL
    CREATE (sourceNode)-[r:HAS_INTEREST]->(target)
    SET r.statementId = row.statementId,
        r.statementDate = row.statementDate,
        r.recordId = row.recordId,
        r.recordStatus = row.recordStatus,
        r.declarationSubject = row.declarationSubject,
        r.isComponent = CASE row.isComponent WHEN 'True' THEN true ELSE false END,
        r.isBeneficialOwnership = CASE row.isBeneficialOwnership WHEN 'True' THEN true ELSE false END,
        r.interestTypes = row.interestTypes,
        r.directOrIndirect = row.directOrIndirect,
        r.shareExact = CASE WHEN row.shareExact IS NOT NULL AND row.shareExact <> ''
                       THEN toFloat(row.shareExact) END,
        r.shareMinimum = CASE WHEN row.shareMinimum IS NOT NULL AND row.shareMinimum <> ''
                         THEN toFloat(row.shareMinimum) END,
        r.shareMaximum = CASE WHEN row.shareMaximum IS NOT NULL AND row.shareMaximum <> ''
                         THEN toFloat(row.shareMaximum) END,
        r.interestStartDate = row.interestStartDate,
        r.interestEndDate = row.interestEndDate,
        r.publisherName = row.publisherName,
        r.publicationDate = row.publicationDate,
        r.bodsVersion = row.bodsVersion,
        r.interests_json = row.interests_json,
        r.componentRecords_json = row.componentRecords_json,
        r.source_json = row.source_json,
        r.annotations_json = row.annotations_json
} IN TRANSACTIONS OF 10000 ROWS;

// --- Step 6: Create full-text search index ---

CREATE FULLTEXT INDEX bods_names IF NOT EXISTS
FOR (n:Entity|Person) ON EACH [n.name];

// --- Summary ---

MATCH (e:Entity) WITH count(e) AS entities
MATCH (p:Person) WITH entities, count(p) AS persons
MATCH ()-[r:HAS_INTEREST]->() WITH entities, persons, count(r) AS relationships
RETURN entities, persons, relationships;
"""

    script_path = output_dir / "import.cypher"
    script_path.write_text(script, encoding="utf-8")
    logger.info("Generated Cypher import script: %s", script_path)


def _generate_admin_import_script(output_dir: Path):
    """Generate a neo4j-admin bulk import shell script."""
    script = """\
#!/bin/bash
# =============================================================================
# BODS Neo4j Bulk Import Script (neo4j-admin)
# Generated by bods-neo4j
#
# Usage: Run this against a STOPPED Neo4j instance for fastest import.
#        This creates a new database from scratch.
#
#   chmod +x import.sh
#   ./import.sh
# =============================================================================

set -euo pipefail

# Configuration
NEO4J_HOME="${NEO4J_HOME:-/var/lib/neo4j}"
DATABASE="${NEO4J_DATABASE:-neo4j}"
IMPORT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Importing BODS data into Neo4j database: ${DATABASE}"
echo "Import directory: ${IMPORT_DIR}"

# Run neo4j-admin import
neo4j-admin database import full "${DATABASE}" \\
    --overwrite-destination \\
    --nodes=Entity="${IMPORT_DIR}/entities.csv" \\
    --nodes=Person="${IMPORT_DIR}/persons.csv" \\
    --relationships=HAS_INTEREST="${IMPORT_DIR}/relationships.csv" \\
    --skip-bad-relationships \\
    --skip-duplicate-nodes \\
    --trim-strings=true \\
    --id-type=STRING

echo ""
echo "Import complete. Start Neo4j and create indexes with:"
echo "  neo4j start"
echo "  cypher-shell < ${IMPORT_DIR}/post_import_indexes.cypher"
"""

    script_path = output_dir / "import.sh"
    script_path.write_text(script, encoding="utf-8")
    script_path.chmod(0o755)

    # Post-import index creation script
    post_import = """\
// Run this after neo4j-admin import to create constraints and indexes
CREATE CONSTRAINT constraint_entity_record_id IF NOT EXISTS
FOR (e:Entity) REQUIRE e.recordId IS UNIQUE;

CREATE CONSTRAINT constraint_person_record_id IF NOT EXISTS
FOR (p:Person) REQUIRE p.recordId IS UNIQUE;

CREATE INDEX idx_entity_name IF NOT EXISTS FOR (e:Entity) ON (e.name);
CREATE INDEX idx_person_name IF NOT EXISTS FOR (p:Person) ON (p.name);
CREATE INDEX idx_entity_jurisdiction IF NOT EXISTS FOR (e:Entity) ON (e.jurisdictionCode);

CREATE FULLTEXT INDEX bods_names IF NOT EXISTS
FOR (n:Entity|Person) ON EACH [n.name];
"""

    post_path = output_dir / "post_import_indexes.cypher"
    post_path.write_text(post_import, encoding="utf-8")

    logger.info("Generated admin import scripts: %s, %s", script_path, post_path)
