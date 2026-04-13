"""Map Neo4j nodes and relationships back to BODS v0.4 statements.

This module reconstructs valid BODS v0.4 statements from Neo4j graph data.

Two modes of operation:
1. Round-trip mode: When nodes contain *_json properties (from a prior BODS import),
   these are deserialised to reconstruct the original BODS statement faithfully.
2. Native mode: When nodes come from a non-BODS graph (e.g. the Neo4j Companies House
   demo), properties are mapped to BODS fields using best-effort heuristics.
"""

import json
import logging
import uuid
from datetime import date
from typing import Optional

from ..config import PublisherConfig

logger = logging.getLogger(__name__)

# UUID v5 namespace for deterministic statement ID generation
BODS_NEO4J_NAMESPACE = uuid.UUID("e8f9a0b1-c2d3-4e5f-a6b7-c8d9e0f1a2b3")


def map_entity_node(
    node: dict,
    publisher_config: PublisherConfig = None,
) -> dict:
    """Map a Neo4j Entity node to a BODS entity statement.

    Args:
        node: Dictionary of node properties (from extractor)
        publisher_config: BODS publisher metadata

    Returns:
        BODS v0.4 entity statement dictionary
    """
    if publisher_config is None:
        publisher_config = PublisherConfig()

    is_round_trip = "statementId" in node and "recordId" in node

    # Build record details
    record_details = {}

    # Entity type
    entity_type = node.get("entityType", "registeredEntity")
    entity_subtype = node.get("entitySubtype", "")
    entity_type_obj = {"type": entity_type}
    if entity_subtype:
        entity_type_obj["subtype"] = entity_subtype
    if node.get("entityTypeDetails"):
        entity_type_obj["details"] = node["entityTypeDetails"]
    record_details["entityType"] = entity_type_obj

    # isComponent
    record_details["isComponent"] = node.get("isComponent", False)

    # Name
    name = node.get("name", "")
    if name:
        record_details["name"] = name

    # Alternate names (from JSON or not present)
    alt_names = _parse_json_prop(node, "alternateNames_json")
    if alt_names:
        record_details["alternateNames"] = alt_names

    # Jurisdiction
    jurisdiction = {}
    if node.get("jurisdictionCode"):
        jurisdiction["code"] = node["jurisdictionCode"]
    if node.get("jurisdictionName"):
        jurisdiction["name"] = node["jurisdictionName"]
    if jurisdiction:
        record_details["jurisdiction"] = jurisdiction

    # Identifiers (prefer round-trip JSON, fall back to primary)
    identifiers = _parse_json_prop(node, "identifiers_json")
    if identifiers:
        record_details["identifiers"] = identifiers
    elif node.get("primaryIdentifierId"):
        identifier = {"id": node["primaryIdentifierId"]}
        if node.get("primaryIdentifierScheme"):
            identifier["scheme"] = node["primaryIdentifierScheme"]
        record_details["identifiers"] = [identifier]

    # Addresses
    addresses = _parse_json_prop(node, "addresses_json")
    if addresses:
        record_details["addresses"] = addresses

    # Dates
    if node.get("foundingDate"):
        record_details["foundingDate"] = node["foundingDate"]
    if node.get("dissolutionDate"):
        record_details["dissolutionDate"] = node["dissolutionDate"]

    # URI
    if node.get("uri"):
        record_details["uri"] = node["uri"]

    # Public listing
    public_listing = _parse_json_prop(node, "publicListing_json")
    if public_listing:
        record_details["publicListing"] = public_listing

    # Formed by statute
    formed_by = _parse_json_prop(node, "formedByStatute_json")
    if formed_by:
        record_details["formedByStatute"] = formed_by

    # Build the statement
    statement = {
        "statementId": node.get("statementId", _generate_statement_id(node, "entity")),
        "recordId": node.get("recordId", _generate_record_id(node, "entity")),
        "recordType": "entity",
        "recordDetails": record_details,
    }

    # Optional fields
    if node.get("statementDate"):
        statement["statementDate"] = node["statementDate"]
    if node.get("recordStatus"):
        statement["recordStatus"] = node["recordStatus"]
    if node.get("declarationSubject"):
        statement["declarationSubject"] = node["declarationSubject"]

    # Source
    source = _parse_json_prop(node, "source_json")
    if source:
        statement["source"] = source
    elif not is_round_trip:
        statement["source"] = _build_source(publisher_config)

    # Annotations
    annotations = _parse_json_prop(node, "annotations_json")
    if annotations:
        statement["annotations"] = annotations

    # Publication details
    statement["publicationDetails"] = _build_publication_details(node, publisher_config)

    return statement


def map_person_node(
    node: dict,
    publisher_config: PublisherConfig = None,
) -> dict:
    """Map a Neo4j Person node to a BODS person statement.

    Args:
        node: Dictionary of node properties (from extractor)
        publisher_config: BODS publisher metadata

    Returns:
        BODS v0.4 person statement dictionary
    """
    if publisher_config is None:
        publisher_config = PublisherConfig()

    is_round_trip = "statementId" in node and "recordId" in node

    # Build record details
    record_details = {}

    # Person type
    record_details["personType"] = node.get("personType", "knownPerson")
    record_details["isComponent"] = node.get("isComponent", False)

    # Names (prefer round-trip JSON, fall back to node name)
    names = _parse_json_prop(node, "names_json")
    if names:
        record_details["names"] = names
    elif node.get("name"):
        name_obj = {"fullName": node["name"]}
        if node.get("familyName"):
            name_obj["familyName"] = node["familyName"]
        if node.get("givenName"):
            name_obj["givenName"] = node["givenName"]
        record_details["names"] = [name_obj]

    # Nationalities
    nationalities = _parse_json_prop(node, "nationalities_json")
    if nationalities:
        record_details["nationalities"] = nationalities
    elif node.get("nationalityCode"):
        record_details["nationalities"] = [{"code": node["nationalityCode"]}]

    # Identifiers
    identifiers = _parse_json_prop(node, "identifiers_json")
    if identifiers:
        record_details["identifiers"] = identifiers

    # Addresses
    addresses = _parse_json_prop(node, "addresses_json")
    if addresses:
        record_details["addresses"] = addresses

    # Dates
    if node.get("birthDate"):
        record_details["birthDate"] = node["birthDate"]
    if node.get("deathDate"):
        record_details["deathDate"] = node["deathDate"]

    # Political exposure
    pep = _parse_json_prop(node, "politicalExposure_json")
    if pep:
        record_details["politicalExposure"] = pep
    elif node.get("pepStatus"):
        record_details["politicalExposure"] = {"status": node["pepStatus"]}

    # Tax residencies
    tax_res = _parse_json_prop(node, "taxResidencies_json")
    if tax_res:
        record_details["taxResidencies"] = tax_res

    # Place of birth
    pob = _parse_json_prop(node, "placeOfBirth_json")
    if pob:
        record_details["placeOfBirth"] = pob

    # Unspecified person details
    if node.get("unspecifiedReason"):
        unspecified = {"reason": node["unspecifiedReason"]}
        if node.get("unspecifiedDescription"):
            unspecified["description"] = node["unspecifiedDescription"]
        record_details["unspecifiedPersonDetails"] = unspecified

    # Build the statement
    statement = {
        "statementId": node.get("statementId", _generate_statement_id(node, "person")),
        "recordId": node.get("recordId", _generate_record_id(node, "person")),
        "recordType": "person",
        "recordDetails": record_details,
    }

    # Optional fields
    if node.get("statementDate"):
        statement["statementDate"] = node["statementDate"]
    if node.get("recordStatus"):
        statement["recordStatus"] = node["recordStatus"]
    if node.get("declarationSubject"):
        statement["declarationSubject"] = node["declarationSubject"]

    # Source
    source = _parse_json_prop(node, "source_json")
    if source:
        statement["source"] = source
    elif not is_round_trip:
        statement["source"] = _build_source(publisher_config)

    # Annotations
    annotations = _parse_json_prop(node, "annotations_json")
    if annotations:
        statement["annotations"] = annotations

    # Publication details
    statement["publicationDetails"] = _build_publication_details(node, publisher_config)

    return statement


def map_relationship(
    rel: dict,
    publisher_config: PublisherConfig = None,
) -> dict:
    """Map a Neo4j HAS_INTEREST relationship to a BODS relationship statement.

    Args:
        rel: Dictionary of relationship properties (from extractor), including
             _sourceRecordId, _targetRecordId, _sourceLabels, _targetLabels
        publisher_config: BODS publisher metadata

    Returns:
        BODS v0.4 relationship statement dictionary
    """
    if publisher_config is None:
        publisher_config = PublisherConfig()

    is_round_trip = "statementId" in rel and "recordId" in rel

    # Build record details
    record_details = {}

    record_details["isComponent"] = rel.get("isComponent", False)

    # Subject (target node = entity being owned/controlled)
    target_record_id = rel.get("_targetRecordId", "")
    if target_record_id:
        record_details["subject"] = target_record_id

    # Interested party (source node = owner/controller)
    source_record_id = rel.get("_sourceRecordId", "")
    if source_record_id:
        record_details["interestedParty"] = source_record_id

    # Interests (prefer round-trip JSON, fall back to individual properties)
    interests = _parse_json_prop(rel, "interests_json")
    if interests:
        record_details["interests"] = interests
    else:
        # Reconstruct from flat properties
        interest = _reconstruct_interest(rel)
        if interest:
            record_details["interests"] = [interest]

    # Component records
    component_records = _parse_json_prop(rel, "componentRecords_json")
    if component_records:
        record_details["componentRecords"] = component_records

    # Build the statement
    statement = {
        "statementId": rel.get("statementId", _generate_statement_id(rel, "relationship")),
        "recordId": rel.get("recordId", _generate_record_id(rel, "relationship")),
        "recordType": "relationship",
        "recordDetails": record_details,
    }

    # Optional fields
    if rel.get("statementDate"):
        statement["statementDate"] = rel["statementDate"]
    if rel.get("recordStatus"):
        statement["recordStatus"] = rel["recordStatus"]
    if rel.get("declarationSubject"):
        statement["declarationSubject"] = rel["declarationSubject"]

    # Source
    source = _parse_json_prop(rel, "source_json")
    if source:
        statement["source"] = source
    elif not is_round_trip:
        statement["source"] = _build_source(publisher_config)

    # Annotations
    annotations = _parse_json_prop(rel, "annotations_json")
    if annotations:
        statement["annotations"] = annotations

    # Publication details
    statement["publicationDetails"] = _build_publication_details(rel, publisher_config)

    return statement


def _reconstruct_interest(rel: dict) -> Optional[dict]:
    """Reconstruct a BODS interest object from flat relationship properties."""
    interest = {}

    # Interest type
    interest_types = rel.get("interestTypes")
    if isinstance(interest_types, str):
        try:
            interest_types = json.loads(interest_types)
        except (json.JSONDecodeError, TypeError):
            interest_types = [interest_types]
    if interest_types and isinstance(interest_types, list) and interest_types[0]:
        interest["type"] = interest_types[0]

    # Beneficial ownership
    if rel.get("isBeneficialOwnership") is not None:
        interest["beneficialOwnershipOrControl"] = bool(rel["isBeneficialOwnership"])

    # Direct or indirect
    if rel.get("directOrIndirect"):
        interest["directOrIndirect"] = rel["directOrIndirect"]

    # Share
    share = {}
    if rel.get("shareExact") is not None:
        share["exact"] = float(rel["shareExact"])
    if rel.get("shareMinimum") is not None:
        share["minimum"] = float(rel["shareMinimum"])
    if rel.get("shareMaximum") is not None:
        share["maximum"] = float(rel["shareMaximum"])
    if share:
        interest["share"] = share

    # Dates
    if rel.get("interestStartDate"):
        interest["startDate"] = rel["interestStartDate"]
    if rel.get("interestEndDate"):
        interest["endDate"] = rel["interestEndDate"]

    return interest if interest else None


def _parse_json_prop(obj: dict, key: str):
    """Parse a JSON-serialised property, returning None if not present or invalid."""
    value = obj.get(key)
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value  # Already parsed
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None


def _generate_statement_id(obj: dict, record_type: str) -> str:
    """Generate a deterministic statement ID from node/relationship properties."""
    seed = f"{record_type}:{obj.get('recordId', obj.get('name', str(id(obj))))}"
    return str(uuid.uuid5(BODS_NEO4J_NAMESPACE, seed))


def _generate_record_id(obj: dict, record_type: str) -> str:
    """Generate a record ID for nodes that don't have one."""
    name = obj.get("name", "")
    node_id = obj.get("primaryIdentifierId", name)
    return f"neo4j-{record_type}-{node_id}" if node_id else f"neo4j-{record_type}-{id(obj)}"


def _build_source(publisher_config: PublisherConfig) -> dict:
    """Build a BODS source object."""
    source = {
        "type": [publisher_config.source_type],
        "description": publisher_config.source_description,
    }
    return source


def _build_publication_details(obj: dict, publisher_config: PublisherConfig) -> dict:
    """Build BODS publication details, preferring preserved values."""
    pub = {
        "publicationDate": obj.get("publicationDate", str(date.today())),
        "bodsVersion": obj.get("bodsVersion", publisher_config.bods_version),
        "publisher": {
            "name": obj.get("publisherName", publisher_config.publisher_name),
        },
    }
    if publisher_config.publisher_url:
        pub["publisher"]["url"] = publisher_config.publisher_url
    if publisher_config.license_url:
        pub["license"] = publisher_config.license_url
    return pub
