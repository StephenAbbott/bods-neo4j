"""Map BODS v0.4 statements to Neo4j nodes and relationships.

This module transforms BODS statements into dictionaries ready for Neo4j import,
preserving full round-trip fidelity so that the data can be converted back to BODS.

Graph Schema:
    Nodes:
        (:Entity)  - from entity statements, with additional type/subtype labels
        (:Person)  - from person statements

    Relationships:
        [:HAS_INTEREST] - from relationship statements (ownership, control, etc.)
        [:REPLACES]     - temporal versioning (when replacesStatements is present)
        [:COMPONENT_OF] - linking component records to primary records

    Properties:
        All BODS metadata is preserved as node/relationship properties.
        Complex nested structures (arrays of objects) are serialised as JSON strings
        to enable full round-trip conversion back to BODS format.
"""

import json
import logging
from typing import Optional

from ..utils.bods_schema import (
    RECORD_TYPE_ENTITY,
    RECORD_TYPE_PERSON,
    RECORD_TYPE_RELATIONSHIP,
    extract_addresses,
    extract_identifiers,
    extract_interests,
    extract_jurisdiction,
    extract_primary_name,
    get_entity_subtype,
    get_entity_type,
    get_neo4j_labels_for_entity,
    get_person_type,
    get_record_details,
    get_record_type,
)

logger = logging.getLogger(__name__)


def map_statement(statement: dict) -> Optional[dict]:
    """Map a BODS statement to a Neo4j-ready dictionary.

    Args:
        statement: A BODS v0.4 statement dictionary

    Returns:
        A dictionary with keys:
            - "type": "node" or "relationship"
            - "labels": list of Neo4j labels (for nodes)
            - "rel_type": relationship type string (for relationships)
            - "properties": dict of properties
            - "source_record_id" / "target_record_id": for relationships
        Returns None if the statement cannot be mapped.
    """
    record_type = get_record_type(statement)

    if record_type == RECORD_TYPE_ENTITY:
        return _map_entity_statement(statement)
    elif record_type == RECORD_TYPE_PERSON:
        return _map_person_statement(statement)
    elif record_type == RECORD_TYPE_RELATIONSHIP:
        return _map_relationship_statement(statement)
    else:
        logger.warning("Unknown record type: %s (statementId: %s)",
                       record_type, statement.get("statementId", "?"))
        return None


def _map_entity_statement(statement: dict) -> dict:
    """Map a BODS entity statement to a Neo4j node."""
    record_details = get_record_details(statement)
    labels = get_neo4j_labels_for_entity(record_details)
    jurisdiction = extract_jurisdiction(record_details)

    properties = {
        # Core BODS fields (preserved for round-trip)
        "statementId": statement.get("statementId", ""),
        "statementDate": statement.get("statementDate", ""),
        "recordId": statement.get("recordId", ""),
        "recordType": RECORD_TYPE_ENTITY,
        "recordStatus": statement.get("recordStatus", ""),
        "declarationSubject": statement.get("declarationSubject", ""),
        # Entity-specific fields
        "name": extract_primary_name(record_details, RECORD_TYPE_ENTITY),
        "entityType": get_entity_type(record_details),
        "entitySubtype": get_entity_subtype(record_details),
        "entityTypeDetails": record_details.get("entityType", {}).get("details", ""),
        "isComponent": record_details.get("isComponent", False),
        # Jurisdiction
        "jurisdictionName": jurisdiction.get("name", ""),
        "jurisdictionCode": jurisdiction.get("code", ""),
        # Dates
        "foundingDate": record_details.get("foundingDate", ""),
        "dissolutionDate": record_details.get("dissolutionDate", ""),
        # URI
        "uri": record_details.get("uri", ""),
    }

    # Serialise complex nested structures as JSON for round-trip fidelity
    identifiers = extract_identifiers(record_details)
    if identifiers:
        properties["identifiers_json"] = json.dumps(identifiers)
        # Also extract primary identifier for easy querying
        if identifiers:
            properties["primaryIdentifierId"] = identifiers[0].get("id", "")
            properties["primaryIdentifierScheme"] = identifiers[0].get("scheme", "")

    addresses = extract_addresses(record_details)
    if addresses:
        properties["addresses_json"] = json.dumps(addresses)
        # Extract first registered address for easy querying
        registered = next(
            (a for a in addresses if a.get("type") == "registered"), None
        )
        if registered:
            properties["registeredAddress"] = registered.get("address", "")
            properties["registeredPostCode"] = registered.get("postCode", "")
            properties["registeredCountry"] = registered.get("country", {}).get("code", "")

    alternate_names = record_details.get("alternateNames", [])
    if alternate_names:
        properties["alternateNames_json"] = json.dumps(alternate_names)

    # Publication details
    pub = statement.get("publicationDetails", {})
    if pub:
        properties["publisherName"] = pub.get("publisher", {}).get("name", "")
        properties["publicationDate"] = pub.get("publicationDate", "")
        properties["bodsVersion"] = pub.get("bodsVersion", "")

    # Source
    source = statement.get("source", {})
    if source:
        properties["source_json"] = json.dumps(source)

    # Annotations
    annotations = statement.get("annotations", [])
    if annotations:
        properties["annotations_json"] = json.dumps(annotations)

    # Public listing
    public_listing = record_details.get("publicListing", {})
    if public_listing:
        properties["publicListing_json"] = json.dumps(public_listing)

    # Formed by statute
    formed_by = record_details.get("formedByStatute", {})
    if formed_by:
        properties["formedByStatute_json"] = json.dumps(formed_by)

    # Clean empty strings
    properties = {k: v for k, v in properties.items() if v != "" and v is not None}

    return {
        "type": "node",
        "labels": labels,
        "properties": properties,
    }


def _map_person_statement(statement: dict) -> dict:
    """Map a BODS person statement to a Neo4j node."""
    record_details = get_record_details(statement)

    properties = {
        # Core BODS fields
        "statementId": statement.get("statementId", ""),
        "statementDate": statement.get("statementDate", ""),
        "recordId": statement.get("recordId", ""),
        "recordType": RECORD_TYPE_PERSON,
        "recordStatus": statement.get("recordStatus", ""),
        "declarationSubject": statement.get("declarationSubject", ""),
        # Person-specific fields
        "name": extract_primary_name(record_details, RECORD_TYPE_PERSON),
        "personType": get_person_type(record_details),
        "isComponent": record_details.get("isComponent", False),
        # Dates
        "birthDate": record_details.get("birthDate", ""),
        "deathDate": record_details.get("deathDate", ""),
    }

    # Names array (serialised for round-trip, with primary name extracted above)
    names = record_details.get("names", [])
    if names:
        properties["names_json"] = json.dumps(names)
        # Extract name parts for querying
        first_name = names[0]
        if first_name.get("familyName"):
            properties["familyName"] = first_name["familyName"]
        if first_name.get("givenName"):
            properties["givenName"] = first_name["givenName"]

    # Nationalities
    nationalities = record_details.get("nationalities", [])
    if nationalities:
        properties["nationalities_json"] = json.dumps(nationalities)
        # Extract first nationality code for querying
        if nationalities and nationalities[0].get("code"):
            properties["nationalityCode"] = nationalities[0]["code"]

    # Identifiers
    identifiers = extract_identifiers(record_details)
    if identifiers:
        properties["identifiers_json"] = json.dumps(identifiers)

    # Addresses
    addresses = extract_addresses(record_details)
    if addresses:
        properties["addresses_json"] = json.dumps(addresses)

    # Political exposure
    pep = record_details.get("politicalExposure", {})
    if pep:
        properties["pepStatus"] = pep.get("status", "")
        properties["politicalExposure_json"] = json.dumps(pep)

    # Tax residencies
    tax_res = record_details.get("taxResidencies", [])
    if tax_res:
        properties["taxResidencies_json"] = json.dumps(tax_res)

    # Place of birth
    pob = record_details.get("placeOfBirth", {})
    if pob:
        properties["placeOfBirth_json"] = json.dumps(pob)

    # Publication details
    pub = statement.get("publicationDetails", {})
    if pub:
        properties["publisherName"] = pub.get("publisher", {}).get("name", "")
        properties["publicationDate"] = pub.get("publicationDate", "")
        properties["bodsVersion"] = pub.get("bodsVersion", "")

    # Source
    source = statement.get("source", {})
    if source:
        properties["source_json"] = json.dumps(source)

    # Annotations
    annotations = statement.get("annotations", [])
    if annotations:
        properties["annotations_json"] = json.dumps(annotations)

    # Unspecified person details
    unspecified = record_details.get("unspecifiedPersonDetails", {})
    if unspecified:
        properties["unspecifiedReason"] = unspecified.get("reason", "")
        properties["unspecifiedDescription"] = unspecified.get("description", "")

    # Clean empty strings
    properties = {k: v for k, v in properties.items() if v != "" and v is not None}

    return {
        "type": "node",
        "labels": ["Person"],
        "properties": properties,
    }


def _map_relationship_statement(statement: dict) -> dict:
    """Map a BODS relationship statement to a Neo4j relationship.

    Direction: (interestedParty)-[:HAS_INTEREST]->(subject)
    This reads as: "The interested party HAS_INTEREST in the subject entity"
    """
    record_details = get_record_details(statement)

    # Resolve subject and interested party
    subject = record_details.get("subject", "")
    interested_party = record_details.get("interestedParty", "")

    # Handle unspecified records (where subject/interestedParty is an object, not a string)
    subject_record_id = subject if isinstance(subject, str) else ""
    interested_party_record_id = interested_party if isinstance(interested_party, str) else ""

    subject_unspecified = subject if isinstance(subject, dict) else None
    ip_unspecified = interested_party if isinstance(interested_party, dict) else None

    # Extract interest details
    interests = extract_interests(record_details)
    properties = {
        # Core BODS fields
        "statementId": statement.get("statementId", ""),
        "statementDate": statement.get("statementDate", ""),
        "recordId": statement.get("recordId", ""),
        "recordType": RECORD_TYPE_RELATIONSHIP,
        "recordStatus": statement.get("recordStatus", ""),
        "declarationSubject": statement.get("declarationSubject", ""),
        # Relationship-specific fields
        "isComponent": record_details.get("isComponent", False),
    }

    # Store full interests array for round-trip fidelity
    if interests:
        properties["interests_json"] = json.dumps(interests)
        # Extract summary fields for easy querying
        interest_types = [i.get("type", "") for i in interests if i.get("type")]
        if interest_types:
            properties["interestTypes"] = interest_types

        # Extract beneficial ownership flag
        bo_flags = [i.get("beneficialOwnershipOrControl") for i in interests
                    if i.get("beneficialOwnershipOrControl") is not None]
        if bo_flags:
            properties["isBeneficialOwnership"] = any(bo_flags)

        # Extract share information from first interest with shares
        for interest in interests:
            share = interest.get("share", {})
            if share:
                if "exact" in share:
                    properties["shareExact"] = share["exact"]
                if "minimum" in share:
                    properties["shareMinimum"] = share["minimum"]
                if "maximum" in share:
                    properties["shareMaximum"] = share["maximum"]
                break  # Use first share found

        # Extract direct/indirect
        doi = [i.get("directOrIndirect", "") for i in interests
               if i.get("directOrIndirect")]
        if doi:
            properties["directOrIndirect"] = doi[0]

        # Extract date range
        start_dates = [i.get("startDate") for i in interests if i.get("startDate")]
        end_dates = [i.get("endDate") for i in interests if i.get("endDate")]
        if start_dates:
            properties["interestStartDate"] = min(start_dates)
        if end_dates:
            properties["interestEndDate"] = max(end_dates)

    # Component records
    component_records = record_details.get("componentRecords", [])
    if component_records:
        properties["componentRecords_json"] = json.dumps(component_records)

    # Unspecified party details
    if subject_unspecified:
        properties["subjectUnspecified_json"] = json.dumps(subject_unspecified)
    if ip_unspecified:
        properties["interestedPartyUnspecified_json"] = json.dumps(ip_unspecified)

    # Publication details
    pub = statement.get("publicationDetails", {})
    if pub:
        properties["publisherName"] = pub.get("publisher", {}).get("name", "")
        properties["publicationDate"] = pub.get("publicationDate", "")
        properties["bodsVersion"] = pub.get("bodsVersion", "")

    # Source
    source = statement.get("source", {})
    if source:
        properties["source_json"] = json.dumps(source)

    # Annotations
    annotations = statement.get("annotations", [])
    if annotations:
        properties["annotations_json"] = json.dumps(annotations)

    # Clean empty strings
    properties = {k: v for k, v in properties.items() if v != "" and v is not None}

    return {
        "type": "relationship",
        "rel_type": "HAS_INTEREST",
        "source_record_id": interested_party_record_id,
        "target_record_id": subject_record_id,
        "properties": properties,
    }
