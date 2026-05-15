"""Map graph-native Neo4j envelopes back to BODS v0.4 statements.

The extractor at ``neo4j_to_bods.extractor`` yields one envelope per BODS
statement, already aggregated with its child nodes (identifiers, addresses,
jurisdiction, interests). This module reconstructs each envelope into a
valid BODS v0.4 statement dict.

The extracted graph already carries all BODS-meaningful fields as scalar /
list properties on the parent node — *except* any forward-compat or
publisher-specific fields, which are preserved verbatim in ``extrasJson``
and merged back in at this layer.
"""

import json
import logging
import uuid
from datetime import date
from typing import Optional

from ..config import PublisherConfig

logger = logging.getLogger(__name__)

# UUID v5 namespace for deterministic statement ID generation when an envelope
# lacks a statementId (i.e. native-graph imports that never went through the
# forward BODS mapper).
BODS_NEO4J_NAMESPACE = uuid.UUID("e8f9a0b1-c2d3-4e5f-a6b7-c8d9e0f1a2b3")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def map_entity_node(envelope: dict, publisher_config: PublisherConfig = None) -> dict:
    """Reconstruct a BODS entity statement from an envelope.

    ``envelope`` is the dict yielded by ``extract_entity_statements``:
        {"node": {...}, "labels": [...], "jurisdiction": {...}|None,
         "identifiers": [...], "addresses": [...]}.
    """
    if publisher_config is None:
        publisher_config = PublisherConfig()

    node = envelope.get("node", {}) or {}
    is_round_trip = bool(node.get("statementId"))
    extras = _parse_extras(node)

    record_details = dict(extras.get("recordDetails", {})) if extras else {}

    # Entity type
    entity_type_obj = {"type": node.get("entityType", "registeredEntity") or "registeredEntity"}
    if node.get("entitySubtype"):
        entity_type_obj["subtype"] = node["entitySubtype"]
    if node.get("entityTypeDetails"):
        entity_type_obj["details"] = node["entityTypeDetails"]
    record_details["entityType"] = entity_type_obj

    record_details["isComponent"] = bool(node.get("isComponent", False))

    if node.get("name"):
        record_details["name"] = node["name"]
    # Preserve `alternateNames: []` when the source had the key present.
    if "alternateNames" in node:
        record_details["alternateNames"] = list(node["alternateNames"])

    # Prefer the statement-local inline `jurisdictionName / jurisdictionCode`
    # on the Entity itself; fall back to the shared :Country node when the
    # entity didn't carry inline scalars (graphs imported from non-BODS
    # sources).
    jurisdiction = envelope.get("jurisdiction") or {}
    out_jur: dict = {}
    if node.get("jurisdictionCode"):
        out_jur["code"] = node["jurisdictionCode"]
    elif jurisdiction.get("code"):
        out_jur["code"] = jurisdiction["code"]
    if node.get("jurisdictionName"):
        out_jur["name"] = node["jurisdictionName"]
    elif jurisdiction.get("name"):
        out_jur["name"] = jurisdiction["name"]
    if out_jur:
        record_details["jurisdiction"] = out_jur

    identifiers = _rebuild_identifiers(envelope.get("identifiers"))
    if identifiers:
        record_details["identifiers"] = identifiers

    addresses = _rebuild_addresses(envelope.get("addresses"))
    if addresses:
        record_details["addresses"] = addresses

    if node.get("foundingDate"):
        record_details["foundingDate"] = node["foundingDate"]
    if node.get("dissolutionDate"):
        record_details["dissolutionDate"] = node["dissolutionDate"]
    if node.get("uri"):
        record_details["uri"] = node["uri"]

    public_listing = _rebuild_public_listing(node)
    if public_listing:
        record_details["publicListing"] = public_listing
    formed_by = _rebuild_formed_by_statute(node)
    if formed_by:
        record_details["formedByStatute"] = formed_by

    statement = _build_statement_envelope(node, "entity", extras, publisher_config, is_round_trip)
    statement["recordDetails"] = record_details

    replaces = node.get("replacesStatements") or []
    if replaces:
        statement["replacesStatements"] = list(replaces)

    return statement


def map_person_node(envelope: dict, publisher_config: PublisherConfig = None) -> dict:
    if publisher_config is None:
        publisher_config = PublisherConfig()

    node = envelope.get("node", {}) or {}
    is_round_trip = bool(node.get("statementId"))
    extras = _parse_extras(node)

    record_details = dict(extras.get("recordDetails", {})) if extras else {}

    record_details["personType"] = node.get("personType", "knownPerson") or "knownPerson"
    record_details["isComponent"] = bool(node.get("isComponent", False))

    names = _rebuild_names(node)
    if names:
        record_details["names"] = names

    if node.get("birthDate"):
        record_details["birthDate"] = node["birthDate"]
    if node.get("deathDate"):
        record_details["deathDate"] = node["deathDate"]

    nationalities = _rebuild_country_list(
        node.get("nationalityCodes"), node.get("nationalityNames"),
    )
    if nationalities:
        record_details["nationalities"] = nationalities

    tax_residencies = _rebuild_country_list(
        node.get("taxResidencyCodes"), node.get("taxResidencyNames"),
    )
    if tax_residencies:
        record_details["taxResidencies"] = tax_residencies

    identifiers = _rebuild_identifiers(envelope.get("identifiers"))
    if identifiers:
        record_details["identifiers"] = identifiers

    addresses = _rebuild_addresses(envelope.get("addresses"))
    if addresses:
        record_details["addresses"] = addresses

    pob = envelope.get("place_of_birth")
    if pob:
        addr = (pob.get("addr") or {}).copy()
        country = pob.get("country") or {}
        pob_out = {}
        if addr.get("address"):
            pob_out["address"] = addr["address"]
        if addr.get("postCode"):
            pob_out["postCode"] = addr["postCode"]
        if country:
            pob_country = {}
            if country.get("code"):
                pob_country["code"] = country["code"]
            if country.get("name"):
                pob_country["name"] = country["name"]
            if pob_country:
                pob_out["country"] = pob_country
        pob_out["type"] = "placeOfBirth"
        if pob_out:
            record_details["placeOfBirth"] = pob_out

    pep = _rebuild_political_exposure(node)
    if pep:
        record_details["politicalExposure"] = pep

    if node.get("unspecifiedReason"):
        unspecified = {"reason": node["unspecifiedReason"]}
        if node.get("unspecifiedDescription"):
            unspecified["description"] = node["unspecifiedDescription"]
        record_details["unspecifiedPersonDetails"] = unspecified

    statement = _build_statement_envelope(node, "person", extras, publisher_config, is_round_trip)
    statement["recordDetails"] = record_details

    replaces = node.get("replacesStatements") or []
    if replaces:
        statement["replacesStatements"] = list(replaces)

    return statement


def map_relationship(envelope: dict, publisher_config: PublisherConfig = None) -> dict:
    """Reconstruct a BODS relationship statement from a group of typed edges.

    The envelope groups all sibling typed edges (OWNS / CONTROLS / …) that
    share a `statementId`. Statement-level metadata (publication, source,
    annotations, extras) lives duplicated on every edge — we read it from
    the first (interestIndex=0) edge to rebuild the BODS statement.
    """
    if publisher_config is None:
        publisher_config = PublisherConfig()

    edges = envelope.get("edges") or []
    if not edges:
        logger.warning("Skipping relationship envelope with no edges: %s",
                       envelope.get("statement_id"))
        return {}

    first = edges[0]
    extras = _parse_extras(first)

    record_details = dict(extras.get("recordDetails", {})) if extras else {}
    record_details["isComponent"] = bool(first.get("isComponent", False))

    # Subject / interested party — string recordIds when the endpoint is
    # :Entity / :Person; reconstruct the inline unspecified object when
    # the endpoint is a :UnspecifiedParty sentinel; or explicit null when
    # the sentinel carries `partyValue: "null"`.
    if envelope.get("subject_record_id"):
        record_details["subject"] = envelope["subject_record_id"]
    elif envelope.get("subject_unspecified"):
        record_details["subject"] = _rebuild_unspecified(envelope["subject_unspecified"])

    if envelope.get("interested_party_record_id"):
        record_details["interestedParty"] = envelope["interested_party_record_id"]
    elif envelope.get("interested_party_unspecified"):
        record_details["interestedParty"] = _rebuild_unspecified(
            envelope["interested_party_unspecified"]
        )

    # Interests — synthetic edges (emitted when the source had no
    # interests[]) are filtered out, and the `interests` key is omitted
    # entirely when none remain so the round-trip preserves the original
    # shape.
    real_edges = [e for e in edges if not e.get("synthetic")]
    if real_edges:
        record_details["interests"] = [_rebuild_interest(e) for e in real_edges]

    if first.get("componentRecordsJson"):
        try:
            record_details["componentRecords"] = json.loads(first["componentRecordsJson"])
        except (TypeError, json.JSONDecodeError):
            pass

    statement = _build_statement_envelope(
        first, "relationship", extras, publisher_config,
        is_round_trip=bool(first.get("statementId")),
    )
    statement["recordDetails"] = record_details

    replaces = first.get("replacesStatements") or []
    if replaces:
        statement["replacesStatements"] = list(replaces)

    return statement


def _rebuild_unspecified(props: dict):
    """Rebuild a BODS inline ``unspecified`` object from a sentinel's properties.

    Returns ``None`` when the sentinel was anchoring an explicit
    ``"interestedParty": null`` (the UK PSC retired-relationship pattern).
    """
    if props.get("partyValue") == "null":
        return None
    out: dict = {}
    for k in ("describedByPersonStatement", "describedByEntityStatement",
              "unspecifiedReason", "unspecifiedDescription"):
        if props.get(k):
            out[k] = props[k]
    raw = props.get("extrasJson")
    if raw:
        try:
            extra = json.loads(raw)
            for k, v in (extra or {}).items():
                out.setdefault(k, v)
        except (TypeError, json.JSONDecodeError):
            pass
    return out


# ---------------------------------------------------------------------------
# Sub-object rebuilders
# ---------------------------------------------------------------------------


def _rebuild_identifiers(specs) -> list:
    if not specs:
        return []
    items = sorted(specs, key=lambda s: s.get("ordinal", 0) or 0)
    out = []
    for spec in items:
        ident = spec.get("ident") or {}
        # Per-edge null-field markers — fields the original BODS identifier
        # had as explicit `null` rather than absent.
        null_fields = set(spec.get("nullFields") or [])
        entry = {}
        if ident.get("id"):
            entry["id"] = ident["id"]
        for field in ("scheme", "schemeName", "uri"):
            if field in null_fields:
                entry[field] = None
            elif ident.get(field):
                entry[field] = ident[field]
        if entry:
            out.append(entry)
    return out


def _rebuild_addresses(specs) -> list:
    if not specs:
        return []
    items = sorted(specs, key=lambda s: s.get("ordinal", 0) or 0)
    out = []
    for spec in items:
        addr = spec.get("addr") or {}
        country = spec.get("country") or {}
        entry = {}
        if spec.get("type"):
            entry["type"] = spec["type"]
        if addr.get("address"):
            entry["address"] = addr["address"]
        elif addr.get("addressEmpty"):
            entry["address"] = ""
        if addr.get("postCode"):
            entry["postCode"] = addr["postCode"]
        country_out: dict = {}
        if country.get("code"):
            country_out["code"] = country["code"]
        if country.get("name"):
            country_out["name"] = country["name"]
        # Fallback: BODS country objects with `name` only (no `code`) are
        # stored on the Address node since there's no key to anchor a
        # :Country node.
        if not country_out and addr.get("countryName"):
            country_out["name"] = addr["countryName"]
        # If the source had an explicit `code: ""` or `code: null`,
        # re-emit it. (For ISO-coded countries we'd have set `code` via
        # the LOCATED_IN edge above; this only fires for the no-code path.)
        if country_out and "code" not in country_out:
            shape = addr.get("countryCodeShape")
            if shape == "empty":
                country_out["code"] = ""
            elif shape == "null":
                country_out["code"] = None
        if country_out:
            entry["country"] = country_out
        if entry:
            out.append(entry)
    return out


def _rebuild_country_list(codes, names) -> list:
    if not codes:
        return []
    names = names or []
    out = []
    for i, code in enumerate(codes):
        if not code:
            continue
        entry = {"code": code}
        if i < len(names) and names[i]:
            entry["name"] = names[i]
        out.append(entry)
    return out


def _rebuild_names(node: dict) -> list:
    """Prefer the preserved namesJson if present (multi-name records); else
    rebuild a single name from scalar properties."""
    raw = node.get("namesJson")
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list) and parsed:
                return parsed
        except (TypeError, json.JSONDecodeError):
            pass

    full = node.get("fullName") or node.get("name")
    if not full and not node.get("familyName") and not node.get("givenName"):
        return []

    entry = {}
    if full:
        entry["fullName"] = full
    if node.get("familyName"):
        entry["familyName"] = node["familyName"]
    if node.get("givenName"):
        entry["givenName"] = node["givenName"]
    if node.get("patronymicName"):
        entry["patronymicName"] = node["patronymicName"]
    if node.get("nameType"):
        entry["type"] = node["nameType"]
    return [entry]


def _rebuild_political_exposure(node: dict) -> dict:
    pep = {}
    if node.get("pepStatus"):
        pep["status"] = node["pepStatus"]
    raw = node.get("politicalExposureDetailsJson")
    if raw:
        try:
            pep["details"] = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            pass
    return pep


def _rebuild_source(node: dict) -> dict:
    out = {}
    types = node.get("sourceTypes")
    if isinstance(types, list) and types:
        out["type"] = list(types)
    if node.get("sourceDescription"):
        out["description"] = node["sourceDescription"]
    if node.get("sourceUrl"):
        out["url"] = node["sourceUrl"]
    if node.get("sourceRetrievedAt"):
        out["retrievedAt"] = node["sourceRetrievedAt"]
    if node.get("sourceAssertedByJson"):
        try:
            out["assertedBy"] = json.loads(node["sourceAssertedByJson"])
        except (TypeError, json.JSONDecodeError):
            pass
    return out


def _rebuild_annotations(node: dict) -> list:
    motivations = node.get("annotationMotivations") or []
    if not motivations:
        return []
    descriptions = node.get("annotationDescriptions") or []
    pointers = node.get("annotationStatementPointers") or []
    pointer_present = node.get("annotationStatementPointerPresent") or []
    dates = node.get("annotationCreationDates") or []
    urls = node.get("annotationUrls") or []
    created_by_names = node.get("annotationCreatedByNames") or []
    created_by_uris = node.get("annotationCreatedByUris") or []

    out = []
    for i, mot in enumerate(motivations):
        entry = {}
        if mot:
            entry["motivation"] = mot
        if i < len(descriptions) and descriptions[i]:
            entry["description"] = descriptions[i]
        # Empty-string `statementPointerTarget` is preserved when the
        # source had the key present (BODS producers like GLEIF emit
        # `statementPointerTarget: ""` explicitly).
        if i < len(pointer_present) and pointer_present[i]:
            entry["statementPointerTarget"] = (
                pointers[i] if i < len(pointers) else ""
            )
        elif i < len(pointers) and pointers[i]:
            entry["statementPointerTarget"] = pointers[i]
        if i < len(dates) and dates[i]:
            entry["creationDate"] = dates[i]
        if i < len(urls) and urls[i]:
            entry["url"] = urls[i]
        created_by = {}
        if i < len(created_by_names) and created_by_names[i]:
            created_by["name"] = created_by_names[i]
        if i < len(created_by_uris) and created_by_uris[i]:
            created_by["uri"] = created_by_uris[i]
        if created_by:
            entry["createdBy"] = created_by
        if entry:
            out.append(entry)
    return out


def _rebuild_public_listing(node: dict) -> dict:
    out = {}
    if node.get("hasPublicListing") is not None:
        out["hasPublicListing"] = bool(node["hasPublicListing"])
    if node.get("companyFilingsURL"):
        out["companyFilingsURL"] = node["companyFilingsURL"]
    raw = node.get("securitiesListingsJson")
    if raw:
        try:
            out["securitiesListings"] = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            pass
    return out


def _rebuild_formed_by_statute(node: dict) -> dict:
    out = {}
    if node.get("formedByStatuteName"):
        out["name"] = node["formedByStatuteName"]
    if node.get("formedByStatuteDate"):
        out["date"] = node["formedByStatuteDate"]
    return out


def _rebuild_interest(props: dict) -> dict:
    """Rebuild a single BODS interest object from an :Interest node's props.
    Omits ``type`` when the original interest had no explicit type set."""
    entry: dict = {}
    if props.get("bodsInterestType"):
        entry["type"] = props["bodsInterestType"]
    if props.get("directOrIndirect"):
        entry["directOrIndirect"] = props["directOrIndirect"]
    if props.get("beneficialOwnershipOrControl") is not None:
        entry["beneficialOwnershipOrControl"] = bool(props["beneficialOwnershipOrControl"])

    share = {}
    if props.get("shareExact") is not None:
        share["exact"] = _coerce_share(props["shareExact"])
    if props.get("shareMinimum") is not None:
        share["minimum"] = _coerce_share(props["shareMinimum"])
    if props.get("shareMaximum") is not None:
        share["maximum"] = _coerce_share(props["shareMaximum"])
    if props.get("shareExclusiveMin") is not None:
        share["exclusiveMinimum"] = _coerce_share(props["shareExclusiveMin"])
    if props.get("shareExclusiveMax") is not None:
        share["exclusiveMaximum"] = _coerce_share(props["shareExclusiveMax"])
    if share:
        entry["share"] = share

    if props.get("startDate"):
        entry["startDate"] = props["startDate"]
    if props.get("endDate"):
        entry["endDate"] = props["endDate"]
    if props.get("details"):
        entry["details"] = props["details"]

    raw_extras = props.get("interestExtrasJson")
    if raw_extras:
        try:
            extras = json.loads(raw_extras)
            for k, v in (extras or {}).items():
                entry.setdefault(k, v)
        except (TypeError, json.JSONDecodeError):
            pass

    return entry


# ---------------------------------------------------------------------------
# Statement envelope
# ---------------------------------------------------------------------------


def _build_statement_envelope(
    node: dict,
    record_type: str,
    extras: Optional[dict],
    publisher_config: PublisherConfig,
    is_round_trip: bool,
) -> dict:
    statement_id = node.get("statementId") or _generate_statement_id(node, record_type)
    record_id = node.get("recordId") or _generate_record_id(node, record_type)

    out = {
        "statementId": statement_id,
        "recordId": record_id,
        "recordType": record_type,
    }

    if node.get("statementDate"):
        out["statementDate"] = node["statementDate"]
    if node.get("recordStatus"):
        out["recordStatus"] = node["recordStatus"]
    if node.get("declarationSubject"):
        out["declarationSubject"] = node["declarationSubject"]

    source = _rebuild_source(node)
    if source:
        out["source"] = source
    elif not is_round_trip:
        out["source"] = _build_default_source(publisher_config)

    annotations = _rebuild_annotations(node)
    if annotations:
        out["annotations"] = annotations
    elif node.get("annotationsKeyPresent"):
        # Source had `annotations: []` — preserve the empty array.
        out["annotations"] = []

    out["publicationDetails"] = _rebuild_publication_details(node, publisher_config)

    # Merge any top-level extras (publisher-specific fields the forward mapper
    # didn't recognise).
    if extras and extras.get("statement"):
        for k, v in extras["statement"].items():
            out.setdefault(k, v)

    return out


def _rebuild_publication_details(node: dict, publisher_config: PublisherConfig) -> dict:
    publisher = {"name": node.get("publisherName") or publisher_config.publisher_name}
    if node.get("publisherUrl"):
        publisher["url"] = node["publisherUrl"]
    elif publisher_config.publisher_url:
        publisher["url"] = publisher_config.publisher_url
    pub = {
        "publicationDate": node.get("publicationDate") or str(date.today()),
        "bodsVersion": node.get("bodsVersion") or publisher_config.bods_version,
        "publisher": publisher,
    }
    if node.get("publicationLicense"):
        pub["license"] = node["publicationLicense"]
    elif publisher_config.license_url:
        pub["license"] = publisher_config.license_url
    return pub


def _build_default_source(publisher_config: PublisherConfig) -> dict:
    return {
        "type": [publisher_config.source_type],
        "description": publisher_config.source_description,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_extras(node_or_first_interest: dict) -> dict:
    raw = node_or_first_interest.get("extrasJson")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def _generate_statement_id(obj: dict, record_type: str) -> str:
    seed = f"{record_type}:{obj.get('recordId', obj.get('name', str(id(obj))))}"
    return str(uuid.uuid5(BODS_NEO4J_NAMESPACE, seed))


def _generate_record_id(obj: dict, record_type: str) -> str:
    name = obj.get("name", "")
    node_id = obj.get("primaryIdentifierId", name)
    return f"neo4j-{record_type}-{node_id}" if node_id else f"neo4j-{record_type}-{id(obj)}"


def _coerce_share(value):
    """BODS share fields are numeric. Neo4j returns floats; preserve ints
    when the value is integral (so `100` round-trips as `100`, not `100.0`).
    Booleans pass through unchanged so legacy producers that use
    `exclusiveMinimum: true` round-trip cleanly."""
    if isinstance(value, bool):
        return value
    try:
        f = float(value)
    except (TypeError, ValueError):
        return value
    if f.is_integer():
        return int(f)
    return f
