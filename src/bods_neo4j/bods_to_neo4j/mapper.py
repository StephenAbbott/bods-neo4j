"""Map BODS v0.4 statements to a graph-native Neo4j shape.

Each statement is transformed into a ``StatementGraph`` — a structured spec of
node + edge instances ready for the driver loader / CSV exporter to materialise.

Graph shape
-----------
Nodes:
  :Entity  (+ subtype labels)         keyed on recordId
  :Person                              keyed on recordId
  :Identifier                          keyed on uid = scheme|id (deduplicated)
  :Address                             keyed on uid = sha256(content) (deduplicated)
  :Country                             keyed on code (deduplicated)
  :UnspecifiedParty                    keyed on uid = statementId:side
                                       (only for unspecified subject / interestedParty)

Edges:
  (party)-[:OWNS|CONTROLS|MANAGES|IS_PARTY_TO|HAS_OTHER_INTEREST {...}]->(subject)
      One typed edge per element of a BODS interests[] array. The edge
      carries:
        - the original BODS interestType verbatim (bodsInterestType)
        - share fields (shareExact, shareMinimum, shareMaximum,
          shareExclusiveMin, shareExclusiveMax)
        - directOrIndirect, beneficialOwnershipOrControl, startDate,
          endDate, details
        - statement-level metadata duplicated onto every edge of the same
          statement (statementId, recordId, statementDate, recordStatus,
          declarationSubject, publicationDate, publisherName, bodsVersion,
          sourceTypes / sourceDescription / sourceUrl,
          annotation*, …) so the reverse mapper can rebuild a single
          relationship statement by grouping edges with the same statementId.

Structural edges:
  (:Entity|:Person)-[:HAS_IDENTIFIER]->(:Identifier)
  (:Entity|:Person)-[:HAS_ADDRESS]->(:Address)
  (:Address)-[:LOCATED_IN]->(:Country)
  (:Entity)-[:REGISTERED_IN]->(:Country)
  (:Person)-[:BORN_IN]->(:Address)
  (:Entity|:Person)-[:COMPONENT_OF]->(:Entity)

``replacesStatements`` (BODS) is *not* materialised as a graph edge. It is
combined from the statement's top-level + recordDetails arrays and surfaced
on the returned ``StatementGraph`` dict (``replaces_statements`` + ``record_id``)
so the driver loader can use it as a forward-only delete-trigger to evict
superseded nodes / edges. The list is also stamped as a scalar list property
on the new node (entity/person) or duplicated onto every family edge
(relationship), so the reverse mapper can re-emit it on the output BODS
statement.

Round-trip fidelity is achieved by reconstructing nested BODS arrays from
related graph nodes / sibling edges (see ``neo4j_to_bods.mapper``). Any
source-field that the extraction does not consume is preserved verbatim in
an ``extrasJson`` property on the parent node or edge.
"""

import hashlib
import json
import logging
import re
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
    interest_family,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants — fields we know how to extract. Anything else flows into
# ``extrasJson`` on the parent node so the round-trip stays lossless against
# publisher-specific extensions / forward-compat BODS fields.
# ---------------------------------------------------------------------------

_STATEMENT_KNOWN_FIELDS = {
    "statementId",
    "statementDate",
    "recordId",
    "recordType",
    "recordStatus",
    "declarationSubject",
    "recordDetails",
    "publicationDetails",
    "source",
    "annotations",
    "replacesStatements",
}

_ENTITY_DETAILS_KNOWN_FIELDS = {
    "entityType",
    "name",
    "alternateNames",
    "isComponent",
    "jurisdiction",
    "identifiers",
    "addresses",
    "foundingDate",
    "dissolutionDate",
    "uri",
    "publicListing",
    "formedByStatute",
    "replacesStatements",
}

_PERSON_DETAILS_KNOWN_FIELDS = {
    "personType",
    "isComponent",
    "names",
    "identifiers",
    "addresses",
    "nationalities",
    "taxResidencies",
    "placeOfBirth",
    "politicalExposure",
    "birthDate",
    "deathDate",
    "unspecifiedPersonDetails",
    "replacesStatements",
}

_REL_DETAILS_KNOWN_FIELDS = {
    "subject",
    "interestedParty",
    "interests",
    "componentRecords",
    "isComponent",
    "replacesStatements",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def map_statement(statement: dict) -> Optional[dict]:
    """Map a BODS statement into a graph-native ``StatementGraph``.

    Returns a dict with keys:
        statement_type:  "entity" | "person" | "relationship"
        nodes:           list of NodeSpec dicts (see _node_spec)
        edges:           list of EdgeSpec dicts (see _edge_spec)
        primary:         the (label, key) of the statement's primary node,
                         or None for relationship statements (which have no
                         single primary node — only Interest nodes).
    """
    record_type = get_record_type(statement)

    if record_type == RECORD_TYPE_ENTITY:
        return _map_entity_statement(statement)
    if record_type == RECORD_TYPE_PERSON:
        return _map_person_statement(statement)
    if record_type == RECORD_TYPE_RELATIONSHIP:
        return _map_relationship_statement(statement)

    logger.warning(
        "Unknown record type: %s (statementId: %s)",
        record_type, statement.get("statementId", "?"),
    )
    return None


# ---------------------------------------------------------------------------
# Entity statements
# ---------------------------------------------------------------------------


def _map_entity_statement(statement: dict) -> dict:
    record_details = get_record_details(statement) or {}
    labels = get_neo4j_labels_for_entity(record_details)
    record_id = statement.get("recordId", "")

    props = _statement_scalars(statement, RECORD_TYPE_ENTITY)
    props["name"] = extract_primary_name(record_details, RECORD_TYPE_ENTITY)
    props["entityType"] = get_entity_type(record_details)
    props["entitySubtype"] = get_entity_subtype(record_details)
    entity_type_obj = record_details.get("entityType", {}) or {}
    if entity_type_obj.get("details"):
        props["entityTypeDetails"] = entity_type_obj["details"]
    props["isComponent"] = bool(record_details.get("isComponent", False))
    if record_details.get("foundingDate"):
        props["foundingDate"] = record_details["foundingDate"]
    if record_details.get("dissolutionDate"):
        props["dissolutionDate"] = record_details["dissolutionDate"]
    if record_details.get("uri"):
        props["uri"] = record_details["uri"]
    # Preserve `alternateNames` whenever the key is present, even when the
    # source emitted an empty array (some BODS producers do — e.g. GLEIF).
    if "alternateNames" in record_details:
        props["alternateNames"] = list(record_details["alternateNames"])
    if record_details.get("publicListing"):
        # Single-cardinality nested object; flatten its leaves for graph access.
        pl = record_details["publicListing"]
        _flatten_public_listing(pl, props)
    if record_details.get("formedByStatute"):
        fbs = record_details["formedByStatute"]
        if fbs.get("name"):
            props["formedByStatuteName"] = fbs["name"]
        if fbs.get("date"):
            props["formedByStatuteDate"] = fbs["date"]

    # The shared :Country node carries one canonical `name` (the latest
    # writer wins on cross-statement dedup), which can disagree with the
    # statement's own jurisdiction.name (some BODS data has the long
    # ISO 3166 name in jurisdiction but a shorter colloquial name in
    # addresses[].country). Preserve the statement-local name inline so
    # the round-trip stays lossless.
    jurisdiction = extract_jurisdiction(record_details) or {}
    if jurisdiction.get("code"):
        props["jurisdictionCode"] = jurisdiction["code"]
    if jurisdiction.get("name"):
        props["jurisdictionName"] = jurisdiction["name"]

    _attach_source(statement.get("source"), props)
    _attach_annotations(statement.get("annotations"), props)

    nodes: list[dict] = []
    edges: list[dict] = []

    primary_key = ("Entity", "recordId", record_id)
    primary_node = _node_spec(
        labels=labels,
        key_property="recordId",
        key_value=record_id,
        properties=_compact(props),
    )

    if jurisdiction.get("code"):
        country_node, country_key = _country_node(jurisdiction.get("code"), jurisdiction.get("name"))
        nodes.append(country_node)
        edges.append(_edge_spec(
            rel_type="REGISTERED_IN",
            start=primary_key, end=country_key,
            properties={"jurisdictionName": jurisdiction.get("name", "")} if jurisdiction.get("name") else {},
        ))

    _emit_identifier_nodes_and_edges(
        primary_key, extract_identifiers(record_details), nodes, edges,
    )
    _emit_address_nodes_and_edges(
        primary_key, extract_addresses(record_details), nodes, edges,
    )

    combined_replaces = list(statement.get("replacesStatements") or []) + list(
        record_details.get("replacesStatements") or []
    )
    if combined_replaces:
        primary_node["properties"]["replacesStatements"] = combined_replaces

    # Anything in the source dicts we did not consume → extrasJson safety net.
    extras = _extras_residual(
        statement_extras=_top_level_extras(statement),
        details_extras=_details_extras(record_details, _ENTITY_DETAILS_KNOWN_FIELDS),
    )
    if extras:
        primary_node["properties"]["extrasJson"] = json.dumps(extras, ensure_ascii=False)

    nodes.insert(0, primary_node)

    return {
        "statement_type": RECORD_TYPE_ENTITY,
        "primary": primary_key,
        "record_id": record_id,
        "replaces_statements": combined_replaces,
        "nodes": nodes,
        "edges": edges,
    }


# ---------------------------------------------------------------------------
# Person statements
# ---------------------------------------------------------------------------


def _map_person_statement(statement: dict) -> dict:
    record_details = get_record_details(statement) or {}
    record_id = statement.get("recordId", "")

    props = _statement_scalars(statement, RECORD_TYPE_PERSON)
    props["personType"] = get_person_type(record_details)
    props["isComponent"] = bool(record_details.get("isComponent", False))
    props["name"] = extract_primary_name(record_details, RECORD_TYPE_PERSON)

    names = record_details.get("names") or []
    if names:
        first = names[0]
        if first.get("fullName"):
            props["fullName"] = first["fullName"]
        if first.get("familyName"):
            props["familyName"] = first["familyName"]
        if first.get("givenName"):
            props["givenName"] = first["givenName"]
        if first.get("patronymicName"):
            props["patronymicName"] = first["patronymicName"]
        if first.get("type"):
            props["nameType"] = first["type"]
        # Preserve the full names array verbatim so multi-name records round-trip.
        props["namesJson"] = json.dumps(names, ensure_ascii=False)

    if record_details.get("birthDate"):
        props["birthDate"] = record_details["birthDate"]
    if record_details.get("deathDate"):
        props["deathDate"] = record_details["deathDate"]

    nationalities = record_details.get("nationalities") or []
    if nationalities:
        props["nationalityCodes"] = [n.get("code", "") for n in nationalities]
        props["nationalityNames"] = [n.get("name", "") for n in nationalities]

    tax_residencies = record_details.get("taxResidencies") or []
    if tax_residencies:
        props["taxResidencyCodes"] = [t.get("code", "") for t in tax_residencies]
        props["taxResidencyNames"] = [t.get("name", "") for t in tax_residencies]

    pep = record_details.get("politicalExposure") or {}
    if pep:
        if pep.get("status"):
            props["pepStatus"] = pep["status"]
        # politicalExposure.details is a free-form list of objects; preserve as JSON.
        if pep.get("details"):
            props["politicalExposureDetailsJson"] = json.dumps(pep["details"], ensure_ascii=False)

    unspecified = record_details.get("unspecifiedPersonDetails") or {}
    if unspecified:
        if unspecified.get("reason"):
            props["unspecifiedReason"] = unspecified["reason"]
        if unspecified.get("description"):
            props["unspecifiedDescription"] = unspecified["description"]

    _attach_source(statement.get("source"), props)
    _attach_annotations(statement.get("annotations"), props)

    nodes: list[dict] = []
    edges: list[dict] = []

    primary_key = ("Person", "recordId", record_id)
    primary_node = _node_spec(
        labels=["Person"],
        key_property="recordId",
        key_value=record_id,
        properties=_compact(props),
    )

    _emit_identifier_nodes_and_edges(
        primary_key, record_details.get("identifiers") or [], nodes, edges,
    )
    _emit_address_nodes_and_edges(
        primary_key, record_details.get("addresses") or [], nodes, edges,
    )

    pob = record_details.get("placeOfBirth") or {}
    if pob:
        addr_node, addr_key = _address_node(pob, default_type="placeOfBirth")
        nodes.append(addr_node)
        edges.append(_edge_spec(
            rel_type="BORN_IN",
            start=primary_key, end=addr_key,
            properties={},
        ))
        country = pob.get("country") or {}
        if country.get("code"):
            country_node, country_key = _country_node(country["code"], country.get("name"))
            nodes.append(country_node)
            edges.append(_edge_spec(
                rel_type="LOCATED_IN",
                start=addr_key, end=country_key,
                properties={},
            ))

    combined_replaces = list(statement.get("replacesStatements") or []) + list(
        record_details.get("replacesStatements") or []
    )
    if combined_replaces:
        primary_node["properties"]["replacesStatements"] = combined_replaces

    extras = _extras_residual(
        statement_extras=_top_level_extras(statement),
        details_extras=_details_extras(record_details, _PERSON_DETAILS_KNOWN_FIELDS),
    )
    if extras:
        primary_node["properties"]["extrasJson"] = json.dumps(extras, ensure_ascii=False)

    nodes.insert(0, primary_node)

    return {
        "statement_type": RECORD_TYPE_PERSON,
        "primary": primary_key,
        "record_id": record_id,
        "replaces_statements": combined_replaces,
        "nodes": nodes,
        "edges": edges,
    }


# ---------------------------------------------------------------------------
# Relationship statements
# ---------------------------------------------------------------------------


def _map_relationship_statement(statement: dict) -> dict:
    record_details = get_record_details(statement) or {}
    statement_id = statement.get("statementId", "")
    record_id = statement.get("recordId", "")

    subject = record_details.get("subject")
    interested_party = record_details.get("interestedParty")

    subject_record_id = subject if isinstance(subject, str) else None
    ip_record_id = interested_party if isinstance(interested_party, str) else None

    subject_unspecified = subject if isinstance(subject, dict) else None
    ip_unspecified = interested_party if isinstance(interested_party, dict) else None

    interests = extract_interests(record_details)

    nodes: list[dict] = []
    edges: list[dict] = []

    # Statement-level scalars are duplicated onto every typed edge emitted
    # for this statement so the graph carries inline provenance. The reverse
    # mapper groups edges by `statementId` to rebuild a single BODS
    # relationship statement.
    base_props = _statement_scalars(statement, RECORD_TYPE_RELATIONSHIP)
    base_props["isComponent"] = bool(record_details.get("isComponent", False))
    _attach_source(statement.get("source"), base_props)
    _attach_annotations(statement.get("annotations"), base_props)

    combined_replaces = list(statement.get("replacesStatements") or []) + list(
        record_details.get("replacesStatements") or []
    )
    if combined_replaces:
        base_props["replacesStatements"] = combined_replaces

    if record_details.get("componentRecords"):
        base_props["componentRecordsJson"] = json.dumps(record_details["componentRecords"], ensure_ascii=False)

    extras = _extras_residual(
        statement_extras=_top_level_extras(statement),
        details_extras=_details_extras(record_details, _REL_DETAILS_KNOWN_FIELDS),
    )

    # Resolve party / subject keys. When BODS gives an inline unspecified
    # object instead of a recordId we materialise a per-statement
    # :UnspecifiedParty sentinel node so the edge has somewhere to anchor;
    # its properties preserve the unspecified payload for round-trip.
    # Some BODS producers (e.g. UK PSC) also emit `interestedParty: null`
    # for retired / closed relationships — we anchor those to a sentinel
    # too, marked `partyValue: "null"`, so the statement survives.
    ip_null_present = "interestedParty" in record_details and interested_party is None
    subj_null_present = "subject" in record_details and subject is None
    party_key = _party_endpoint_key(
        ip_record_id, ip_unspecified, ip_null_present,
        statement_id, side="party", nodes=nodes,
    )
    subject_key = _party_endpoint_key(
        subject_record_id, subject_unspecified, subj_null_present,
        statement_id, side="subject", nodes=nodes,
    )

    # BODS allows a relationship statement with no interests[] (or empty).
    # Emit one synthetic typed edge so the statement still surfaces in the
    # graph; the reverse mapper drops synthetic edges from the rebuilt array.
    is_synthetic_anchor = not interests
    if is_synthetic_anchor:
        interests = [{}]

    for index, interest in enumerate(interests):
        bods_type = interest.get("type")
        family = interest_family(bods_type or "unknownInterest")

        edge_props = dict(base_props)
        edge_props["interestIndex"] = index
        if bods_type:
            edge_props["bodsInterestType"] = bods_type
        edge_props["family"] = family
        if is_synthetic_anchor:
            edge_props["synthetic"] = True
        edge_props["directOrIndirect"] = interest.get("directOrIndirect", "") or ""
        if interest.get("beneficialOwnershipOrControl") is not None:
            edge_props["beneficialOwnershipOrControl"] = bool(
                interest["beneficialOwnershipOrControl"]
            )
        share = interest.get("share") or {}
        if "exact" in share:
            edge_props["shareExact"] = float(share["exact"])
        if "minimum" in share:
            edge_props["shareMinimum"] = float(share["minimum"])
        if "maximum" in share:
            edge_props["shareMaximum"] = float(share["maximum"])
        # BODS share.exclusiveMinimum / exclusiveMaximum can be a boundary
        # number OR a boolean flag depending on producer; preserve verbatim.
        if "exclusiveMinimum" in share:
            edge_props["shareExclusiveMin"] = share["exclusiveMinimum"]
        if "exclusiveMaximum" in share:
            edge_props["shareExclusiveMax"] = share["exclusiveMaximum"]
        if interest.get("startDate"):
            edge_props["startDate"] = interest["startDate"]
        if interest.get("endDate"):
            edge_props["endDate"] = interest["endDate"]
        if interest.get("details"):
            edge_props["details"] = interest["details"]
            # Many BODS producers (GLEIF, UK PSC, ...) put structured
            # "<Category>: <Value>" strings in `details`. Surface them as
            # generic `detailsCategory` / `detailsValue` properties so
            # analysts can filter without text-matching. Producer-agnostic
            # — fires on anything matching the convention.
            for k, v in _parse_details_kv(interest["details"]).items():
                edge_props[k] = v
        # interest-level extras: unknown keys + known keys whose value is
        # explicit empty/null (extraction would have dropped them).
        _known = {"type", "directOrIndirect", "beneficialOwnershipOrControl",
                  "share", "startDate", "endDate", "details"}
        i_extras = {}
        for k, v in interest.items():
            if k not in _known:
                i_extras[k] = v
            elif v is None or v == "" or v == [] or v == {}:
                i_extras[k] = v
        if i_extras:
            edge_props["interestExtrasJson"] = json.dumps(i_extras, ensure_ascii=False)

        # Statement-level extras land on the FIRST edge only so they aren't
        # duplicated across siblings; reverse mapper reads them from the
        # lowest-index edge.
        if index == 0 and extras:
            edge_props["extrasJson"] = json.dumps(extras, ensure_ascii=False)

        if party_key is not None and subject_key is not None:
            edges.append(_edge_spec(
                rel_type=family,
                start=party_key, end=subject_key,
                properties=_compact(edge_props),
            ))

    return {
        "statement_type": RECORD_TYPE_RELATIONSHIP,
        "primary": None,
        "statement_id": statement_id,
        "record_id": record_id,
        "replaces_statements": combined_replaces,
        "interested_party_record_id": ip_record_id,
        "subject_record_id": subject_record_id,
        "nodes": nodes,
        "edges": edges,
    }


# Many BODS producers use a convention of `"<Category>: <Value>"` strings
# in the interest `details` field. Examples seen in real data:
#   GLEIF:   "Relationship Type: IS_ULTIMATELY_CONSOLIDATED_BY"
#   GLEIF:   "Exception Category: DIRECT_ACCOUNTING_CONSOLIDATION_PARENT"
#   UK PSC:  "Relationship Type: persons-with-significant-control-statement"
# This regex captures that shape (1-32 char title-case-words category +
# colon-space + free-text value) and leaves anything else as a plain
# `details` string. Producer-agnostic by design.
_DETAILS_KV_PATTERN = re.compile(
    r"^([A-Z][A-Za-z]*(?:\s[A-Z][A-Za-z]*){0,4}):\s(.+)$"
)


def _parse_details_kv(details: str) -> dict:
    """Parse a ``"<Category>: <Value>"`` ``details`` string into structured
    properties. Producer-agnostic — fires on any BODS source that follows
    the convention. Reverse mapper rebuilds the original string from
    ``details`` (kept verbatim), so this is purely a query-convenience.

    Returns ``{}`` when ``details`` doesn't match the pattern.
    """
    if not details or not isinstance(details, str):
        return {}
    m = _DETAILS_KV_PATTERN.match(details)
    if not m:
        return {}
    return {
        "detailsCategory": m.group(1),
        "detailsValue": m.group(2),
    }


def _party_endpoint_key(
    record_id: Optional[str],
    unspecified: Optional[dict],
    null_present: bool,
    statement_id: str,
    side: str,
    nodes: list,
):
    """Resolve the node key the typed edge should attach to.

    - If a `recordId` was given, the edge points at the :Entity / :Person
      with that recordId (resolved at load time via OPTIONAL MATCH).
    - If BODS gave an inline unspecified object, materialise a per-statement
      :UnspecifiedParty sentinel and anchor the edge there. The sentinel's
      properties preserve the BODS unspecified payload so the round-trip
      can rebuild the inline object.
    - If BODS gave an explicit null (key present, value None — UK PSC emits
      this for retired relationships), anchor to a sentinel marked
      ``partyValue: "null"`` so the statement survives the round-trip.
    - Otherwise return None (the edge is dropped — no anchor available).
    """
    if record_id:
        return _resolve_party_key(record_id)
    if unspecified:
        uid = f"{statement_id}:{side}"
        props = {"uid": uid, "side": side, "statementId": statement_id}
        for k in ("describedByPersonStatement", "describedByEntityStatement",
                  "unspecifiedReason", "unspecifiedDescription"):
            if unspecified.get(k):
                props[k] = unspecified[k]
        extras = {
            k: v for k, v in unspecified.items()
            if k not in {"describedByPersonStatement", "describedByEntityStatement",
                         "unspecifiedReason", "unspecifiedDescription"}
        }
        if extras:
            props["extrasJson"] = json.dumps(extras, ensure_ascii=False)
        nodes.append(_node_spec(
            labels=["UnspecifiedParty"],
            key_property="uid",
            key_value=uid,
            properties=_compact(props),
        ))
        return ("UnspecifiedParty", "uid", uid)
    if null_present:
        uid = f"{statement_id}:{side}"
        nodes.append(_node_spec(
            labels=["UnspecifiedParty"],
            key_property="uid",
            key_value=uid,
            properties=_compact({
                "uid": uid,
                "side": side,
                "statementId": statement_id,
                "partyValue": "null",
            }),
        ))
        return ("UnspecifiedParty", "uid", uid)
    return None


# ---------------------------------------------------------------------------
# Child-node helpers
# ---------------------------------------------------------------------------


def _emit_identifier_nodes_and_edges(parent_key, identifiers, nodes, edges):
    for index, ident in enumerate(identifiers or []):
        ident_uid = _identifier_uid(ident)
        if not ident_uid:
            continue
        nodes.append(_node_spec(
            labels=["Identifier"],
            key_property="uid",
            key_value=ident_uid,
            properties=_compact({
                "uid": ident_uid,
                "scheme": ident.get("scheme", ""),
                "id": ident.get("id", ""),
                "schemeName": ident.get("schemeName", ""),
                "uri": ident.get("uri", ""),
            }),
        ))
        # Track per-statement null patterns on the edge (not on the
        # dedup'd Identifier node). Some BODS producers emit
        # `{"scheme": null, "schemeName": null}` explicitly; preserving
        # the per-occurrence null pattern keeps the round-trip lossless.
        edge_props = {"ordinal": index, "isPrimary": index == 0}
        null_fields = [
            k for k in ("scheme", "schemeName", "uri")
            if k in ident and ident[k] is None
        ]
        if null_fields:
            edge_props["nullFields"] = null_fields
        edges.append(_edge_spec(
            rel_type="HAS_IDENTIFIER",
            start=parent_key,
            end=("Identifier", "uid", ident_uid),
            properties=edge_props,
        ))


def _emit_address_nodes_and_edges(parent_key, addresses, nodes, edges):
    for index, addr in enumerate(addresses or []):
        addr_node, addr_key = _address_node(addr)
        nodes.append(addr_node)
        edges.append(_edge_spec(
            rel_type="HAS_ADDRESS",
            start=parent_key,
            end=addr_key,
            properties={
                "type": addr.get("type", ""),
                "ordinal": index,
            },
        ))
        country = addr.get("country") or {}
        if country.get("code"):
            country_node, country_key = _country_node(country["code"], country.get("name"))
            nodes.append(country_node)
            edges.append(_edge_spec(
                rel_type="LOCATED_IN",
                start=addr_key,
                end=country_key,
                properties={},
            ))


def _address_node(addr: dict, default_type: str = ""):
    """Build an Address node spec and return ``(node_spec, key_tuple)``.

    When the BODS country object carries a `name` but no `code` (legitimate
    in some producers — e.g. fermcat's "Republic of Ireland"), the name is
    kept as a `countryName` property on the Address itself so the round-trip
    isn't lossy. When `code` is present, the country is modelled as a
    :Country node via a `[:LOCATED_IN]` edge (caller's responsibility)."""
    country = addr.get("country") or {}
    address_text = addr.get("address", "") or ""
    post_code = addr.get("postCode", "") or ""
    country_code = country.get("code", "") or ""
    country_name = country.get("name", "") or ""
    addr_type = addr.get("type", default_type) or default_type
    # Dedup hash is case-sensitive — case differences in real BODS data
    # (e.g. "rue" vs "Rue") usually indicate the producer's exact original
    # string, which we must preserve for round-trip fidelity.
    uid_seed = f"{address_text}|{post_code}|{country_code}|{country_name}".strip()
    uid = hashlib.sha256(uid_seed.encode("utf-8")).hexdigest()[:32]
    props = {
        "uid": uid,
        "address": address_text,
        "postCode": post_code,
        "defaultType": addr_type,  # diagnostic; edge.type is authoritative
    }
    if country_name and not country_code:
        props["countryName"] = country_name
    # Track the raw shape of country.code so the round-trip can
    # distinguish `code: ""`, `code: null`, and absent. GLEIF (and other
    # producers) emit explicit `code: ""` for non-ISO jurisdictions like
    # Kosovo (XK).
    if not country_code and "code" in country:
        if country["code"] is None:
            props["countryCodeShape"] = "null"
        else:
            props["countryCodeShape"] = "empty"
    # Preserve explicit empty-string string fields (UK PSC emits
    # `"address": ""` on some service-address records).
    if "address" in addr and addr["address"] == "":
        props["addressEmpty"] = True
    return _node_spec(
        labels=["Address"],
        key_property="uid",
        key_value=uid,
        properties=_compact(props),
    ), ("Address", "uid", uid)


def _country_node(code: str, name: Optional[str]):
    props = {"code": code}
    if name:
        props["name"] = name
    return _node_spec(
        labels=["Country"],
        key_property="code",
        key_value=code,
        properties=_compact(props),
    ), ("Country", "code", code)


def _identifier_uid(ident: dict) -> str:
    scheme = ident.get("scheme") or ident.get("schemeName") or ""
    ident_id = ident.get("id", "")
    if scheme and ident_id:
        return f"{scheme}|{ident_id}"
    if ident_id:
        return f"_unscoped|{ident_id}"
    return ""


def _resolve_party_key(record_id: str):
    """Party endpoints may be Entity or Person; the loader resolves the actual
    label by trying both. We use the more permissive 'Node' marker plus the
    recordId so downstream consumers know to match either label."""
    return ("Node", "recordId", record_id)


# ---------------------------------------------------------------------------
# Inline-property helpers
# ---------------------------------------------------------------------------


def _statement_scalars(statement: dict, record_type: str) -> dict:
    pub = statement.get("publicationDetails") or {}
    publisher = pub.get("publisher") or {}
    return _compact({
        "statementId": statement.get("statementId", ""),
        "statementDate": statement.get("statementDate", ""),
        "recordId": statement.get("recordId", ""),
        "recordType": record_type,
        "recordStatus": statement.get("recordStatus", ""),
        "declarationSubject": statement.get("declarationSubject", ""),
        "publicationDate": pub.get("publicationDate", ""),
        "bodsVersion": pub.get("bodsVersion", ""),
        "publisherName": publisher.get("name", ""),
        "publisherUrl": publisher.get("url", ""),
        "publicationLicense": pub.get("license", ""),
    })


def _attach_source(source, props: dict) -> None:
    if not source:
        return
    types = source.get("type")
    if isinstance(types, list):
        props["sourceTypes"] = [str(t) for t in types]
    elif types:
        props["sourceTypes"] = [str(types)]
    if source.get("description"):
        props["sourceDescription"] = source["description"]
    if source.get("url"):
        props["sourceUrl"] = source["url"]
    if source.get("retrievedAt"):
        props["sourceRetrievedAt"] = source["retrievedAt"]
    if source.get("assertedBy"):
        # ``assertedBy`` is an array of objects with name/uri; structurally
        # awkward to flatten so preserve as JSON for round-trip.
        props["sourceAssertedByJson"] = json.dumps(source["assertedBy"], ensure_ascii=False)


def _attach_annotations(annotations, props: dict) -> None:
    # Distinguish "no annotations key in source" (annotations is None when
    # caller passed statement.get('annotations')) from "annotations: []".
    if annotations is None:
        return
    # Key was present even if empty — flag so reverse mapper re-emits []
    props["annotationsKeyPresent"] = True
    if not annotations:
        return
    motivations, descriptions, pointers, created_dates, urls = [], [], [], [], []
    created_by_names, created_by_uris = [], []
    # Track which annotations had an explicit empty pointer string so we
    # can re-emit `statementPointerTarget: ""` rather than dropping it.
    pointer_present_flags = []
    for ann in annotations:
        motivations.append(ann.get("motivation", "") or "")
        descriptions.append(ann.get("description", "") or "")
        pointers.append(ann.get("statementPointerTarget", "") or "")
        pointer_present_flags.append("statementPointerTarget" in ann)
        created_dates.append(ann.get("creationDate", "") or "")
        urls.append(ann.get("url", "") or "")
        created_by_obj = ann.get("createdBy") or {}
        created_by_names.append(created_by_obj.get("name", "") or "")
        created_by_uris.append(created_by_obj.get("uri", "") or "")
    props["annotationMotivations"] = motivations
    props["annotationDescriptions"] = descriptions
    props["annotationStatementPointers"] = pointers
    props["annotationStatementPointerPresent"] = pointer_present_flags
    props["annotationCreationDates"] = created_dates
    props["annotationUrls"] = urls
    props["annotationCreatedByNames"] = created_by_names
    props["annotationCreatedByUris"] = created_by_uris


def _flatten_public_listing(pl: dict, props: dict) -> None:
    if pl.get("hasPublicListing") is not None:
        props["hasPublicListing"] = bool(pl["hasPublicListing"])
    if pl.get("companyFilingsURL"):
        props["companyFilingsURL"] = pl["companyFilingsURL"]
    if pl.get("securitiesListings"):
        props["securitiesListingsJson"] = json.dumps(pl["securitiesListings"], ensure_ascii=False)


# ---------------------------------------------------------------------------
# Extras / residual fields
# ---------------------------------------------------------------------------


def _top_level_extras(statement: dict) -> dict:
    return {k: v for k, v in statement.items() if k not in _STATEMENT_KNOWN_FIELDS}


def _details_extras(record_details: dict, known: set) -> dict:
    """Return fields that won't survive structured extraction.

    - Unknown keys (forward-compat / publisher-specific fields).
    - Known keys whose value is explicitly null / empty list / empty dict
      (the extraction code skips empty values, so without rescuing them
      here the round-trip would emit a missing key rather than the original
      explicit ``null`` / ``[]``).
    """
    out = {}
    for k, v in (record_details or {}).items():
        if k not in known:
            out[k] = v
        elif v is None or v == [] or v == {}:
            out[k] = v
    return out


def _extras_residual(statement_extras: dict, details_extras: dict) -> dict:
    """Combine top-level and details-level residuals into one extras blob."""
    extras = {}
    if statement_extras:
        extras["statement"] = statement_extras
    if details_extras:
        extras["recordDetails"] = details_extras
    return extras


# ---------------------------------------------------------------------------
# Spec constructors
# ---------------------------------------------------------------------------


def _node_spec(labels: list, key_property: str, key_value: str, properties: dict) -> dict:
    return {
        "labels": list(labels),
        "key_property": key_property,
        "key_value": key_value,
        "properties": properties,
    }


def _edge_spec(rel_type: str, start, end, properties: dict) -> dict:
    return {
        "rel_type": rel_type,
        "start_label": start[0],
        "start_key_property": start[1],
        "start_key_value": start[2],
        "end_label": end[0],
        "end_key_property": end[1],
        "end_key_value": end[2],
        "properties": _compact(properties),
    }


def _compact(props: dict) -> dict:
    """Drop empty strings / Nones; leave booleans, zeros, empty lists alone."""
    out = {}
    for k, v in props.items():
        if v is None:
            continue
        if isinstance(v, str) and v == "":
            continue
        out[k] = v
    return out
