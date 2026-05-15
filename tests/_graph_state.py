"""In-memory simulation of the Neo4j graph for round-trip tests.

Builds a (label, key) -> properties node table and a rel-type -> [edge specs]
table from the forward mapper's output, then constructs the same envelopes the
extractor would yield. Lets the reverse mapper run without a live database.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from bods_neo4j.bods_to_neo4j.mapper import map_statement
from bods_neo4j.utils.bods_schema import FAMILY_REL_TYPES


class GraphState:
    """Forward-map a stream of BODS statements into an in-memory graph."""

    def __init__(self) -> None:
        # (primary_label, key_value) -> properties (right-biased merge across
        # multiple statements that touch the same dedup-keyed child).
        self.nodes: dict[tuple[str, str], dict] = {}
        # rel_type -> list of edge specs
        self.edges: dict[str, list] = defaultdict(list)

    @classmethod
    def from_statements(cls, statements: Iterable[dict]) -> "GraphState":
        state = cls()
        for s in statements:
            state.ingest(s)
        return state

    def ingest(self, statement: dict) -> None:
        graph = map_statement(statement)
        if graph is None:
            return
        for node in graph["nodes"]:
            primary_label = node["labels"][0]
            key = (primary_label, node["key_value"])
            existing = self.nodes.get(key)
            if existing is None:
                self.nodes[key] = dict(node["properties"])
            else:
                for k, v in node["properties"].items():
                    if v not in (None, ""):
                        existing[k] = v
        for edge in graph["edges"]:
            self.edges[edge["rel_type"]].append(edge)

    def ingest_with_dedup(self, statement: dict) -> None:
        """Mirror the logical dedup semantics for round-trip tests.

        The production loader achieves dedup by filtering superseded
        statements during a Pass 0 file scan (see
        ``bods_to_neo4j.driver_loader._scan_for_dedup`` /
        ``_is_loser``). This helper mirrors the same end-state by
        evicting older entries when a newer statement arrives, so unit
        tests can build a "current state" graph one statement at a time
        without needing a pre-scan.
        """
        graph = map_statement(statement)
        if graph is None:
            return

        if graph["statement_type"] in ("entity", "person"):
            for sid in graph.get("replaces_statements") or []:
                self._evict_node_by_statement_id(sid)
        elif graph["statement_type"] == "relationship":
            record_id = graph.get("record_id") or ""
            replaces = set(graph.get("replaces_statements") or [])
            doomed_sids: set[str] = set()
            for fam in FAMILY_REL_TYPES:
                kept = []
                for e in self.edges.get(fam, []):
                    props = e["properties"]
                    if (record_id and props.get("recordId") == record_id) or (
                        props.get("statementId") in replaces
                    ):
                        sid = props.get("statementId")
                        if sid:
                            doomed_sids.add(sid)
                        continue
                    kept.append(e)
                self.edges[fam] = kept
            for sid in doomed_sids:
                for side in ("party", "subject"):
                    self.nodes.pop(("UnspecifiedParty", f"{sid}:{side}"), None)

        self.ingest(statement)

    def _evict_node_by_statement_id(self, statement_id: str) -> None:
        """Drop any :Entity / :Person node whose statementId matches, plus any
        edges incident to it. Mirrors DETACH DELETE semantics in Cypher.
        """
        evicted_keys: list[tuple[str, str]] = []
        for key, props in list(self.nodes.items()):
            label = key[0]
            if label not in ("Entity", "Person"):
                continue
            if props.get("statementId") == statement_id:
                del self.nodes[key]
                evicted_keys.append(key)
        if not evicted_keys:
            return
        evicted_label_keys = {(label, kv) for label, kv in evicted_keys}
        for rel_type, specs in list(self.edges.items()):
            self.edges[rel_type] = [
                e for e in specs
                if (e["start_label"], e["start_key_value"]) not in evicted_label_keys
                and (e["end_label"], e["end_key_value"]) not in evicted_label_keys
            ]

    # ------------------------------------------------------------------
    # Envelope builders — same shape the extractor yields.
    # ------------------------------------------------------------------

    def entity_envelope(self, record_id: str) -> dict:
        props = self.nodes[("Entity", record_id)]
        return {
            "node": props,
            "labels": ["Entity"],
            "jurisdiction": self._first_country_for("REGISTERED_IN", "Entity", record_id),
            "identifiers": self._identifier_envelopes("Entity", record_id),
            "addresses": self._address_envelopes("Entity", record_id),
        }

    def person_envelope(self, record_id: str) -> dict:
        props = self.nodes[("Person", record_id)]
        return {
            "node": props,
            "labels": ["Person"],
            "identifiers": self._identifier_envelopes("Person", record_id),
            "addresses": self._address_envelopes("Person", record_id),
            "place_of_birth": self._first_pob_for(record_id),
        }

    def relationship_envelope(self, statement_id: str) -> dict:
        """Collect all typed edges for the given statementId and rebuild the
        envelope the extractor would yield.
        """
        matching: list[tuple] = []
        for fam in FAMILY_REL_TYPES:
            for e in self.edges.get(fam, []):
                if e["properties"].get("statementId") == statement_id:
                    matching.append((fam, e))
        matching.sort(key=lambda fe: fe[1]["properties"].get("interestIndex", 0) or 0)

        ip_record_id = None
        ip_unspecified = None
        subj_record_id = None
        subj_unspecified = None
        if matching:
            fam, e = matching[0]
            start_label = e["start_label"]
            end_label = e["end_label"]
            start_key = e["start_key_value"]
            end_key = e["end_key_value"]

            if start_label == "UnspecifiedParty":
                ip_unspecified = self.nodes.get(("UnspecifiedParty", start_key))
            else:
                ip_record_id = start_key

            if end_label == "UnspecifiedParty":
                subj_unspecified = self.nodes.get(("UnspecifiedParty", end_key))
            else:
                subj_record_id = end_key

        return {
            "statement_id": statement_id,
            "edges": [dict(e["properties"]) for _fam, e in matching],
            "edge_rel_types": [fam for fam, _e in matching],
            "interested_party_record_id": ip_record_id,
            "interested_party_labels": [],
            "interested_party_unspecified": ip_unspecified,
            "subject_record_id": subj_record_id,
            "subject_labels": [],
            "subject_unspecified": subj_unspecified,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _identifier_envelopes(self, parent_label: str, parent_key: str) -> list:
        out = []
        for e in self.edges.get("HAS_IDENTIFIER", []):
            if e["start_key_value"] != parent_key:
                continue
            ident = self.nodes.get(("Identifier", e["end_key_value"]))
            if ident:
                out.append({
                    "ident": ident,
                    "ordinal": e["properties"].get("ordinal"),
                    "isPrimary": e["properties"].get("isPrimary", False),
                    "nullFields": e["properties"].get("nullFields"),
                })
        return out

    def _address_envelopes(self, parent_label: str, parent_key: str) -> list:
        out = []
        for e in self.edges.get("HAS_ADDRESS", []):
            if e["start_key_value"] != parent_key:
                continue
            addr = self.nodes.get(("Address", e["end_key_value"]))
            if not addr:
                continue
            country = None
            for li in self.edges.get("LOCATED_IN", []):
                if li["start_key_value"] == addr["uid"]:
                    country = self.nodes.get(("Country", li["end_key_value"]))
                    break
            out.append({
                "addr": addr,
                "type": e["properties"].get("type"),
                "ordinal": e["properties"].get("ordinal"),
                "country": country,
            })
        return out

    def _first_country_for(self, rel_type: str, parent_label: str, parent_key: str) -> dict | None:
        for e in self.edges.get(rel_type, []):
            if e["start_key_value"] == parent_key:
                country = self.nodes.get(("Country", e["end_key_value"]))
                if country:
                    return country
        return None

    def _first_pob_for(self, person_key: str) -> dict | None:
        for e in self.edges.get("BORN_IN", []):
            if e["start_key_value"] != person_key:
                continue
            addr = self.nodes.get(("Address", e["end_key_value"]))
            if not addr:
                continue
            country = None
            for li in self.edges.get("LOCATED_IN", []):
                if li["start_key_value"] == addr["uid"]:
                    country = self.nodes.get(("Country", li["end_key_value"]))
                    break
            return {"addr": addr, "country": country}
        return None
