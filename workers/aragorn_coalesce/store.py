"""Read-only access to the AnswerCoalesce DuckDB graph.

Ported from AnswerCoalesce's ``src/graph_coalescence/duckdb_store.py``, keeping
what set-input (MCQ) enrichment needs: the enrichment query itself, node names,
and the provenance of the support edges. EDGAR's inference queries are not
ported.

Each process opens its own read-only connection on first use (the worker runs
enrichment in a process pool), so importing this module needs no database.
"""

import os
from dataclasses import dataclass
from typing import Iterable, Optional

import duckdb
import orjson
import pyarrow
from duckdb.sqltypes import BIGINT, DOUBLE
from scipy.special import pdtrc

from shepherd_utils.config import settings

#: Schema versions this reader understands. 7 is what AnswerCoalesce builds;
#: 8 (``build_duckdb.py`` here) adds each evidence row's knowledge level and
#: agent type.
SUPPORTED_SCHEMA_VERSIONS = ("7", "8")
#: What TRAPI 2.0 calls a knowledge level or agent type nobody recorded.
NOT_PROVIDED = "not_provided"
#: Nodes never offered as an enrichment: too general to mean anything.
BLOCKLIST = (
    "HP:0000118",
    "MONDO:0000001",
    "MONDO:0700096",
    "UMLS:C1333305",
    "CHEBI:24431",
    "CHEBI:23367",
    "CHEBI:33579",
    "CHEBI:36357",
    "CHEBI:33675",
    "CHEBI:33302",
    "CHEBI:33304",
    "CHEBI:33582",
    "CHEBI:25806",
    "CHEBI:50860",
    "CHEBI:51143",
    "CHEBI:32988",
    "CHEBI:33285",
    "CHEBI:33256",
    "CHEBI:36962",
    "CHEBI:35352",
    "CHEBI:36963",
    "CHEBI:25367",
    "CHEBI:72695",
    "CHEBI:33595",
    "CHEBI:33832",
    "CHEBI:37577",
    "CHEBI:24532",
    "CHEBI:5686",
    "NCBITaxon:9606",
)
#: The TRAPI 2.0 RetrievalSource properties; a KGX source may carry others
#: (``id``, ``category``) that a TRAPI edge must not.
_SOURCE_KEYS = (
    "resource_id",
    "resource_role",
    "upstream_resource_ids",
    "source_record_urls",
)


@dataclass(frozen=True)
class EnrichmentCandidate:
    """One enriched (neighbor, relation, direction) feature of the input set."""

    neighbor_curie: str
    predicate_json: str
    member_is_subject: bool
    is_symmetric: bool
    p_value: float
    support_count: int
    background_count: int
    linked_curies: tuple[str, ...]
    neighbor_categories: tuple[str, ...]


@dataclass(frozen=True)
class Evidence:
    """One source edge behind a fact, as TRAPI 2.0 needs it."""

    sources: list
    knowledge_level: str
    agent_type: str


class _State:
    connection: Optional[duckdb.DuckDBPyConnection] = None
    path: Optional[str] = None
    schema_version: Optional[str] = None
    relation_rows: Optional[list] = None


_state = _State()


def _poisson_survival(support_count, expected_count):
    return pyarrow.array(
        pdtrc(
            support_count.to_numpy(zero_copy_only=False) - 1,
            expected_count.to_numpy(zero_copy_only=False),
        )
    )


def connection() -> duckdb.DuckDBPyConnection:
    """This process's read-only connection to the configured database."""
    path = os.path.abspath(settings.answer_coalesce_duckdb_path)
    if _state.connection is not None and _state.path == path:
        return _state.connection
    close_connection()
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"AnswerCoalesce DuckDB database not found at {path}. Mount it there "
            "or set ANSWER_COALESCE_DUCKDB_PATH (build one with "
            "`python -m workers.aragorn_coalesce.build_duckdb`)."
        )
    conn = duckdb.connect(path, read_only=True)
    try:
        row = conn.execute(
            "SELECT value FROM metadata WHERE key = 'schema_version'"
        ).fetchone()
        version = row[0] if row else "missing"
        if version not in SUPPORTED_SCHEMA_VERSIONS:
            raise RuntimeError(
                f"AnswerCoalesce DuckDB schema version {version} is not "
                f"supported; expected one of {', '.join(SUPPORTED_SCHEMA_VERSIONS)}. "
                "Rebuild the database."
            )
        conn.create_function(
            "ac_poisson_survival",
            _poisson_survival,
            [BIGINT, DOUBLE],
            DOUBLE,
            type="arrow",
        )
        temp_directory = settings.answer_coalesce_duckdb_temp_directory or (
            f"/tmp/answer-coalesce-duckdb-{os.getpid()}"
        )
        for name, value in (
            ("temp_directory", temp_directory),
            ("memory_limit", settings.answer_coalesce_duckdb_memory_limit),
            (
                "max_temp_directory_size",
                settings.answer_coalesce_duckdb_max_temp_directory_size,
            ),
            ("threads", str(settings.answer_coalesce_duckdb_threads)),
            ("preserve_insertion_order", "false"),
        ):
            conn.execute(f"SET {name} = ?", [value])
    except Exception:
        conn.close()
        raise
    _state.connection = conn
    _state.path = path
    _state.schema_version = version
    return conn


def close_connection() -> None:
    """Close this process's connection (tests switch databases with it)."""
    if _state.connection is not None:
        _state.connection.close()
    _state.connection = None
    _state.path = None
    _state.schema_version = None
    _state.relation_rows = None


def _normalized_constraint(constraint) -> dict:
    if isinstance(constraint, dict):
        return constraint
    if isinstance(constraint, str) and constraint.startswith("{"):
        return orjson.loads(constraint)
    return {"predicate": constraint}


def _relation_ids(predicate_constraints, context_qualifiers) -> Optional[set]:
    """The relations matching every context qualifier and any constraint.

    None means "no restriction". A constraint matches a relation whose
    predicate and qualifiers include all of the constraint's.
    """
    constraints = [_normalized_constraint(c) for c in (predicate_constraints or [])]
    if not constraints and not context_qualifiers:
        return None
    if _state.relation_rows is None:
        _state.relation_rows = [
            (relation_id, orjson.loads(relation_json))
            for relation_id, relation_json in connection()
            .execute("SELECT relation_id, predicate_json FROM relation")
            .fetchall()
        ]
    matched = set()
    for relation_id, relation in _state.relation_rows:
        if not all(
            relation.get(key) == value
            for key, value in (context_qualifiers or {}).items()
        ):
            continue
        if constraints and not any(
            constraint.items() <= relation.items() for constraint in constraints
        ):
            continue
        matched.add(relation_id)
    return matched


def get_node_names(curies: Iterable[str]) -> dict[str, str]:
    curies = list(dict.fromkeys(curies))
    if not curies:
        return {}
    rows = (
        connection()
        .execute(
            "SELECT curie, name FROM node WHERE curie IN (SELECT unnest(?))",
            [curies],
        )
        .fetchall()
    )
    return {curie: name or "" for curie, name in rows}


def enrichment_candidates(
    input_ids,
    input_category,
    *,
    input_is_subject=None,
    node_constraints=None,
    predicate_constraints=None,
    context_qualifiers=None,
    pvalue_threshold=None,
    max_results=None,
):
    """The features over-represented among ``input_ids``, best p-value first.

    A feature is a (neighbor node, relation, direction) pattern. Its p-value is
    the Poisson survival of seeing it on ``support_count`` of the inputs when
    ``background_count`` of the ``total_node_count`` nodes in the input
    category have it. Returns ``(candidates, total_node_count)``.
    """
    input_ids = list(dict.fromkeys(input_ids))
    if not input_ids:
        return [], 0

    conn = connection()
    categories = {
        category: (category_id, int(node_count))
        for category_id, category, node_count in conn.execute(
            """
            SELECT category_id, category, node_count
            FROM category_count
            WHERE category IN (?, 'biolink:NamedThing')
            """,
            [input_category],
        ).fetchall()
    }
    background = categories.get(input_category) or categories.get("biolink:NamedThing")
    if background is None:
        return [], 0
    background_category_id, total_node_count = background

    input_node_ids = [
        row[0]
        for row in conn.execute(
            "SELECT node_id FROM node WHERE curie IN (SELECT unnest(?))",
            [input_ids],
        ).fetchall()
    ]
    if not input_node_ids:
        return [], total_node_count

    relation_ids = _relation_ids(predicate_constraints, context_qualifiers)
    if relation_ids == set():
        return [], total_node_count

    params: list = [input_node_ids]
    relation_filter = ""
    if relation_ids is not None:
        relation_filter = "AND feature.relation_id IN (SELECT unnest(?))"
        params.append(sorted(relation_ids))
    direction_join = ""
    direction_filter = ""
    if input_is_subject is not None:
        # Asymmetric relations must run in the query's direction; symmetric
        # ones are eligible either way.
        direction_join = """
            JOIN relation direction_relation
              ON direction_relation.relation_id = feature.relation_id
        """
        direction_filter = """
            AND (direction_relation.is_symmetric OR feature.member_is_subject = ?)
        """
        params.append(input_is_subject)
    params.append(list(BLOCKLIST))
    category_filter = ""
    requested_categories = node_constraints or ["biolink:NamedThing"]
    if "biolink:NamedThing" not in requested_categories:
        category_filter = """
            AND EXISTS (
                SELECT 1
                FROM unnest(neighbor.categories) AS category_value(category)
                WHERE category IN (SELECT unnest(?))
            )
        """
        params.append(list(requested_categories))

    n_inputs = len(input_ids)
    params += [n_inputs, total_node_count, background_category_id]
    params += [n_inputs, total_node_count]
    pvalue_filter = ""
    if pvalue_threshold is not None:
        pvalue_filter = "WHERE p_value < ?"
        params.append(pvalue_threshold)
    limit = ""
    if max_results is not None:
        limit = "LIMIT ?"
        params.append(max_results)

    rows = conn.execute(
        f"""
        WITH
        matched_membership AS MATERIALIZED (
            SELECT member_node_id, feature_id
            FROM membership
            WHERE member_node_id IN (SELECT unnest(?))
        ),
        eligible_membership AS MATERIALIZED (
            SELECT candidate.member_node_id, candidate.feature_id
            FROM matched_membership candidate
            JOIN feature USING (feature_id)
            {direction_join}
            JOIN node neighbor ON neighbor.node_id = feature.neighbor_node_id
            WHERE TRUE
              {relation_filter}
              {direction_filter}
              AND neighbor.curie NOT IN (SELECT unnest(?))
              {category_filter}
        ),
        candidate_support AS (
            SELECT feature_id, count(*)::BIGINT AS support_count
            FROM eligible_membership
            GROUP BY feature_id
        ),
        surviving AS (
            SELECT
                candidate.feature_id,
                candidate.support_count,
                stats.background_count,
                ac_poisson_survival(
                    candidate.support_count,
                    stats.background_count * ?::DOUBLE / ?
                ) AS p_value
            FROM candidate_support candidate
            JOIN feature_stats stats
              ON stats.category_id = ?
             AND stats.feature_id = candidate.feature_id
            -- only features seen more often than chance predicts
            WHERE candidate.support_count >= stats.background_count * ? / ?
        ),
        selected AS (
            SELECT
                surviving.feature_id,
                surviving.support_count,
                surviving.background_count,
                surviving.p_value
            FROM surviving
            JOIN feature USING (feature_id)
            {pvalue_filter}
            ORDER BY
                surviving.p_value,
                feature.neighbor_node_id,
                feature.relation_id,
                feature.member_is_subject,
                surviving.feature_id
            {limit}
        ),
        linked AS (
            SELECT
                selected.feature_id,
                selected.support_count,
                selected.background_count,
                selected.p_value,
                list(member.curie ORDER BY member.curie) AS linked_curies
            FROM selected
            JOIN eligible_membership USING (feature_id)
            JOIN node member ON member.node_id = eligible_membership.member_node_id
            GROUP BY
                selected.feature_id,
                selected.support_count,
                selected.background_count,
                selected.p_value
        )
        SELECT
            neighbor.curie,
            relation.predicate_json,
            feature.member_is_subject,
            relation.is_symmetric,
            candidate.p_value,
            candidate.support_count,
            candidate.background_count,
            candidate.linked_curies,
            neighbor.categories
        FROM linked candidate
        JOIN feature USING (feature_id)
        JOIN relation USING (relation_id)
        JOIN node neighbor ON neighbor.node_id = feature.neighbor_node_id
        ORDER BY
            candidate.p_value,
            neighbor.curie,
            feature.relation_id,
            feature.member_is_subject
        """,
        params,
    ).fetchall()

    return [
        EnrichmentCandidate(
            neighbor_curie=row[0],
            predicate_json=row[1],
            member_is_subject=row[2],
            is_symmetric=row[3],
            p_value=row[4],
            support_count=row[5],
            background_count=row[6],
            linked_curies=tuple(row[7]),
            neighbor_categories=tuple(row[8]),
        )
        for row in rows
    ], total_node_count


def trapi_sources(provenance) -> list[dict]:
    """A stored provenance record as TRAPI 2.0 RetrievalSources.

    The builder stores either a KGX edge's ``sources`` list verbatim or, for
    ROBOKOP-style KGX, a mapping of role to infores (or list of infores).
    """
    if isinstance(provenance, list):
        return [
            {key: source[key] for key in _SOURCE_KEYS if source.get(key)}
            for source in provenance
            if isinstance(source, dict)
        ]
    primary = None
    sources = []
    for role, value in provenance.items():
        role = role.removeprefix("biolink:")
        for resource_id in value if isinstance(value, list) else [value]:
            if not resource_id:
                continue
            source = {"resource_id": resource_id, "resource_role": role}
            sources.append(source)
            if role == "primary_knowledge_source":
                primary = resource_id
    # ROBOKOP's flat form doesn't say what each aggregator retrieved from; the
    # primary source is the best available answer.
    if primary is not None:
        for source in sources:
            if source["resource_role"] != "primary_knowledge_source":
                source["upstream_resource_ids"] = [primary]
    return sources


def get_edge_provenance(edges) -> dict[tuple, list[Evidence]]:
    """The source edges behind each (subject, predicate_json, object) edge.

    An edge's relation may be implied rather than stated (a ``treats`` fact
    supports ``related_to``), and a symmetric edge may be stored the other way
    round; both are resolved through ``relation_implication``.
    """
    edges = list(dict.fromkeys(edges))
    if not edges:
        return {}
    conn = connection()
    relations = {
        predicate_json: (relation_id, is_symmetric)
        for predicate_json, relation_id, is_symmetric in conn.execute(
            """
            SELECT predicate_json, relation_id, is_symmetric
            FROM relation
            WHERE predicate_json IN (SELECT unnest(?))
            """,
            [list(dict.fromkeys(edge[1] for edge in edges))],
        ).fetchall()
    }
    node_ids = dict(
        conn.execute(
            "SELECT curie, node_id FROM node WHERE curie IN (SELECT unnest(?))",
            [list(dict.fromkeys(c for s, _, o in edges for c in (s, o)))],
        ).fetchall()
    )
    resolved = [
        (s, node_ids[s], relations[p][0], o, node_ids[o], relations[p][1])
        for s, p, o in edges
        if p in relations and s in node_ids and o in node_ids
    ]
    if not resolved:
        return {}

    requested = pyarrow.table(
        {
            "requested_subject": [edge[0] for edge in resolved],
            "subject_node_id": [edge[1] for edge in resolved],
            "relation_id": [edge[2] for edge in resolved],
            "requested_object": [edge[3] for edge in resolved],
            "object_node_id": [edge[4] for edge in resolved],
            "is_symmetric": [edge[5] for edge in resolved],
        }
    )
    klat_columns = (
        "evidence.knowledge_level, evidence.agent_type"
        if _state.schema_version != "7"
        else "NULL, NULL"
    )
    conn.register("_requested_provenance", requested)
    try:
        rows = conn.execute(f"""
            WITH resolved AS (
                SELECT
                    requested.requested_subject,
                    requested.relation_id,
                    requested.requested_object,
                    fact.fact_id
                FROM _requested_provenance requested
                JOIN relation_implication implication
                  ON implication.implied_relation_id = requested.relation_id
                JOIN fact
                  ON fact.relation_id = implication.concrete_relation_id
                 AND fact.subject_node_id = requested.subject_node_id
                 AND fact.object_node_id = requested.object_node_id

                UNION

                SELECT
                    requested.requested_subject,
                    requested.relation_id,
                    requested.requested_object,
                    fact.fact_id
                FROM _requested_provenance requested
                JOIN relation_implication implication
                  ON implication.implied_relation_id = requested.relation_id
                JOIN fact
                  ON requested.is_symmetric
                 AND fact.relation_id = implication.concrete_relation_id
                 AND fact.subject_node_id = requested.object_node_id
                 AND fact.object_node_id = requested.subject_node_id
            )
            SELECT
                resolved.requested_subject,
                resolved.relation_id,
                resolved.requested_object,
                evidence.sources_json,
                {klat_columns}
            FROM resolved
            JOIN evidence USING (fact_id)
            ORDER BY evidence.evidence_id
            """).fetchall()
    finally:
        conn.unregister("_requested_provenance")

    predicate_by_relation = {
        relation_id: predicate_json
        for predicate_json, (relation_id, _) in relations.items()
    }
    provenance: dict[tuple, list[Evidence]] = {}
    for subject, relation_id, object_, sources_json, kl, at in rows:
        key = (subject, predicate_by_relation[relation_id], object_)
        provenance.setdefault(key, []).append(
            Evidence(
                sources=trapi_sources(orjson.loads(sources_json)),
                knowledge_level=kl or NOT_PROVIDED,
                agent_type=at or NOT_PROVIDED,
            )
        )
    return provenance
