"""Build the aragorn.graphenrich worker's DuckDB database from KGX JSONL.

Ported from AnswerCoalesce (``src/graph_coalescence/build_duckdb.py``). The
database layout is described in AnswerCoalesce's ``docs/duckdb-architecture.md``;
the one Shepherd change is that ``evidence`` also keeps each source edge's
``knowledge_level`` and ``agent_type`` (schema version 8), which TRAPI 2.0
requires on every knowledge-graph edge.

Run it from the repository root (or inside the worker's image, where /app is
the root)::

    python -m workers.aragorn_graphenrich.build_duckdb \
        --nodes nodes.jsonl --edges edges.jsonl \
        --output graphenrich/graphenrich.duckdb

It needs network access for the Biolink model (``bmt``) and, unless
``--skip-blocklist`` is given, the Translator blocklist.
"""

import argparse
import functools
import gzip
import os
import shutil
import time
from itertools import product
from pathlib import Path

import bmt
import duckdb
import orjson
import pyarrow
import requests

BLOCKLIST_URL = "https://raw.githubusercontent.com/NCATSTranslator/Relay/master/config/blocklist.json"
FILTER_PREDICATES = {
    "biolink:related_to_at_concept_level",
    "biolink:related_to_at_instance_level",
}
PRIMARY_KNOWLEDGE_SOURCE = "primary_knowledge_source"
ROBOKOP_PRIMARY_SOURCE_KEYS = (
    PRIMARY_KNOWLEDGE_SOURCE,
    f"biolink:{PRIMARY_KNOWLEDGE_SOURCE}",
)
ROBOKOP_AGGREGATOR_SOURCE_KEYS = (
    "aggregator_knowledge_source",
    "biolink:aggregator_knowledge_source",
)
SCHEMA_VERSION = "8"
DEFAULT_MEMORY_LIMIT = "6GB"
DEFAULT_MAX_TEMP_DIRECTORY_SIZE = "20GB"
DEFAULT_THREADS = 4
OBJECT_ASPECT_QUALIFIER = "object_aspect_qualifier"
OBJECT_DIRECTION_QUALIFIER = "object_direction_qualifier"
QUALIFIED_PREDICATE = "qualified_predicate"
ASPECT_ENUM = "GeneOrGeneProductOrChemicalEntityAspectEnum"
DIRECTION_ENUM = "DirectionQualifierEnum"

KNOWLEDGE_LEVEL = "knowledge_level"
AGENT_TYPE = "agent_type"


@functools.cache
def _toolkit():
    """The Biolink model toolkit, loaded on first use (it fetches the model)."""
    return bmt.Toolkit()


@functools.cache
def is_qualifier(key):
    return _toolkit().is_qualifier(key)


@functools.cache
def is_symmetric(predicate):
    return bool(_toolkit().is_symmetric(predicate))


def jsonl_rows(path):
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rb") as stream:
        for line in stream:
            if line.strip():
                yield orjson.loads(line)


def get_filter_nodes():
    response = requests.get(BLOCKLIST_URL, timeout=60)
    response.raise_for_status()
    return set(response.json())


def extract_prov(edge):
    """Validate and return one raw edge's complete provenance."""
    edge_id = edge.get("id") or (
        f"{edge.get('subject')} {edge.get('predicate')} {edge.get('object')}"
    )
    robokop_prov = {
        key: edge[key]
        for key in ROBOKOP_PRIMARY_SOURCE_KEYS + ROBOKOP_AGGREGATOR_SOURCE_KEYS
        if key in edge
    }
    if any(key in edge for key in ROBOKOP_PRIMARY_SOURCE_KEYS):
        primary_sources = []
        for key in ROBOKOP_PRIMARY_SOURCE_KEYS:
            value = edge.get(key)
            if isinstance(value, list):
                primary_sources.extend(value)
            elif value:
                primary_sources.append(value)
        if len(primary_sources) != 1:
            raise ValueError(
                f"Edge {edge_id} must have exactly one primary_knowledge_source; "
                f"found {len(primary_sources)}"
            )
        return robokop_prov

    sources = edge.get("sources")
    if not isinstance(sources, list):
        sources = []
    if any(not isinstance(source, dict) for source in sources):
        raise ValueError(f"Edge {edge_id} has a non-object entry in sources")
    primary_sources = [
        source
        for source in sources
        if source.get("resource_role") == PRIMARY_KNOWLEDGE_SOURCE
    ]
    if len(primary_sources) != 1:
        raise ValueError(
            f"Edge {edge_id} must have exactly one primary_knowledge_source; "
            f"found {len(primary_sources)}"
        )
    if not primary_sources[0].get("resource_id"):
        raise ValueError(
            f"Edge {edge_id} has a primary_knowledge_source without a resource_id"
        )
    return sources


def _edge_property(edge, name):
    """A KGX edge's ``name`` (bare or ``biolink:``-prefixed), or None."""
    value = edge.get(name) or edge.get(f"biolink:{name}")
    return value if isinstance(value, str) and value else None


def predicate_json(edge):
    parts = {"predicate": edge["predicate"]}
    for key, value in edge.items():
        if is_qualifier(key):
            parts[key.removeprefix("biolink:")] = value
    return orjson.dumps(parts, option=orjson.OPT_SORT_KEYS).decode()


def _serialized_relation(relation):
    return orjson.dumps(relation, option=orjson.OPT_SORT_KEYS).decode()


@functools.cache
def _predicate_ancestors(predicate):
    return tuple(
        _toolkit().get_ancestors(predicate, formatted=True, reflexive=False) or ()
    )


@functools.cache
def _qualifier_ancestors(value, enum_name):
    return tuple(
        _toolkit().get_permissible_value_ancestors(
            permissible_value=value,
            enum_name=enum_name,
        )
        or ()
    )


def implied_relations(concrete_relation):
    """Return the relation signatures emitted by ORION's redundant KG transform."""
    implied = {}

    def add(relation):
        implied[_serialized_relation(relation)] = relation

    # Preserve every concrete relation even if an unrecognized qualifier value
    # has no hierarchy entry in the current Biolink model.
    add(concrete_relation)

    aspect_values = [None]
    if OBJECT_ASPECT_QUALIFIER in concrete_relation:
        aspect_values = _qualifier_ancestors(
            concrete_relation[OBJECT_ASPECT_QUALIFIER],
            ASPECT_ENUM,
        )

    direction_values = [None]
    if OBJECT_DIRECTION_QUALIFIER in concrete_relation:
        direction_values += _qualifier_ancestors(
            concrete_relation[OBJECT_DIRECTION_QUALIFIER],
            DIRECTION_ENUM,
        )

    for aspect, direction in product(aspect_values, direction_values):
        relation = concrete_relation.copy()
        if aspect:
            relation[OBJECT_ASPECT_QUALIFIER] = aspect
        else:
            relation.pop(OBJECT_ASPECT_QUALIFIER, None)
        if direction:
            relation[OBJECT_DIRECTION_QUALIFIER] = direction
        else:
            relation.pop(OBJECT_DIRECTION_QUALIFIER, None)
        add(relation)

    if OBJECT_ASPECT_QUALIFIER in concrete_relation:
        relation = concrete_relation.copy()
        relation.pop(OBJECT_ASPECT_QUALIFIER, None)
        relation.pop(OBJECT_DIRECTION_QUALIFIER, None)
        relation.pop(QUALIFIED_PREDICATE, None)
        add(relation)

    for ancestor in _predicate_ancestors(concrete_relation["predicate"]):
        relation = concrete_relation.copy()
        relation["predicate"] = ancestor
        relation.pop(OBJECT_ASPECT_QUALIFIER, None)
        relation.pop(OBJECT_DIRECTION_QUALIFIER, None)
        relation.pop(QUALIFIED_PREDICATE, None)
        add(relation)

    return tuple(implied.values())


def _insert_batch(connection, table, columns, rows):
    """Insert a Python row batch through DuckDB's vectorized Arrow scanner."""
    batch = pyarrow.table(
        {column: [row[index] for row in rows] for index, column in enumerate(columns)}
    )
    connection.register("_insert_batch", batch)
    try:
        selected_columns = ", ".join(columns)
        connection.execute(
            f"INSERT INTO {table} ({selected_columns}) "
            f"SELECT {selected_columns} FROM _insert_batch"
        )
    finally:
        connection.unregister("_insert_batch")


def _report_progress(label, count, started_at):
    elapsed = time.monotonic() - started_at
    rate = count / elapsed if elapsed else 0
    print(
        f"{label}: {count:,} rows in {elapsed:,.1f}s ({rate:,.0f} rows/s)", flush=True
    )


def _configure_connection(
    connection,
    temp_directory,
    *,
    memory_limit,
    max_temp_directory_size,
    threads,
):
    settings = {
        "memory_limit": memory_limit,
        "max_temp_directory_size": max_temp_directory_size,
        "temp_directory": str(temp_directory),
        "threads": str(threads),
        "preserve_insertion_order": "false",
        "enable_progress_bar": "false",
    }
    for name, value in settings.items():
        connection.execute(
            f"SET {name} = ?",
            [value],
        )


def _create_staging_schema(connection):
    connection.execute("""
        CREATE TABLE raw_node (
            curie VARCHAR PRIMARY KEY,
            name VARCHAR,
            categories VARCHAR[] NOT NULL
        );
        CREATE TABLE raw_edge (
            edge_order BIGINT NOT NULL,
            original_edge_id VARCHAR NOT NULL,
            subject_curie VARCHAR NOT NULL,
            object_curie VARCHAR NOT NULL,
            predicate_json VARCHAR NOT NULL,
            predicate VARCHAR NOT NULL,
            is_symmetric BOOLEAN NOT NULL,
            sources_json VARCHAR NOT NULL,
            knowledge_level VARCHAR,
            agent_type VARCHAR
        );
        CREATE TABLE raw_relation_implication (
            concrete_predicate_json VARCHAR NOT NULL,
            implied_predicate_json VARCHAR NOT NULL,
            implied_predicate VARCHAR NOT NULL,
            implied_is_symmetric BOOLEAN NOT NULL
        );
        CREATE TABLE raw_relation_hierarchy (
            descendant_predicate_json VARCHAR NOT NULL,
            ancestor_predicate_json VARCHAR NOT NULL
        );
        """)


def _create_relation_implications(connection, batch_size):
    columns = (
        "concrete_predicate_json",
        "implied_predicate_json",
        "implied_predicate",
        "implied_is_symmetric",
    )
    batch = []
    implication_count = 0
    started_at = time.monotonic()
    concrete_relations = connection.execute("""
        SELECT DISTINCT predicate_json
        FROM raw_edge
        ORDER BY predicate_json
        """).fetchall()
    for (concrete_predicate_json,) in concrete_relations:
        concrete_relation = orjson.loads(concrete_predicate_json)
        for implied_relation in implied_relations(concrete_relation):
            implied_predicate = implied_relation["predicate"]
            if implied_predicate in FILTER_PREDICATES:
                continue
            batch.append(
                (
                    concrete_predicate_json,
                    _serialized_relation(implied_relation),
                    implied_predicate,
                    is_symmetric(implied_predicate),
                )
            )
        if len(batch) >= batch_size:
            _insert_batch(
                connection,
                "raw_relation_implication",
                columns,
                batch,
            )
            implication_count += len(batch)
            batch.clear()
    if batch:
        _insert_batch(
            connection,
            "raw_relation_implication",
            columns,
            batch,
        )
        implication_count += len(batch)
    _report_progress(
        "Expanded relation signatures",
        implication_count,
        started_at,
    )


def _create_relation_hierarchy(connection, batch_size):
    """Materialize strict ancestry between query-visible relation signatures."""
    columns = (
        "descendant_predicate_json",
        "ancestor_predicate_json",
    )
    available_relations = {row[0] for row in connection.execute("""
            SELECT DISTINCT implied_predicate_json
            FROM raw_relation_implication
            """).fetchall()}
    batch = []
    hierarchy_count = 0
    started_at = time.monotonic()
    for descendant_predicate_json in sorted(available_relations):
        descendant_relation = orjson.loads(descendant_predicate_json)
        for ancestor_relation in implied_relations(descendant_relation):
            ancestor_predicate_json = _serialized_relation(ancestor_relation)
            if (
                ancestor_predicate_json == descendant_predicate_json
                or ancestor_predicate_json not in available_relations
            ):
                continue
            batch.append(
                (
                    descendant_predicate_json,
                    ancestor_predicate_json,
                )
            )
        if len(batch) >= batch_size:
            _insert_batch(
                connection,
                "raw_relation_hierarchy",
                columns,
                batch,
            )
            hierarchy_count += len(batch)
            batch.clear()
    if batch:
        _insert_batch(
            connection,
            "raw_relation_hierarchy",
            columns,
            batch,
        )
        hierarchy_count += len(batch)
    _report_progress(
        "Built relation hierarchy",
        hierarchy_count,
        started_at,
    )


def _create_feature_stats(connection):
    connection.execute("""
        CREATE TABLE feature_stats (
            category_id BIGINT NOT NULL,
            feature_id BIGINT NOT NULL,
            background_count BIGINT NOT NULL
        )
        """)
    categories = [row for row in connection.execute("""
            SELECT category_id, category
            FROM category_count
            ORDER BY category_id
            """).fetchall()]
    started_at = time.monotonic()
    total_rows = 0
    for index, (category_id, category) in enumerate(categories, start=1):
        category_started_at = time.monotonic()
        connection.execute(
            """
            INSERT INTO feature_stats
            SELECT
                ? AS category_id,
                membership.feature_id,
                count(*)::BIGINT AS background_count
            FROM membership
            JOIN node ON node.node_id = membership.member_node_id
            WHERE list_contains(node.categories, ?)
            GROUP BY
                membership.feature_id
            ORDER BY
                membership.feature_id
            """,
            [category_id, category],
        )
        category_rows = connection.execute(
            "SELECT count(*) FROM feature_stats WHERE category_id = ?",
            [category_id],
        ).fetchone()[0]
        total_rows += category_rows
        print(
            "Built feature statistics "
            f"{index}/{len(categories)} for {category}: "
            f"{category_rows:,} rows in "
            f"{time.monotonic() - category_started_at:,.1f}s",
            flush=True,
        )
    print(
        f"Built feature statistics: {total_rows:,} rows in "
        f"{time.monotonic() - started_at:,.1f}s",
        flush=True,
    )


def _create_derived_schema(connection):
    statements = (
        (
            "relation",
            """
        CREATE TABLE relation AS
        SELECT
            row_number() OVER (
                ORDER BY implied_predicate_json
            )::BIGINT AS relation_id,
            implied_predicate_json AS predicate_json,
            any_value(implied_predicate) AS predicate,
            bool_or(implied_is_symmetric) AS is_symmetric
        FROM raw_relation_implication
        GROUP BY implied_predicate_json;
        """,
        ),
        (
            "relation indexes",
            """

        ALTER TABLE relation ADD PRIMARY KEY (relation_id);
        CREATE UNIQUE INDEX relation_signature_idx ON relation(predicate_json);
        """,
        ),
        (
            "relation implications",
            """

        CREATE TABLE relation_implication AS
        SELECT DISTINCT
            concrete.relation_id AS concrete_relation_id,
            implied.relation_id AS implied_relation_id
        FROM raw_relation_implication raw
        JOIN relation concrete
          ON concrete.predicate_json = raw.concrete_predicate_json
        JOIN relation implied
          ON implied.predicate_json = raw.implied_predicate_json
        ORDER BY concrete_relation_id, implied_relation_id;

        CREATE UNIQUE INDEX relation_implication_pair_idx
            ON relation_implication(
                concrete_relation_id,
                implied_relation_id
            );
        CREATE INDEX relation_implication_implied_idx
            ON relation_implication(implied_relation_id);
        """,
        ),
        (
            "relation hierarchy",
            """

        CREATE TABLE relation_hierarchy AS
        SELECT DISTINCT
            descendant.relation_id AS descendant_relation_id,
            ancestor.relation_id AS ancestor_relation_id
        FROM raw_relation_hierarchy raw
        JOIN relation descendant
          ON descendant.predicate_json = raw.descendant_predicate_json
        JOIN relation ancestor
          ON ancestor.predicate_json = raw.ancestor_predicate_json
        ORDER BY descendant_relation_id, ancestor_relation_id;

        CREATE UNIQUE INDEX relation_hierarchy_pair_idx
            ON relation_hierarchy(
                descendant_relation_id,
                ancestor_relation_id
            );
        CREATE INDEX relation_hierarchy_ancestor_idx
            ON relation_hierarchy(ancestor_relation_id);
        """,
        ),
        (
            "nodes",
            """

        CREATE TABLE node AS
        SELECT
            row_number() OVER (ORDER BY curie)::BIGINT AS node_id,
            curie,
            name,
            categories
        FROM raw_node
        ORDER BY curie;
        """,
        ),
        (
            "node indexes",
            """

        ALTER TABLE node ADD PRIMARY KEY (node_id);
        CREATE UNIQUE INDEX node_curie_idx ON node(curie);
        """,
        ),
        (
            "facts",
            """

        CREATE TABLE fact AS
        SELECT
            row_number() OVER (
                ORDER BY
                    subject_node.node_id,
                    relation.relation_id,
                    object_node.node_id
            )::BIGINT AS fact_id,
            subject_node.node_id AS subject_node_id,
            object_node.node_id AS object_node_id,
            relation.relation_id
        FROM (
            SELECT DISTINCT subject_curie, object_curie, predicate_json
            FROM raw_edge
        ) raw
        JOIN relation USING (predicate_json)
        JOIN node subject_node ON subject_node.curie = raw.subject_curie
        JOIN node object_node ON object_node.curie = raw.object_curie
        ORDER BY
            subject_node_id,
            relation_id,
            object_node_id;
        """,
        ),
        (
            "fact indexes",
            """

        ALTER TABLE fact ADD PRIMARY KEY (fact_id);
        CREATE UNIQUE INDEX fact_semantic_idx
            ON fact(subject_node_id, relation_id, object_node_id);
        """,
        ),
        (
            "evidence",
            """

        CREATE TABLE evidence AS
        SELECT
            row_number() OVER (ORDER BY raw.edge_order)::BIGINT AS evidence_id,
            raw.original_edge_id,
            fact.fact_id,
            raw.sources_json,
            raw.knowledge_level,
            raw.agent_type
        FROM raw_edge raw
        JOIN relation USING (predicate_json)
        JOIN node subject_node ON subject_node.curie = raw.subject_curie
        JOIN node object_node ON object_node.curie = raw.object_curie
        JOIN fact
         ON fact.subject_node_id = subject_node.node_id
         AND fact.object_node_id = object_node.node_id
         AND fact.relation_id = relation.relation_id;
        """,
        ),
        (
            "evidence indexes",
            """

        ALTER TABLE evidence ADD PRIMARY KEY (evidence_id);
        CREATE INDEX evidence_fact_idx ON evidence(fact_id);
        """,
        ),
        (
            "features",
            """

        CREATE TABLE feature AS
        WITH expanded_fact AS (
            SELECT
                fact.subject_node_id,
                fact.object_node_id,
                implication.implied_relation_id AS relation_id
            FROM fact
            JOIN relation_implication implication
              ON implication.concrete_relation_id = fact.relation_id
        ),
        semantic_membership AS (
            SELECT
                expanded_fact.object_node_id AS neighbor_node_id,
                expanded_fact.relation_id,
                TRUE AS member_is_subject
            FROM expanded_fact

            UNION

            SELECT
                expanded_fact.subject_node_id AS neighbor_node_id,
                expanded_fact.relation_id,
                relation.is_symmetric AS member_is_subject
            FROM expanded_fact
            JOIN relation USING (relation_id)
            WHERE expanded_fact.subject_node_id != expanded_fact.object_node_id
               OR NOT relation.is_symmetric
        )
        SELECT
            row_number() OVER (
                ORDER BY
                    neighbor_node_id,
                    relation_id,
                    member_is_subject
            )::BIGINT AS feature_id,
            neighbor_node_id,
            relation_id,
            member_is_subject
        FROM semantic_membership
        ORDER BY feature_id;
        """,
        ),
        (
            "feature indexes",
            """

        ALTER TABLE feature ADD PRIMARY KEY (feature_id);
        """,
        ),
        (
            "feature hierarchy",
            """

        CREATE TABLE feature_hierarchy AS
        SELECT
            descendant.feature_id AS descendant_feature_id,
            ancestor.feature_id AS ancestor_feature_id
        FROM feature descendant
        JOIN relation_hierarchy hierarchy
          ON hierarchy.descendant_relation_id = descendant.relation_id
        JOIN feature ancestor
          ON ancestor.neighbor_node_id = descendant.neighbor_node_id
         AND ancestor.member_is_subject = descendant.member_is_subject
         AND ancestor.relation_id = hierarchy.ancestor_relation_id
        ORDER BY descendant_feature_id, ancestor_feature_id;

        CREATE UNIQUE INDEX feature_hierarchy_pair_idx
            ON feature_hierarchy(
                descendant_feature_id,
                ancestor_feature_id
            );
        """,
        ),
        (
            "membership",
            """

        CREATE TABLE membership AS
        WITH expanded_fact AS (
            SELECT
                fact.subject_node_id,
                fact.object_node_id,
                implication.implied_relation_id AS relation_id
            FROM fact
            JOIN relation_implication implication
              ON implication.concrete_relation_id = fact.relation_id
        ),
        semantic_membership AS (
            SELECT
                expanded_fact.subject_node_id AS member_node_id,
                expanded_fact.object_node_id AS neighbor_node_id,
                expanded_fact.relation_id,
                TRUE AS member_is_subject
            FROM expanded_fact

            UNION

            SELECT
                expanded_fact.object_node_id AS member_node_id,
                expanded_fact.subject_node_id AS neighbor_node_id,
                expanded_fact.relation_id,
                relation.is_symmetric AS member_is_subject
            FROM expanded_fact
            JOIN relation USING (relation_id)
            WHERE expanded_fact.subject_node_id != expanded_fact.object_node_id
               OR NOT relation.is_symmetric
        )
        SELECT
            semantic_membership.member_node_id,
            feature.feature_id
        FROM semantic_membership
        JOIN feature
          ON feature.neighbor_node_id = semantic_membership.neighbor_node_id
         AND feature.relation_id = semantic_membership.relation_id
         AND feature.member_is_subject = semantic_membership.member_is_subject
        ORDER BY
            semantic_membership.member_node_id,
            feature.feature_id;
        """,
        ),
        (
            "membership indexes",
            """

        CREATE INDEX membership_member_idx ON membership(member_node_id);
        """,
        ),
        (
            "category counts",
            """

        CREATE TABLE category_count AS
        SELECT
            row_number() OVER (ORDER BY category)::BIGINT AS category_id,
            category,
            node_count
        FROM (
            SELECT
                category,
                count(*)::BIGINT AS node_count
            FROM node, unnest(categories) AS category_value(category)
            GROUP BY category
        )
        ORDER BY category_id;

        ALTER TABLE category_count ADD PRIMARY KEY (category_id);
        CREATE UNIQUE INDEX category_count_category_idx
            ON category_count(category);
        """,
        ),
        (
            "metadata",
            """

        CREATE TABLE metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR NOT NULL
        );

        DROP TABLE raw_node;
        DROP TABLE raw_edge;
        DROP TABLE raw_relation_implication;
        DROP TABLE raw_relation_hierarchy;
        """,
        ),
    )
    for label, statement in statements:
        started_at = time.monotonic()
        print(f"Building {label}...", flush=True)
        connection.execute(statement)
        print(
            f"Built {label} in {time.monotonic() - started_at:,.1f}s",
            flush=True,
        )
    _create_feature_stats(connection)
    connection.execute(
        "INSERT INTO metadata VALUES ('schema_version', ?)",
        [SCHEMA_VERSION],
    )


def build_database(
    node_file,
    edge_file,
    output_path,
    *,
    blocklist=None,
    batch_size=250_000,
    memory_limit=DEFAULT_MEMORY_LIMIT,
    max_temp_directory_size=DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
    threads=DEFAULT_THREADS,
):
    """Build an immutable graphenrich DuckDB database from KGX JSONL."""
    output_path = Path(output_path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    building_path = output_path.with_suffix(output_path.suffix + ".building")
    temp_directory = building_path.with_suffix(building_path.suffix + ".tmp")
    building_path.unlink(missing_ok=True)
    shutil.rmtree(temp_directory, ignore_errors=True)

    blocked = get_filter_nodes() if blocklist is None else set(blocklist)
    connection = duckdb.connect(str(building_path))
    try:
        _configure_connection(
            connection,
            temp_directory,
            memory_limit=memory_limit,
            max_temp_directory_size=max_temp_directory_size,
            threads=threads,
        )
        _create_staging_schema(connection)

        node_columns = ("curie", "name", "categories")
        node_batch = []
        node_count = 0
        node_started_at = time.monotonic()
        next_node_report = 1_000_000
        for node in jsonl_rows(node_file):
            curie = node["id"]
            if curie.startswith("CAID") or curie in blocked:
                continue
            categories = list(dict.fromkeys(node["category"]))
            node_batch.append((curie, node.get("name"), categories))
            if len(node_batch) >= batch_size:
                _insert_batch(connection, "raw_node", node_columns, node_batch)
                node_count += len(node_batch)
                node_batch.clear()
                if node_count >= next_node_report:
                    _report_progress("Loaded nodes", node_count, node_started_at)
                    next_node_report += 1_000_000
        if node_batch:
            _insert_batch(connection, "raw_node", node_columns, node_batch)
            node_count += len(node_batch)
        _report_progress("Loaded nodes", node_count, node_started_at)

        edge_columns = (
            "edge_order",
            "original_edge_id",
            "subject_curie",
            "object_curie",
            "predicate_json",
            "predicate",
            "is_symmetric",
            "sources_json",
            "knowledge_level",
            "agent_type",
        )
        edge_batch = []
        raw_edge_count = 0
        edge_started_at = time.monotonic()
        next_edge_report = 1_000_000
        for edge_order, edge in enumerate(jsonl_rows(edge_file), start=1):
            provenance = extract_prov(edge)
            subject = edge["subject"]
            object_ = edge["object"]
            predicate = edge["predicate"]
            if (
                subject.startswith("CAID")
                or object_.startswith("CAID")
                or subject in blocked
                or object_ in blocked
                or predicate in FILTER_PREDICATES
            ):
                continue
            edge_batch.append(
                (
                    edge_order,
                    edge.get("id") or f"kgx-edge-{edge_order}",
                    subject,
                    object_,
                    predicate_json(edge),
                    predicate,
                    is_symmetric(predicate),
                    orjson.dumps(provenance).decode(),
                    _edge_property(edge, KNOWLEDGE_LEVEL),
                    _edge_property(edge, AGENT_TYPE),
                )
            )
            if len(edge_batch) >= batch_size:
                _insert_batch(connection, "raw_edge", edge_columns, edge_batch)
                raw_edge_count += len(edge_batch)
                edge_batch.clear()
                if raw_edge_count >= next_edge_report:
                    _report_progress(
                        "Loaded edges",
                        raw_edge_count,
                        edge_started_at,
                    )
                    next_edge_report += 1_000_000
        if edge_batch:
            _insert_batch(connection, "raw_edge", edge_columns, edge_batch)
            raw_edge_count += len(edge_batch)
        _report_progress("Loaded edges", raw_edge_count, edge_started_at)

        missing = connection.execute("""
            SELECT curie
            FROM (
                SELECT subject_curie AS curie FROM raw_edge
                UNION
                SELECT object_curie AS curie FROM raw_edge
            ) endpoint
            ANTI JOIN raw_node USING (curie)
            LIMIT 10
            """).fetchall()
        if missing:
            missing_curies = ", ".join(row[0] for row in missing)
            raise ValueError(
                f"Edges reference nodes absent from the node file: {missing_curies}"
            )

        _create_relation_implications(connection, batch_size)
        _create_relation_hierarchy(connection, batch_size)
        _create_derived_schema(connection)
        connection.execute(
            "INSERT INTO metadata VALUES ('node_count', ?), ('raw_edge_count', ?)",
            [str(node_count), str(raw_edge_count)],
        )
        connection.execute("ANALYZE")
        connection.execute("CHECKPOINT")
    except Exception:
        connection.close()
        building_path.unlink(missing_ok=True)
        shutil.rmtree(temp_directory, ignore_errors=True)
        raise
    else:
        connection.close()
        shutil.rmtree(temp_directory, ignore_errors=True)
        os.replace(building_path, output_path)

    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Build a graphenrich DuckDB database from KGX JSONL files."
    )
    parser.add_argument("--nodes", required=True, help="KGX node JSONL or JSONL.GZ")
    parser.add_argument("--edges", required=True, help="KGX edge JSONL or JSONL.GZ")
    parser.add_argument("--output", required=True, help="Output .duckdb path")
    parser.add_argument(
        "--skip-blocklist",
        action="store_true",
        help="Do not download and apply the Translator blocklist.",
    )
    parser.add_argument(
        "--memory-limit",
        default=os.getenv("AC_DUCKDB_BUILD_MEMORY_LIMIT", DEFAULT_MEMORY_LIMIT),
        help=f"DuckDB build memory limit (default: {DEFAULT_MEMORY_LIMIT}).",
    )
    parser.add_argument(
        "--max-temp-directory-size",
        default=os.getenv(
            "AC_DUCKDB_BUILD_MAX_TEMP_DIRECTORY_SIZE",
            DEFAULT_MAX_TEMP_DIRECTORY_SIZE,
        ),
        help=(
            "Maximum DuckDB spill space "
            f"(default: {DEFAULT_MAX_TEMP_DIRECTORY_SIZE})."
        ),
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=int(os.getenv("AC_DUCKDB_BUILD_THREADS", DEFAULT_THREADS)),
        help=f"DuckDB build threads (default: {DEFAULT_THREADS}).",
    )
    args = parser.parse_args()
    build_database(
        args.nodes,
        args.edges,
        args.output,
        blocklist=set() if args.skip_blocklist else None,
        memory_limit=args.memory_limit,
        max_temp_directory_size=args.max_temp_directory_size,
        threads=args.threads,
    )


if __name__ == "__main__":
    main()
