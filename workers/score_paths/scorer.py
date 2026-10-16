"""Score a TRAPI message's paths with the sequence reranker.

Everything the reranker reads is in one LMDB, written by squashbert's
scripts/export_reranker.py under the keys below: the TorchScript model, its
config, and the vectors and context tables for the graph's nodes, categories
and hop phrases. A path is read the way squashbert's training paths were
written, so it looks up the vectors it was trained on; squashbert's tests
check that reading and the scores, which is why this module imports nothing
from shepherd_utils.
"""

import functools
import hashlib
import io
import json
import re
import time

import numpy as np
import torch
from bmt import Toolkit

NODE, CATEGORY, PHRASE = "node", "category", "phrase"
NODE_CONTEXT, EDGE_CONTEXT = "node_context", "edge_context"
# Nodes and categories keep the SAPBERT caches' float16
DTYPES = {
    NODE: np.float16,
    CATEGORY: np.float16,
    PHRASE: np.float32,
    NODE_CONTEXT: np.float32,
    EDGE_CONTEXT: np.float32,
}
MODEL_KEY = b"reranker:model.pt"
CONFIG_KEY = b"reranker:config.json"
# LMDB refuses keys over 511 bytes
MAX_KEY_BYTES = 480

SCHEMA_URL = (
    "https://raw.githubusercontent.com/biolink/biolink-model/"
    "refs/tags/v{version}/biolink-model.yaml"
)
PREDICATE_MAP_URL = (
    "https://raw.githubusercontent.com/biolink/biolink-model/"
    "refs/tags/v{version}/predicate_mapping.yaml"
)

ONTOLOGY_PREDICATES = {"biolink:subclass_of", "biolink:superclass_of"}
# The qualifiers that decide how an edge reads
READING_SLOTS = (
    "qualified_predicate",
    "object_aspect_qualifier",
    "object_direction_qualifier",
    "subject_aspect_qualifier",
    "subject_direction_qualifier",
)
# A qualifier value a hop label can hold
_VALUE = re.compile(r"[\w.:-]+( [\w.:-]+)*")

N_HOPS_SCALE = 8.0
# The node table's columns: out-degree, in-degree, IC, IC present
NODE_CONTEXT_DIM = 4
_NODE_COLUMNS = (
    ("deg_out_max", 0, np.max),
    ("deg_out_mean", 0, np.mean),
    ("deg_in_max", 1, np.max),
    ("deg_in_mean", 1, np.mean),
    ("ic_mean", 2, np.mean),
    ("ic_present_frac", 3, np.mean),
)
_EDGE_BASES = (
    "edge_count",
    "pubs",
    "primary_sources",
    "distinct_sources",
    "attributes_seen",
)


def lmdb_key(kind: str, text: str) -> bytes:
    key = f"{kind}:{text}".encode()
    if len(key) > MAX_KEY_BYTES:
        key = f"{kind}#".encode() + hashlib.sha256(key).hexdigest().encode()
    return key


def edge_context_key(a: str, b: str) -> bytes:
    """Keyed by the pair's 64-bit blake2b digest, as squashbert's PairTable
    stores every pair of the graph."""
    pair = "|".join(sorted((a, b)))
    digest = hashlib.blake2b(pair.encode(), digest_size=8).digest()
    return f"{EDGE_CONTEXT}#".encode() + digest


def load_reranker(txn):
    """`(TorchScript model, config)` from the LMDB."""
    blob, config = txn.get(MODEL_KEY), txn.get(CONFIG_KEY)
    if blob is None or config is None:
        raise RuntimeError(
            "the embeddings LMDB holds no reranker; write one with squashbert's "
            "scripts/export_reranker.py"
        )
    model = torch.jit.load(io.BytesIO(blob), map_location="cpu")
    model.eval()
    config = json.loads(config)
    config["length_scale"] = {
        int(length): (float(mean), float(spread))
        for length, (mean, spread) in config["length_scale"].items()
    }
    return model, config


def load_toolkit(config) -> Toolkit:
    """The biolink release the training paths were read by."""
    version = config["biolink_version"]
    return Toolkit(
        schema=SCHEMA_URL.format(version=version),
        predicate_map=PREDICATE_MAP_URL.format(version=version),
    )


def predicate_name(predicate: str) -> str:
    return predicate.removeprefix("biolink:").replace("_", " ")


def predicate_curie(name: str) -> str:
    return "biolink:" + name.replace(" ", "_")


# A bmt lookup takes about a millisecond and a message asks the same few
# thousands of times, so answers are kept for the life of the process.
@functools.lru_cache(maxsize=None)
def inverse_predicate(bmt, predicate: str) -> str | None:
    # squashbert's stand-in for an inverse the model lacks
    if predicate in ("biolink:has_chemical_role", "has_chemical_role"):
        return "biolink:is_chemical_role_of"
    if predicate in ("biolink:is_chemical_role_of", "is_chemical_role_of"):
        return "biolink:has_chemical_role"
    name = predicate_name(predicate)
    if not bmt.is_predicate(name):
        return None
    if bmt.is_symmetric(name):
        return predicate_curie(name)
    try:
        inverse = bmt.get_inverse(name)
    except AttributeError:
        # bmt raises on a declared inverse the schema does not hold
        return None
    return None if inverse is None else predicate_curie(inverse)


@functools.lru_cache(maxsize=None)
def most_specific_category(bmt, categories: tuple[str, ...]) -> str:
    """The deepest biolink class, else the first mixin; not the first listed,
    which the translator KGX's alphabetical lists make BiologicalEntity."""
    best = None
    for category in categories:
        depth = bmt.get_element_depth(category) if bmt.is_category(category) else None
        if depth is not None and (best is None or depth > best[0]):
            best = (depth, category)
    if best is not None:
        chosen = best[1]
    else:
        mixins = [category for category in categories if bmt.is_mixin(category)]
        if not mixins:
            raise ValueError(f"No biolink categories found in {categories!r}")
        chosen = mixins[0]
    if chosen.startswith("biolink:"):
        return chosen
    return "biolink:" + chosen.replace(" ", "")


def _worded(verb: str, aspect, direction, walked_back: bool) -> str:
    word = predicate_name(verb)
    if aspect is None:
        return word
    change = aspect.replace("_", " ")
    if direction is not None:
        change = f"{direction.replace('_', ' ')} {change}"
    return f"has {change} {word}" if walked_back else f"{word} {change} of"


def edge_phrase(bmt, edge: dict, backward: bool) -> str | None:
    """The phrase a TRAPI edge reads as, walked as stored or backwards; None
    where it cannot be walked that way."""
    predicate = edge["predicate"]
    found = {
        item["qualifier_type_id"].removeprefix("biolink:"): item["qualifier_value"]
        for item in edge.get("qualifiers") or []
    }
    slots = {
        slot: str(found[slot]) for slot in READING_SLOTS if found.get(slot) is not None
    }
    verb, aspect, direction = predicate, None, None
    # Subject-side qualifiers, and a direction without an aspect, have no
    # wording: such an edge reads as its bare predicate
    unworded = (
        slots.get("subject_aspect_qualifier") is not None
        or slots.get("subject_direction_qualifier") is not None
        or (
            slots.get("object_direction_qualifier") is not None
            and slots.get("object_aspect_qualifier") is None
        )
    )
    if not unworded:
        aspect = slots.get("object_aspect_qualifier")
        direction = slots.get("object_direction_qualifier")
        qualified = slots.get("qualified_predicate")
        if qualified is not None and (direction is not None or aspect is None):
            verb = predicate_curie(predicate_name(qualified))
        for value in (aspect, direction):
            if value is not None and not _VALUE.fullmatch(value):
                raise ValueError(f"qualifier value {value!r} cannot be read")
    if not backward:
        return _worded(verb, aspect, direction, False)
    # Only the predicate decides whether an edge can be walked backwards; a
    # qualified predicate with no inverse reads as the bare inverse
    plain = inverse_predicate(bmt, predicate)
    if plain is None:
        return None
    inverse = inverse_predicate(bmt, verb)
    if inverse is None:
        return _worded(plain, None, None, False)
    return _worded(inverse, aspect, direction, True)


def hop_phrase(bmt, current: str, following: str, edge_ids, edges: dict) -> str | None:
    """Every edge of the path between two nodes, read from `current`; None
    where none can be walked across."""
    carried = {}
    for edge_id in edge_ids:
        edge = edges.get(edge_id)
        if not edge or edge["predicate"] in ONTOLOGY_PREDICATES:
            continue
        if edge["subject"] == current and edge["object"] == following:
            backward = False
        elif edge["object"] == current and edge["subject"] == following:
            backward = True
        else:
            continue
        phrase = edge_phrase(bmt, edge, backward)
        if phrase is None:
            continue
        negated = False
        for attribute in edge.get("attributes") or ():
            if (
                attribute.get("original_attribute_name") == "negated"
                or attribute.get("attribute_type_id") == "biolink:negated"
            ):
                value = attribute.get("value")
                if value is not None and not isinstance(value, bool):
                    raise ValueError(f"negated is {value!r}; expected a boolean")
                negated = bool(value)
                break
        # A label only NOT annotations carry is dropped
        carried[phrase] = carried.get(phrase, False) or not negated
    phrases = sorted(phrase for phrase, kept in carried.items() if kept)
    if not phrases:
        return None
    if len(phrases) == 1:
        return phrases[0]
    return "[" + " or ".join(phrases) + "]"


def order_path_nodes(source: str, target: str, edge_ids, edges: dict) -> list[str]:
    ordered = [source]
    while target not in ordered:
        progress = False
        for edge_id in edge_ids:
            edge = edges.get(edge_id)
            if not edge:
                continue
            tail = ordered[-1]
            if edge["subject"] == tail and edge["object"] not in ordered:
                ordered.append(edge["object"])
                progress = True
            elif edge["object"] == tail and edge["subject"] not in ordered:
                ordered.append(edge["subject"])
                progress = True
        if not progress:
            raise ValueError(
                f"Could not construct path from {source} to {target} from given edges"
            )
    return ordered


def read_path(bmt, source: str, target: str, edge_ids, knowledge_graph: dict):
    """`(node CURIEs, their categories, hop phrases)`; ValueError for a path
    that cannot be read."""
    nodes, edges = knowledge_graph["nodes"], knowledge_graph["edges"]
    ordered = order_path_nodes(source, target, edge_ids, edges)
    categories = []
    for curie in ordered:
        node = nodes.get(curie)
        if node is None:
            raise ValueError(f"{curie} is not in the knowledge graph")
        found = node.get("categories") or []
        categories.append(
            most_specific_category(bmt, tuple(found)) if found else "biolink:NamedThing"
        )
    phrases = []
    for current, following in zip(ordered[:-1], ordered[1:]):
        phrase = hop_phrase(bmt, current, following, edge_ids, edges)
        if phrase is None:
            raise ValueError(
                f"no edge of the path can be walked from {current} to {following}"
            )
        phrases.append(phrase)
    return ordered, categories, phrases


def path_scalars(scalar_names, node_curies, node_context=None, edge_context=None):
    """One path's scalars in `scalar_names` order. `node_context(curie)` and
    `edge_context(a, b)` give a table's vector, zeros where it has none; a
    table not given leaves its scalars zero, as is the retrieval cost g."""
    out = np.zeros(len(scalar_names), dtype=np.float32)
    col = {name: j for j, name in enumerate(scalar_names)}
    out[col["n_hops_scaled"]] = (len(node_curies) - 1) / N_HOPS_SCALE
    if node_context is not None:
        vecs = np.stack([node_context(c) for c in node_curies])
        for name, j, reduce in _NODE_COLUMNS:
            out[col[name]] = reduce(vecs[:, j])
    if edge_context is not None:
        hops = np.stack(
            [edge_context(a, b) for a, b in zip(node_curies[:-1], node_curies[1:])]
        )
        for j, base in enumerate(_EDGE_BASES):
            out[col[f"{base}_mean"]] = hops[:, j].mean()
            out[col[f"{base}_min"]] = hops[:, j].min()
    return out


def vector(txn, kind: str, text: str) -> np.ndarray:
    raw = txn.get(lmdb_key(kind, text))
    if raw is None:
        raise KeyError(f"{kind}:{text}")
    return np.frombuffer(raw, dtype=DTYPES[kind])


def path_features(txn, config, nodes, categories, phrases, kept=None):
    """One path's `(node_x, edge_x, scalars)`; KeyError naming the first
    vector the LMDB lacks. `kept` carries the category and phrase vectors,
    as float32, from one path of a message to the next: there are few, read
    over and over. Nodes are not kept, so it stays small."""
    dim = config["embed_dim"]
    kept = {} if kept is None else kept

    def repeated(kind, text):
        if (kind, text) not in kept:
            kept[kind, text] = vector(txn, kind, text).astype(np.float32)
        return kept[kind, text]

    node_x = np.empty((len(nodes), 2 * dim), dtype=np.float32)
    for j, (curie, category) in enumerate(zip(nodes, categories)):
        node_x[j, :dim] = vector(txn, NODE, curie)
        node_x[j, dim:] = repeated(CATEGORY, category)
    edge_x = np.stack([repeated(PHRASE, phrase) for phrase in phrases])

    def context(key, width):
        raw = txn.get(key)
        if raw is None:
            return np.zeros(width, dtype=np.float32)
        return np.frombuffer(raw, dtype=np.float32)

    scalars = path_scalars(
        config["scalar_names"],
        nodes,
        (
            (lambda c: context(lmdb_key(NODE_CONTEXT, c), NODE_CONTEXT_DIM))
            if config["node_context"]
            else None
        ),
        (
            (lambda a, b: context(edge_context_key(a, b), len(_EDGE_BASES)))
            if config["edge_context"]
            else None
        ),
    )
    return node_x, edge_x, scalars


def is_direct_path(source, target, path, knowledge_graph):
    """Whether every edge of the path joins the two pinned nodes."""
    edges = knowledge_graph["edges"]
    return bool(path) and all(
        eid in edges
        and {edges[eid]["subject"], edges[eid]["object"]} == {source, target}
        for eid in path
    )


def score_message(message, txn, model, config, bmt, chunk_size):
    """Give every analysis a `score` in place, and return counts for the log.

    The direct edge scores 1. Other paths are scored a length at a time, at
    most `chunk_size` at once, on the checkpoint's length scale -- how paths
    of different lengths are ranked together -- through a logistic into
    (0, 1). A path that cannot be read, has no length scale, or lacks a
    vector keeps 0.
    """
    stats = {
        "analyses": 0,
        "scored": 0,
        "direct": 0,
        "no_binding": 0,
        "bad_path": 0,
        "no_scale": 0,
        "missing": 0,
        "missing_samples": [],
        "model_seconds": 0.0,
    }
    msg = message["message"]
    knowledge_graph = msg["knowledge_graph"]
    auxiliary_graphs = msg.get("auxiliary_graphs") or {}
    qpath_id, qpath = next(iter(msg["query_graph"]["paths"].items()))
    pending, kept = {}, {}

    def flush(n_hops):
        batch = pending.pop(n_hops, [])
        if not batch:
            return
        started = time.time()
        node_x, edge_x, scalars = (
            np.stack(column) for column in zip(*(features for _, features in batch))
        )
        with torch.inference_mode():
            raw = model(
                torch.from_numpy(node_x),
                torch.from_numpy(edge_x),
                torch.full((len(batch),), n_hops, dtype=torch.int64),
                torch.from_numpy(scalars),
                torch.zeros(len(batch), dtype=torch.int64),
            )
        raw = raw.float().numpy()
        stats["model_seconds"] += time.time() - started
        mean, spread = config["length_scale"][n_hops]
        z = ((raw.astype(np.float64) - mean) / spread).astype(np.float32)
        scores = 1.0 / (1.0 + np.exp(-z.astype(np.float64)))
        for (analysis, _), score in zip(batch, scores):
            analysis["score"] = float(score)
        stats["scored"] += len(batch)

    for result in msg.get("results") or []:
        analyses = result.get("analyses") or []
        for analysis in analyses:
            analysis["score"] = 0.0
            stats["analyses"] += 1
        try:
            source = result["node_bindings"][qpath["subject"]][0]["id"]
            target = result["node_bindings"][qpath["object"]][0]["id"]
        except (KeyError, IndexError, TypeError):
            stats["no_binding"] += len(analyses)
            continue
        for analysis in analyses:
            try:
                path_bindings = (analysis.get("path_bindings") or {}).get(qpath_id)
                aux_id = path_bindings[0]["id"]
                edge_ids = auxiliary_graphs[aux_id]["edges"]
            except (KeyError, IndexError, TypeError):
                stats["no_binding"] += 1
                continue
            if is_direct_path(source, target, edge_ids, knowledge_graph):
                analysis["score"] = 1.0
                stats["direct"] += 1
                continue
            try:
                nodes, categories, phrases = read_path(
                    bmt, source, target, edge_ids, knowledge_graph
                )
            except ValueError:
                stats["bad_path"] += 1
                continue
            n_hops = len(phrases)
            if n_hops not in config["length_scale"]:
                stats["no_scale"] += 1
                continue
            try:
                features = path_features(txn, config, nodes, categories, phrases, kept)
            except KeyError as e:
                stats["missing"] += 1
                samples = stats["missing_samples"]
                if len(samples) < 5 and e.args[0] not in samples:
                    samples.append(e.args[0])
                continue
            pending.setdefault(n_hops, []).append((analysis, features))
            if len(pending[n_hops]) >= chunk_size:
                flush(n_hops)
    for n_hops in sorted(pending):
        flush(n_hops)
    return stats
