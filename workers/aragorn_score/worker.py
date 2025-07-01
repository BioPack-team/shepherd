"""Aragorn ARA scoring module."""

import asyncio
import copy
import json
import logging
import math
import time
import uuid
from collections import defaultdict
from itertools import combinations

import numpy as np

from shepherd_utils.db import get_message, get_query_state, save_message
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "aragorn.score"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]


DEFAULT_WEIGHT = 1e-2

BLENDED_PROFILE = {
    "source_weights": {
        "infores:omnicorp": {
            "literature_co-occurrence": 1,
        },
        "infores:text-mining-provider-targeted": {
            "publications": 0.5,
            "confidence_score": 1,
        },
        "infores:genetics-data-provider": {"p_value": 1},
        "infores:icees-kg": {"p_value": 1},
        "infores:gwas-catalog": {"p_value": 1},
    },
    "source_transformation": {
        "infores:omnicorp": {
            "literature_co-occurrence": {
                "lower": -1,
                "upper": 1,
                "midpoint": 0,
                "rate": 0.00033,
            }
        },
        "infores:text-mining-provider-targeted": {
            "publications": {"lower": -1, "upper": 1, "midpoint": 0, "rate": 4e-4},
            "confidence_score": {"lower": -1, "upper": 1, "midpoint": 0, "rate": 3},
        },
        "infores:genetics-data-provider": {
            "p_value": {"lower": 0, "upper": 1, "midpoint": 0.05, "rate": -150}
        },
        "infores:icees-kg": {
            "p_value": {"lower": 0, "upper": 1, "midpoint": 0.05, "rate": -150}
        },
        "infores:gwas-catalog": {
            "p_value": {"lower": 0, "upper": 1, "midpoint": 0.05, "rate": -150}
        },
    },
    "unknown_source_weight": {
        "publications": 1,
        "literature_co-occurrence": 1,
        "affinity": 1,
        "unknown_property": 0,
    },
    "unknown_source_transformation": {
        "publications": {"lower": -1, "upper": 1, "midpoint": 0, "rate": 0.574213221},
        "literature_co-occurrence": {
            "lower": -1,
            "upper": 1,
            "midpoint": 0,
            "rate": 0.001373265360835,
        },
        "affinity": {"lower": -1, "upper": 1, "midpoint": 0, "rate": 0.4},
        "unknown_property": {"lower": 0, "upper": 0, "midpoint": 0, "rate": 0},
    },
    "base_weights": {
        "default_weight": 1e-2,
        "infores:omnicorp": 0,
        "infores:drugcentral": 5e-2,
        "infores:hetionet": 3e-2,
        "infores:text-mining-provider-targeted": 5e-3,
        "infores:icees-kg": 3e-2,
        "infores:gwas-catalog": 3e-2,
    },
    "omnicorp_relevence": 0.0025,
}

CURATED_PROFILE = {
    "source_weights": {
        "infores:automat-pharos": {
            "publications": 0.7,
        },
        "infores:aragorn-ranker-ara": {
            "publications": 0.25,
        },
        "infores:semmeddb": {
            "publications": 0.05,
        },
    },
    "source_transformation": {
        "infores:automat-pharos": {
            "publications": {
                "lower": 0,
                "upper": 1,
                "midpoint": 0,
                "rate": 0.574213221,
            },
            "p_value": {
                "lower": 1,
                "upper": 0,
                "midpoint": 0.055,
                "rate": 200.574213221,
            },
        },
        "infores:aragorn-ranker-ara": {
            "publications": {
                "lower": 0,
                "upper": 1,
                "midpoint": 0,
                "rate": 0.574213221,
            },
            "p_value": {
                "lower": 1,
                "upper": 0,
                "midpoint": 0.055,
                "rate": 200.574213221,
            },
        },
        "infores:semmeddb": {
            "publications": {
                "lower": 0,
                "upper": 1,
                "midpoint": 0,
                "rate": 0.574213221,
            },
            "p_value": {
                "lower": 1,
                "upper": 0,
                "midpoint": 0.055,
                "rate": 200.574213221,
            },
        },
    },
    "unknown_source_weight": {"publications": 1, "unknown_property": 0},
    "unknown_source_transformation": {
        "publications": {"lower": 0, "upper": 1, "midpoint": 0, "rate": 0.574213221},
        "p_value": {"lower": 1, "upper": 0, "midpoint": 0.055, "rate": 200.574213221},
        "unknown_property": {
            "lower": 0,
            "upper": 1,
            "midpoint": 0,
            "rate": 0.574213221,
        },
    },
    "base_weights": {"default_weight": 1e-2},
    "omnicorp_relevence": 0.0025,
}

CORRELATED_PROFILE = {
    "source_weights": {
        "infores:automat-pharos": {
            "publications": 0.7,
        },
        "infores:aragorn-ranker-ara": {
            "publications": 0.25,
        },
        "infores:semmeddb": {
            "publications": 0.05,
        },
    },
    "source_transformation": {
        "infores:automat-pharos": {
            "publications": {
                "lower": 0,
                "upper": 1,
                "midpoint": 0,
                "rate": 0.574213221,
            },
            "p_value": {
                "lower": 1,
                "upper": 0,
                "midpoint": 0.055,
                "rate": 200.574213221,
            },
        },
        "infores:aragorn-ranker-ara": {
            "publications": {
                "lower": 0,
                "upper": 1,
                "midpoint": 0,
                "rate": 0.574213221,
            },
            "p_value": {
                "lower": 1,
                "upper": 0,
                "midpoint": 0.055,
                "rate": 200.574213221,
            },
        },
        "infores:semmeddb": {
            "publications": {
                "lower": 0,
                "upper": 1,
                "midpoint": 0,
                "rate": 0.574213221,
            },
            "p_value": {
                "lower": 1,
                "upper": 0,
                "midpoint": 0.055,
                "rate": 200.574213221,
            },
        },
    },
    "unknown_source_weight": {"publications": 1, "unknown_property": 0},
    "unknown_source_transformation": {
        "publications": {"lower": 0, "upper": 1, "midpoint": 0, "rate": 0.574213221},
        "p_value": {"lower": 1, "upper": 0, "midpoint": 0.055, "rate": 200.574213221},
        "unknown_property": {
            "lower": 0,
            "upper": 1,
            "midpoint": 0,
            "rate": 0.574213221,
        },
    },
    "base_weights": {"default_weight": 1e-2},
    "omnicorp_relevence": 0.0025,
}

CLINICAL_PROFILE = {
    "source_weights": {
        "infores:automat-pharos": {
            "publications": 0.7,
        },
        "infores:aragorn-ranker-ara": {
            "publications": 0.25,
        },
        "infores:semmeddb": {
            "publications": 0.05,
        },
    },
    "source_transformation": {
        "infores:automat-pharos": {
            "publications": {
                "lower": 0,
                "upper": 1,
                "midpoint": 0,
                "rate": 0.574213221,
            },
            "p_value": {
                "lower": 1,
                "upper": 0,
                "midpoint": 0.055,
                "rate": 200.574213221,
            },
        },
        "infores:aragorn-ranker-ara": {
            "publications": {
                "lower": 0,
                "upper": 1,
                "midpoint": 0,
                "rate": 0.574213221,
            },
            "p_value": {
                "lower": 1,
                "upper": 0,
                "midpoint": 0.055,
                "rate": 200.574213221,
            },
        },
        "infores:semmeddb": {
            "publications": {
                "lower": 0,
                "upper": 1,
                "midpoint": 0,
                "rate": 0.574213221,
            },
            "p_value": {
                "lower": 1,
                "upper": 0,
                "midpoint": 0.055,
                "rate": 200.574213221,
            },
        },
    },
    "unknown_source_weight": {"publications": 1, "unknown_property": 0},
    "unknown_source_transformation": {
        "publications": {"lower": 0, "upper": 1, "midpoint": 0, "rate": 0.574213221},
        "p_value": {"lower": 1, "upper": 0, "midpoint": 0.055, "rate": 200.574213221},
        "unknown_property": {
            "lower": 0,
            "upper": 1,
            "midpoint": 0,
            "rate": 0.574213221,
        },
    },
    "base_weights": {"default_weight": 1e-2},
    "omnicorp_relevence": 0.0025,
}


def get_base_weight(source, base_weights=BLENDED_PROFILE["base_weights"]):
    return base_weights.get(source, base_weights.get("default_weight", DEFAULT_WEIGHT))


def get_source_weight(
    source,
    property,
    source_weights=BLENDED_PROFILE["source_weights"],
    unknown_source_weight=BLENDED_PROFILE["unknown_source_weight"],
):
    return source_weights.get(source, unknown_source_weight).get(
        property, unknown_source_weight["unknown_property"]
    )


def get_source_sigmoid(
    value,
    source="unknown",
    property="unknown",
    source_transformation=BLENDED_PROFILE["source_transformation"],
    unknown_source_transformation=BLENDED_PROFILE["unknown_source_transformation"],
):
    """
    0-centered sigmoid used to map the number of publications found by a source
    to its weight. For all unknown sources, this function evaluates to 0.

    Args:
        num_pubs (int): Number of publications from source.
        steepness (float): Parameter in [0, inf) that specifies the steepness
            of the sigmoid.

    Returns:
        Weight associated with `effective_pubs` publications (using sigmoid)
    """
    parameters = source_transformation.get(source, unknown_source_transformation).get(
        property, unknown_source_transformation["unknown_property"]
    )
    upper = parameters["upper"]
    lower = parameters["lower"]
    midpoint = parameters["midpoint"]
    rate = parameters["rate"]
    return lower + ((upper - lower) / (1 + math.exp(-1 * rate * (value - midpoint))))


def get_profile(profile="blended"):
    if profile == "clinical":
        source_weights = CLINICAL_PROFILE["source_weights"]
        unknown_source_weight = CLINICAL_PROFILE["unknown_source_weight"]
        source_transformation = CLINICAL_PROFILE["source_transformation"]
        unknown_source_transformation = CLINICAL_PROFILE[
            "unknown_source_transformation"
        ]
        base_weights = CLINICAL_PROFILE["base_weights"]
    elif profile == "correlated":
        source_weights = CORRELATED_PROFILE["source_weights"]
        unknown_source_weight = CORRELATED_PROFILE["unknown_source_weight"]
        source_transformation = CORRELATED_PROFILE["source_transformation"]
        unknown_source_transformation = CORRELATED_PROFILE[
            "unknown_source_transformation"
        ]
        base_weights = CORRELATED_PROFILE["base_weights"]

    elif profile == "curated":
        source_weights = CURATED_PROFILE["source_weights"]
        unknown_source_weight = CURATED_PROFILE["unknown_source_weight"]
        source_transformation = CURATED_PROFILE["source_transformation"]
        unknown_source_transformation = CURATED_PROFILE["unknown_source_transformation"]
        base_weights = CURATED_PROFILE["base_weights"]

    else:
        source_weights = BLENDED_PROFILE["source_weights"]
        unknown_source_weight = BLENDED_PROFILE["unknown_source_weight"]
        source_transformation = BLENDED_PROFILE["source_transformation"]
        unknown_source_transformation = BLENDED_PROFILE["unknown_source_transformation"]
        base_weights = BLENDED_PROFILE["base_weights"]

    return (
        source_weights,
        unknown_source_weight,
        source_transformation,
        unknown_source_transformation,
        base_weights,
    )


class Ranker:
    """
    Aragorn Ranker.
    """

    DEFAULT_WEIGHT = -1 / np.log(1e-3)

    def __init__(self, message, logger: logging.Logger, profile="blended"):
        """Create ranker."""
        self.logger = logger

        # Decompose message
        self.kgraph = message.get("knowledge_graph", {"nodes": {}, "edges": {}})
        self.qgraph = message.get("query_graph", {"nodes": {}, "edges": {}})
        self.agraphs = message.get("auxiliary_graphs", {})

        # Apply profile
        (
            source_weights,
            unknown_source_weight,
            source_transformation,
            unknown_source_transformation,
            base_weights,
        ) = get_profile(profile)

        self.source_weights = source_weights
        self.unknown_source_weight = unknown_source_weight
        self.source_transformation = source_transformation
        self.unknown_source_transformation = unknown_source_transformation
        self.base_weights = base_weights

        # Initialize the caches for nodes and edges
        # These are used to find numeric values of edges
        self.node_pubs = dict()
        self.edge_values = dict()

    def rank(self, answers, jaccard_like=False):
        """Generate a sorted list and scores for a set of subgraphs."""

        # get subgraph statistics
        answers_ = []
        scores_for_sort = []

        for answer in answers:
            scored_answer, _ = self.score(answer, jaccard_like=jaccard_like)
            answers_.append(scored_answer)
            # get the highest scored analysis for this answer
            scores_for_sort.append(
                max(
                    analysis["score"]
                    for analysis in answer.get("analyses", [{"score": 0}])
                )
            )
        # order the answers by score (rank)
        answers = [
            answer
            for score, answer in sorted(
                zip(scores_for_sort, answers),
                key=lambda score_answer_pair: score_answer_pair[0],
            )
        ]
        return answers

    def probes(self):

        # Identify Probes
        #################
        # Q Graph Connectivity Matrix
        # put IDs in list to maintain order
        q_node_ids = list(self.qgraph["nodes"].keys())
        n_q_nodes = len(q_node_ids)
        # initialize and fill connectivity matrix
        q_conn = np.full((n_q_nodes, n_q_nodes), 0)
        for e in self.qgraph["edges"].values():
            e_sub = q_node_ids.index(e["subject"])
            e_obj = q_node_ids.index(e["object"])
            if e_sub is not None and e_obj is not None:
                q_conn[e_sub, e_obj] = 1

        # Determine probes based on connectivity
        node_conn = np.sum(q_conn, 0) + np.sum(q_conn, 1).T
        probe_nodes = []
        for conn in range(np.max(node_conn)):
            is_this_conn = node_conn == (conn + 1)
            probe_nodes += list(np.where(is_this_conn)[0])
            if len(probe_nodes) > 1:
                break
        q_probes = list(combinations(probe_nodes, 2))

        # Convert probes back to q_node_ids
        probes = [(q_node_ids[p[0]], q_node_ids[p[1]]) for p in q_probes]

        return probes

    def score(self, answer, jaccard_like=False):
        """Compute answer score."""

        # Probes used for scoring are based on the q_graph
        # This will be a list of q_node_id tuples
        probes = self.probes()

        # The r_graph is all of the information we need to score each analysis
        # This includes walking through all of the support graphs
        # And organizing nodes and edges into a more manageable form scoring
        # There is some repeated work accross analyses so we calculate all r_graphs
        # at once
        r_graphs = self.get_rgraph(answer)

        # For each analysis we have a unique r_graph to score
        analysis_details = []
        for i_analysis, r_graph in enumerate(r_graphs):
            # First we calculate the graph laplacian
            # The probes are needed to make sure we don't remove anything
            # that we actually wanted to use for scoring
            laplacian, probe_inds, laplacian_details = self.graph_laplacian(
                r_graph, probes
            )

            # For various reasons (malformed responses typical), we might have
            # a weird laplacian. We already checked and tried to clean up above
            # If this still happens at this point it is because a probe has a
            # problem (q_node got pruned out)
            if np.any(np.all(np.abs(laplacian) == 0, axis=0)):
                answer["analyses"][i_analysis]["score"] = 0
                continue

            # Once we have the graph laplacian we can find the effective
            # resistance between all of the probes
            # The exp(-1 * .) here converts us back to normalized space

            score = float(np.exp(-kirchhoff(laplacian, probe_inds)))

            # Fail safe to get rid of NaNs.
            score = score if np.isfinite(score) and score >= 0 else -1

            if jaccard_like:
                answer["analyses"][i_analysis]["score"] = score / (1 - score)
            else:
                answer["analyses"][i_analysis]["score"] = score

            # Package up details
            this_analysis_details = {
                "score": score,
                "r_graph": r_graph,
                "probes": probes,
                # edge_id: full edge
                "edges": {
                    e_info[2]: self.kgraph["edges"][e_info[2]]
                    for e_info in r_graph["edges"]
                },
                "laplacian": laplacian,
                "probe_inds": probe_inds,
            }
            this_analysis_details.update(laplacian_details)

            analysis_details.append(this_analysis_details)

        return answer, analysis_details

    def graph_laplacian(self, r_graph, probes):
        """Generate graph Laplacian."""

        nodes = list(r_graph["nodes"])  # Must convert to list
        edges = list(r_graph["edges"])  # Must have consistent order

        # The graph laplacian will be a square matrix
        # If all goes well it will be len(nodes) by len(nodes)
        num_nodes = len(nodes)

        # For each edge in the answer graph
        # Make a dictionary of edge subject, predicate, source , properties.

        # We will also keep track of a weighted version of the edge values
        # these are influenced by the profile
        edge_values_mat = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        for edge in edges:
            # Recall that edge is a tuple
            # This will contain
            # (r_node_subject_id, r_node_subject_id, k_edge_id)
            r_subject = edge[0]
            r_object = edge[1]
            k_edge_id = edge[2]

            # The edge weight can be found via lookup
            edge_vals = self.get_edge_values(k_edge_id)

            edge_values_mat[r_subject][r_object][k_edge_id] = edge_vals
            # This enforces symmetry in edges/wires
            edge_values_mat[r_object][r_subject][k_edge_id] = edge_vals

        # Make a set of all subject object q_node_ids that have q_edges
        qedge_qnode_ids = set(
            [
                frozenset((e["subject"], e["object"]))
                for e in self.qgraph["edges"].values()
            ]
        )

        # Now go through these edges
        # Turn each value into an edge weight
        # Then calculate the graph laplacian
        laplacian = np.zeros((num_nodes, num_nodes))
        weight_mat = np.zeros((num_nodes, num_nodes))  # For debugging

        # for each item in the weight matrix (edge weight between i and j)
        for i, sub_r_node_id in enumerate(nodes):
            for j, obj_r_node_id in enumerate(nodes):
                # If these r_nodes correspond to q_nodes and there is a q_edge
                # between then we set a default weight for this weight
                # Otherwise we set the default weight to 0
                # This ensures that bound q_edges (which match to provided
                # k_edges) get a weight
                edge_qnode_ids = frozenset((sub_r_node_id, obj_r_node_id))

                weight = (
                    self.DEFAULT_WEIGHT if edge_qnode_ids in qedge_qnode_ids else 0.0
                )

                for edge_id, edge in edge_values_mat[sub_r_node_id][
                    obj_r_node_id
                ].items():
                    for source, properties in edge.items():
                        for property, values in properties.items():
                            w = values["weight"]

                            if w >= 1:  # > as an emergency
                                w = 0.9999999  # 1 causes numerical issues

                            # -1 / np.log(source_weighted) is our mapping from
                            # a [0,1] weight to an admittance.
                            # These admittances just add since they are all in
                            # parallel which is equivalent to a noisy or.
                            weight = weight + -1 / (np.log(w))

                weight_mat[i, j] += weight  # For debugging

        # Using weight_mat you can calculate the laplacian
        # We could do this in the loop above, but let's be explicit
        weight_row_sums = np.sum(weight_mat, axis=1)
        laplacian = -1 * weight_mat.copy()
        for i in range(num_nodes):
            laplacian[i, i] = weight_row_sums[i]

        # Clean up Laplacian (remove extra nodes etc.)
        # Sometimes, mostly because of a bug of some kind,
        # There will be rows of all zeros in the laplacian.
        # This will cause numerical issues.
        # We can remove these, as long as they aren't probes.
        removal_candidate = np.all(np.abs(laplacian) == 0, axis=0)
        # Don't permit removing probes
        for p in probes:
            p_i = (nodes.index(p[0]), nodes.index(p[1]))
            removal_candidate[p_i[0]] = False
            removal_candidate[p_i[1]] = False

        keep = np.logical_not(removal_candidate)
        kept_nodes = [n for i, n in enumerate(nodes) if keep[i]]

        # Convert probes to new laplacian inds
        probe_inds = [(kept_nodes.index(p[0]), kept_nodes.index(p[1])) for p in probes]
        details = {"edge_values": edge_values_mat, "weight_mat": weight_mat}

        # remove row and col for nodes not kept
        return laplacian[keep, :][:, keep], probe_inds, details

    def get_rgraph(self, result):
        """Get "ranker" subgraph."""
        answer = copy.deepcopy(result)

        # All analyses share some common r_graph nodes. We can make those now
        r_graph_shared = dict()
        r_graph_shared["nodes"] = set()
        r_graph_shared["nodes_map"] = defaultdict(list)
        for nb_id, nbs in answer["node_bindings"].items():
            r_graph_shared["nodes"].add(nb_id)
            for nb in nbs:
                r_graph_shared["nodes_map"][nb["id"]].append(nb_id)

        # Build the results KG for each analysis:
        # The nodes for the results KG are the same for all analyses
        # We can populate these node_ids by walking through all node bindings
        result_kg_shared = {"node_ids": set(), "edge_ids": set()}
        for nb_id, nbs in answer.get("node_bindings", {}).items():
            for nb in nbs:
                n_id = nb.get("id", None)
                if n_id:
                    result_kg_shared["node_ids"].add(n_id)

        # For each analysis we need to build a KG of all nodes and edges
        analysis_r_graphs = []
        for anal in answer["analyses"]:
            # Copy this list of globally bound nodes
            anal_kg = copy.deepcopy(result_kg_shared)

            # Walk and find all edges in this analysis
            for eb_id, ebs in anal["edge_bindings"].items():
                for eb in ebs:
                    e_id = eb.get("id", None)
                    if e_id:
                        anal_kg["edge_ids"].add(e_id)

            # Parse through all support graphs used in this analysis
            # If there are support graphs on/for this analysis (not edge)
            # Does this happen anymore?
            sg_ids = anal.get("support_graphs", [])
            for sg_id in sg_ids:
                sg = self.agraphs.get(sg_id, None)
                if sg:
                    # We found the referenced support graph
                    # Add in the corresponding nodes
                    sg_nodes = sg.get("nodes", [])
                    for sgn in sg_nodes:
                        anal_kg["node_ids"].add(sgn)

                    # Add in the corresponding edges
                    sg_edges = sg.get("edges", [])
                    for sge in sg_edges:
                        anal_kg["edge_ids"].add(sge)

            # Now we need to go through all of the edges we have found thus far
            # We need to find any additional support graphs
            # We will do that with this recursive function call
            # Since results_kg uses sets, things will not get duplicated
            current_edge_ids = copy.deepcopy(anal_kg["edge_ids"])
            for edge_id in current_edge_ids:
                anal_kg = get_edge_support_kg(
                    edge_id, self.kgraph, self.agraphs, anal_kg
                )

            # At this point each analysis now has a complete anal_kg
            # This is the complete picture of all nodes and edges used by this
            # analysis and includes everything from all support graphs
            # (recursively)

            # To make things simpler below it is helpful if will build a
            # complete list of additional nodes and edges that have been added
            # as part of support
            anal["support_nodes"] = copy.deepcopy(anal_kg["node_ids"])
            anal["support_edges"] = copy.deepcopy(anal_kg["edge_ids"])

            # remove support graph edges which are already in edge bindings
            # so they are not double-counted
            for eb_id, ebs in anal["edge_bindings"].items():
                for eb in ebs:
                    e_id = eb.get("id", None)
                    if e_id and e_id in anal["support_edges"]:
                        anal["support_edges"].remove(e_id)

            # same for nodes
            for nb_id, nbs in answer["node_bindings"].items():
                for nb in nbs:
                    n_id = nb.get("id", None)
                    if n_id and n_id in anal["support_nodes"]:
                        anal["support_nodes"].remove(n_id)

            # It is also convenient to have a list of all edges that were bound
            anal["bound_edges"] = anal_kg["edge_ids"] - anal["support_edges"]

            # We need to build the r_graph which is a little different than the
            # analysis graph. In the list of nodes in the analysis we need to
            # consider the specific node bindings. For example, it is possible
            # to use a k_node in multiple bindings to different q_nodes
            # the r_graph makes "r_nodes" for each q_node

            # Any additional support nodes are added accordingly to the r_graph
            # as individual r_nodes

            # Then we need to include all edges but have them point at the
            # correct r_nodes. We need to reroute them accordingly by looking
            # up the origin k_node ids in the nodes_map

            # First we copy over the shared nodes
            anal_r_graph = copy.deepcopy(r_graph_shared)

            # Add in support nodes to the r_graph
            # We will make an r_graph node
            # And a mapping to that r_graph node
            for sn in anal["support_nodes"]:
                r_graph_node_id = f"support_node_{sn}"
                anal_r_graph["nodes"].add(r_graph_node_id)
                anal_r_graph["nodes_map"][sn].append(r_graph_node_id)

            # For each edge we need to find the corresponding r_nodes
            # for the subject and object
            # We will use a tuple for these as a sparse matrix type of thing
            # Bound edges will go between non support nodes
            anal_r_graph["edges"] = set()
            for e_id in anal["bound_edges"] | anal["support_edges"]:
                e = self.kgraph["edges"].get(e_id)
                if not e:
                    self.logger.warning(f"Edge {e_id} not found in knowledge graph")
                    continue
                subject_id = e["subject"]
                subject_r_ids = anal_r_graph["nodes_map"][subject_id]

                object_id = e["object"]
                object_r_ids = anal_r_graph["nodes_map"][object_id]

                for r_sub in subject_r_ids:
                    for r_obj in object_r_ids:
                        anal_r_graph["edges"].add((r_sub, r_obj, e_id))

            # Build a list of these for each analysis
            analysis_r_graphs.append(anal_r_graph)

        return analysis_r_graphs

    def get_omnicorp_node_pubs(self, node_id):
        """
        Find and return the omnicorp publication counts attached to each node.
        This method will also cache the result for this message
        """

        # Check cache
        if node_id in self.node_pubs:
            return self.node_pubs[node_id]

        # Not in the cache, make sure it's a valid node
        node = self.kgraph["nodes"].get(node_id)
        if not node:
            # Should we error here or just cache 0?
            # I think error
            raise KeyError(f"Invalid node ID {node_id}")

        # Extract the node information we are interested in

        # Parse the node attributes to find the publication count
        omnicorp_article_count = 0
        attributes = node.get("attributes", [])

        # Look through attributes and check for the omnicorp edges
        for p in attributes:
            # is this what we are looking for
            # Over time this has changed it's name
            # num_publications is currently in use (2024-03)
            # but for historical sake we keep the old name
            if p.get("original_attribute_name", "") in [
                "omnicorp_article_count",
                "num_publications",
            ]:
                omnicorp_article_count = p["value"]
                break  # There can be only one

        # Safely parse
        try:
            omnicorp_article_count = int(omnicorp_article_count)
        except ValueError:
            omnicorp_article_count = 0

        # Cache it
        self.node_pubs[node_id] = omnicorp_article_count

        return omnicorp_article_count

    def get_edge_values(self, edge_id):
        """
        This transforms all edge attributes into values that can be used for
        ranking. This does not consider all attributes, just the ones that we
        can currently handle. If we want to handle more things we need to add
        more cases here. This will also cache the result for this message
        """

        # literature co-occurrence assumes a global number of pubs
        # This is a constant but could potentially be included in a profile
        TOTAL_PUBS = 27840000

        # Check cache
        if edge_id in self.edge_values:
            return self.edge_values[edge_id]

        # Not in the cache, make sure it's a valid node
        edge = self.kgraph["edges"].get(edge_id)
        if not edge:
            # Should we error here or just cache empty?
            # I think error
            raise KeyError(f"Invalid edge ID {edge_id}")

        # Extract the edge information we are interested in

        # Get edge source information this is looking for primary_knowledge_source
        edge_source = "unspecified"
        for source in edge.get("sources", []):
            if "primary_knowledge_source" == source.get("resource_role", None):
                edge_source = source.get("resource_id", "unspecified")
                break  # There can be only one

        # We find literature co-occurance edges via predicate
        edge_pred = edge.get("predicate", "")

        # Currently we use three types of information
        # Init storage for the values we may find
        usable_edge_attr = {
            "publications": [],
            "num_publications": 0,
            "literature_coocurrence": None,
            "p_value": None,
            "affinity": None,
        }

        # Look through attributes and
        for attribute in edge.get("attributes", []):
            orig_attr_name = attribute.get("original_attribute_name", "")
            if not orig_attr_name:
                orig_attr_name = ""

            attr_type_id = attribute.get("attribute_type_id", "")
            if not attr_type_id:
                attr_type_id = ""

            # We will look at both the original_attribute_name and the
            # attribute_type_id. The attribute_type_id is the real method
            # But we should maintain some degree of backwards compatibility

            # Publications
            if (
                orig_attr_name == "publications"
                or attr_type_id == "biolink:supporting_document"
                or attr_type_id == "biolink:publications"
            ):

                # Parse pubs to handle all the cases we have observed
                pubs = attribute.get("value", [])

                if isinstance(pubs, str):
                    pubs = [pubs]

                # Attempt to parse pubs incase it has string lists
                if len(pubs) == 1:
                    if "|" in pubs[0]:
                        # "publications": ['PMID:1234|2345|83984']
                        pubs = pubs[0].split("|")
                    elif "," in pubs[0]:
                        # "publications": ['PMID:1234,2345,83984']
                        pubs = pubs[0].split(",")

                usable_edge_attr["publications"] = pubs
                usable_edge_attr["num_publications"] = len(pubs)

            if attr_type_id == "biolink:evidence_count":
                usable_edge_attr["num_publications"] = attribute.get("value", 0)

            # P-Values
            # first 4 probably never happen
            if (
                "p_value" in orig_attr_name
                or "p-value" in orig_attr_name
                or "p_value" in attr_type_id
                or "p-value" in attr_type_id
                or "pValue" in orig_attr_name
                or "fisher_exact_p" in orig_attr_name
                or "gwas_pvalue" in orig_attr_name
            ):

                p_value = attribute.get("value", None)

                # Some times the reported p_value is a list like [p_value]
                if isinstance(p_value, list):
                    p_value = p_value[0] if len(p_value) > 0 else None

                if isinstance(p_value, str):
                    # Parse strings safely
                    try:
                        p_value = float(p_value)
                    except ValueError:
                        p_value = None

                usable_edge_attr["p_value"] = p_value

            # Literature Co-occurrence actually uses the num_publications found
            # above so we make sure we do it last.
            if (
                edge_pred == "biolink:occurs_together_in_literature_with"
                and attr_type_id == "biolink:has_count"
            ):

                # We assume this is from a literature co-occurrence source
                # (like omnicorp)
                num_pubs = attribute.get("value", 0)
                # Parse strings safely
                try:
                    num_pubs = int(num_pubs)
                except ValueError:
                    num_pubs = 0

                subject_pubs = self.get_omnicorp_node_pubs(edge["subject"])
                object_pubs = self.get_omnicorp_node_pubs(edge["object"])

                # Literature co-occurrence score
                cov = (num_pubs / TOTAL_PUBS) - (subject_pubs / TOTAL_PUBS) * (
                    object_pubs / TOTAL_PUBS
                )
                cov = max((cov, 0.0))
                usable_edge_attr["literature_coocurrence"] = cov * TOTAL_PUBS
            # else:
            #     # Every other edge has an assumed publication of 1
            #     usable_edge_attr['num_publications'] += 1

            # affinities
            if orig_attr_name == "affinity":
                usable_edge_attr["affinity"] = attribute.get("value", 0)

            # confidence score
            if orig_attr_name == "biolink:tmkp_confidence_score":
                usable_edge_attr["confidence_score"] = attribute.get("value", 0)

        # At this point we have all of the information extracted from the edge
        # We have have looked through all attributes and updated
        # usable_edge_attr. Now we can construct the edge values using these
        # attributes and the base weight

        this_edge_vals = defaultdict(dict)
        base_weight = get_base_weight(edge_source, self.base_weights)
        this_edge_vals[edge_source]["base_weight"] = {"weight": base_weight}
        if usable_edge_attr["p_value"] is not None:
            property_w = get_source_sigmoid(
                usable_edge_attr["p_value"],
                edge_source,
                "p_value",
                self.source_transformation,
                self.unknown_source_transformation,
            )
            source_w = get_source_weight(
                edge_source, "p_value", self.source_weights, self.unknown_source_weight
            )

            this_edge_vals[edge_source]["p_value"] = {
                "value": usable_edge_attr["p_value"],
                "property_weight": property_w,
                "source_weight": source_w,
                "weight": property_w * source_w,
            }

        if usable_edge_attr["num_publications"]:
            property_w = get_source_sigmoid(
                usable_edge_attr["num_publications"],
                edge_source,
                "publications",
                self.source_transformation,
                self.unknown_source_transformation,
            )

            source_w = get_source_weight(
                edge_source,
                "publications",
                self.source_weights,
                self.unknown_source_weight,
            )

            this_edge_vals[edge_source]["publications"] = {
                "value": usable_edge_attr["num_publications"],
                "property_weight": property_w,
                "source_weight": source_w,
                "weight": property_w * source_w,
            }

        if usable_edge_attr["literature_coocurrence"] is not None:
            property_w = get_source_sigmoid(
                usable_edge_attr["literature_coocurrence"],
                edge_source,
                "literature_co-occurrence",
                self.source_transformation,
                self.unknown_source_transformation,
            )

            source_w = get_source_weight(
                edge_source,
                "literature_co-occurrence",
                self.source_weights,
                self.unknown_source_weight,
            )
            this_edge_vals[edge_source]["literature_coocurrence"] = {
                "value": usable_edge_attr["literature_coocurrence"],
                "property_weight": property_w,
                "source_weight": source_w,
                "weight": property_w * source_w,
            }

        if usable_edge_attr["affinity"] is not None:

            property_w = get_source_sigmoid(
                usable_edge_attr["affinity"],
                edge_source,
                "affinity",
                self.source_transformation,
                self.unknown_source_transformation,
            )

            source_w = get_source_weight(
                edge_source, "affinity", self.source_weights, self.unknown_source_weight
            )

            this_edge_vals[edge_source]["affinity"] = {
                "value": usable_edge_attr["affinity"],
                "property_weight": property_w,
                "source_weight": source_w,
                "weight": property_w * source_w,
            }

        # Cache it
        self.edge_values[edge_id] = this_edge_vals
        return this_edge_vals


def kirchhoff(L, probes):
    """Compute Kirchhoff index, including only specific nodes."""
    try:
        num_nodes = L.shape[0]
        cols = []
        for x, y in probes:
            d = np.zeros(num_nodes)
            d[x] = -1
            d[y] = 1
            cols.append(d)
        x = np.stack(cols, axis=1)
    except:
        # print(cols)
        return -np.inf

    return np.trace(x.T @ np.linalg.lstsq(L, x, rcond=None)[0])


def get_edge_support_kg(edge_id, kg, aux_graphs, edge_kg=None):
    if edge_kg is None:
        edge_kg = {"node_ids": set(), "edge_ids": set()}

    edge = kg["edges"].get(edge_id, None)
    if not edge:
        return edge_kg

    edge_attr = edge.get("attributes", None)
    if not edge_attr:
        return edge_kg

    edge_kg["edge_ids"].add(edge_id)

    # If we have edge attrs we might be adding new nodes
    sub = edge.get("subject", None)
    if sub:
        edge_kg["node_ids"].add(sub)

    obj = edge.get("object", None)
    if obj:
        edge_kg["node_ids"].add(obj)

    for attr in edge_attr:
        attr_type = attr.get("attribute_type_id", None)
        if attr_type == "biolink:support_graphs":
            # We actually have a biolink support graph
            more_support_graphs = attr.get("value", [])
            for sg_id in more_support_graphs:
                sg = aux_graphs.get(sg_id, None)
                if not sg:
                    continue

                sg_edges = sg.get("edges", [])
                sg_nodes = sg.get("nodes", [])
                for sgn in sg_nodes:
                    edge_kg["node_ids"].add(sgn)

                for add_edge_id in sg_edges:
                    try:
                        add_edge = kg["edges"][add_edge_id]
                    except KeyError:
                        # This shouldn't happen, but it's defending against
                        # some malformed TRAPI
                        continue

                    # Get this edge and add it to the edge_kg
                    edge_kg["edge_ids"].add(add_edge_id)

                    add_edge_sub = add_edge.get("subject", None)
                    if add_edge_sub:
                        edge_kg["node_ids"].add(add_edge_sub)

                    add_edge_object = add_edge.get("object", None)
                    if add_edge_object:
                        edge_kg["node_ids"].add(add_edge_object)

                    edge_kg = get_edge_support_kg(add_edge_id, kg, aux_graphs, edge_kg)

    return edge_kg


async def aragorn_score(task, logger: logging.Logger):
    """Use Aragorn Ranking to give all results a score."""
    start = time.time()
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])
    in_message = await get_message(response_id, logger)

    # save the logs for the response (if any)
    if "logs" not in in_message or in_message["logs"] is None:
        in_message["logs"] = []
    else:
        # these timestamps are causing json serialization issues
        # so here we convert them to strings.
        for log in in_message["logs"]:
            log["timestamp"] = str(log["timestamp"])

    message = in_message["message"]
    if ("results" not in message) or (message["results"] is None):
        # No results to weight. abort
        return

    # get a reference to the results
    answers = message["results"]

    try:
        # resistance distance ranking
        pr = Ranker(message, logger)

        # rank the answers. there should be a score for each bound result after this
        # TODO: is this right?
        jaccard_like = False
        answers = pr.rank(answers, jaccard_like=jaccard_like)

        # save the results
        message["results"] = answers
    except Exception as e:
        # put the error in the response
        logger.exception(f"Aragorn-ranker/score exception {e}")
        # save any log entries
        # in_message['logs'].append(create_log_entry(f'Exception: {str(e)}', 'ERROR'))

    # return the result to the caller
    logger.info("Score complete. Returning")

    await save_message(response_id, in_message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)

    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def poll_for_tasks():
    async for task, logger in get_tasks(STREAM, GROUP, CONSUMER):
        asyncio.create_task(aragorn_score(task, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
