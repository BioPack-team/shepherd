"""
ARAX Ranker Module

A behavior-for-behavior port of RTX/code/ARAX/ARAXQuery/ARAX_ranker.py
(RTXteam/RTX @ 9485431), adapted only in that it reads and writes Shepherd's
dict-based TRAPI envelope instead of ARAX's openapi_server model objects.
See docs/ARAX_PORT_BASELINE.md (DEC-1, DEC-8, RNK-*).

Parity is deliberate, including ARAX's quirks; do not "fix" them here without
recording a deviation in the baseline doc:

- An edge's data source is the last ``--``-separated segment of its KG key.
- "no value!" attribute values are rewritten to 0 on the edge itself.
- Attribute min/max stats use a falsy test, so a 0 is treated as unset.
- An existing truthy ``confidence`` attribute overrides the computed one.
- Errors propagate exactly where ARAX would raise (a missing analysis, a
  dangling edge binding, publications normalizing a count of zero, ...).
- Edge confidences are internal only. ARAX stores them as a non-model
  attribute that is never serialized, so they are not written to the KG here.

The one piece of ARAX's ``aggregate_scores_dmk`` not carried over is its
``QueryGraphInfo().assess(message)`` call, whose result ARAX discards (the
merge is commented out), so it has no observable effect on the output.
"""

import logging
import math
import re
from typing import Any, Callable, Dict, List

import networkx as nx
import numpy as np
import numpy.typing as npt
import scipy.stats

EDGE_CONFIDENCE_MANUAL_AGENT = 0.90


def _get_query_graph_networkx_from_query_graph(query_graph: Dict) -> nx.MultiDiGraph:
    query_graph_nx = nx.MultiDiGraph()
    query_graph_nx.add_nodes_from(
        [key for key in query_graph["nodes"] if "creative_" not in key]
    )
    edge_list = [
        [edge["subject"], edge["object"], key, {"weight": 0.0}]
        for key, edge in query_graph["edges"].items()
        if "creative_" not in key
    ]
    query_graph_nx.add_edges_from(edge_list)
    return query_graph_nx


def _calculate_final_individual_edge_confidence(
    base_score: float, attribute_scores: List[float]
) -> float:
    # Eric's loop algorithm: W_r = W_r + (1 - W_r) * W_i over descending scores
    W_r = base_score
    sorted_attribute_scores = sorted(attribute_scores, reverse=True)
    for W_i in sorted_attribute_scores:
        W_r = W_r + (1 - W_r) * W_i
    return W_r


def _calculate_final_result_score(all_edge_scores: List[float]) -> float:
    return _calculate_final_individual_edge_confidence(0, all_edge_scores)


def _process_valid_edge_ids(
    valid_edge_id_info: Dict[str, Dict], edge_confidences: Dict[str, Any]
) -> Dict[str, Dict]:
    results: Dict[str, Dict] = {}
    for qedge_key, edge_info in valid_edge_id_info.items():
        results[qedge_key] = {}
        results[qedge_key]["edge_tuple"] = edge_info["edge_tuple"]
        results[qedge_key]["scores"] = []

        same_edge_ids: Dict[str, List[float]] = {}
        for edge_binding in edge_info["edge_binding_list"]:
            edge_id = edge_binding["id"].split(":", 2)[-1]
            if edge_id not in same_edge_ids:
                same_edge_ids[edge_id] = []
            same_edge_ids[edge_id].append(edge_confidences[edge_binding["id"]])

        # Take the average of the scores for each edge id
        for edge_id, scores in same_edge_ids.items():
            results[qedge_key]["scores"].append(sum(scores) / len(scores))
    return results


def _get_weighted_graph_networkx_from_result_graph(
    edge_confidences: Dict[str, Any], qg_nx: nx.MultiDiGraph, result: Dict
) -> nx.MultiDiGraph:
    res_graph = qg_nx.copy()
    qg_edge_tuples = tuple(qg_nx.edges(keys=True, data=True))
    qg_edge_key_to_edge_tuple = {
        edge_tuple[2]: edge_tuple for edge_tuple in qg_edge_tuples
    }

    valid_edge_id_info = {}
    for analysis in result["analyses"]:
        for qedge_key, edge_binding_list in analysis["edge_bindings"].items():
            if "creative_" not in qedge_key:  # ignore all xDTD/xCRG supported edges
                qedge_tuple = qg_edge_key_to_edge_tuple[qedge_key]
                valid_edge_id_info[qedge_key] = {
                    "edge_tuple": qedge_tuple,
                    "edge_binding_list": edge_binding_list,
                }

    processed_valid_edge_ids = _process_valid_edge_ids(
        valid_edge_id_info, edge_confidences
    )

    for qedge_key, edge_info in processed_valid_edge_ids.items():
        qedge_tuple = edge_info["edge_tuple"]
        scores = edge_info["scores"]
        res_graph[qedge_tuple[0]][qedge_tuple[1]][qedge_tuple[2]]["weight"] = (
            _calculate_final_result_score(scores)
        )
    return res_graph


def _get_weighted_graphs_networkx_from_result_graphs(
    edge_confidences: Dict[str, Any], qg_nx: nx.MultiDiGraph, results: List[Dict]
) -> List[nx.MultiDiGraph]:
    return [
        _get_weighted_graph_networkx_from_result_graph(edge_confidences, qg_nx, result)
        for result in results
    ]


# credit: StackOverflow:15590812
def _collapse_nx_multigraph_to_weighted_graph(graph_nx):
    if type(graph_nx) is nx.MultiGraph:
        ret_graph = nx.Graph()
    elif type(graph_nx) is nx.MultiDiGraph:
        ret_graph = nx.DiGraph()
    for u, v, data in graph_nx.edges(data=True):
        w = data["weight"] if "weight" in data else 1.0
        if ret_graph.has_edge(u, v):
            ret_graph[u][v]["weight"] += w
        else:
            ret_graph.add_edge(u, v, weight=w)
    return ret_graph


# Quantile ranks in *ascending* order (a higher x has a higher "rank"); ties get
# the same (max) rank.
def _quantile_rank_list(x: List[float]) -> npt.NDArray[np.float64]:
    y = scipy.stats.rankdata(x, method="max")
    return y / len(y)


def _score_networkx_graphs_by_max_flow(result_graphs_nx) -> List[float]:
    max_flow_values = []
    for result_graph_nx in result_graphs_nx:
        if len(result_graph_nx) > 1:
            apsp_dict = dict(
                nx.algorithms.shortest_paths.unweighted.all_pairs_shortest_path_length(
                    result_graph_nx
                )
            )
            path_len_with_pairs_list = [
                (node_i, node_j, path_len)
                for node_i, node_i_dict in apsp_dict.items()
                for node_j, path_len in node_i_dict.items()
            ]
            max_path_len = max(item[2] for item in path_len_with_pairs_list)
            pairs_with_max_path_len = [
                item[0:2]
                for item in path_len_with_pairs_list
                if item[2] == max_path_len
            ]
            max_flow_values_for_node_pairs = []
            result_graph_collapsed_nx = _collapse_nx_multigraph_to_weighted_graph(
                result_graph_nx
            )
            for source_node_id, target_node_id in pairs_with_max_path_len:
                max_flow_values_for_node_pairs.append(
                    nx.algorithms.flow.maximum_flow_value(
                        result_graph_collapsed_nx,
                        source_node_id,
                        target_node_id,
                        capacity="weight",
                    )
                )
            max_flow_value = 0.0
            if len(max_flow_values_for_node_pairs) > 0:
                max_flow_value = _calculate_final_individual_edge_confidence(
                    0, max_flow_values_for_node_pairs
                )
        else:
            max_flow_value = 1.0
        max_flow_values.append(max_flow_value)
    return max_flow_values


def _score_networkx_graphs_by_longest_path(result_graphs_nx) -> List[float]:
    result_scores = []
    for result_graph_nx in result_graphs_nx:
        apsp_dict = dict(
            nx.algorithms.shortest_paths.unweighted.all_pairs_shortest_path_length(
                result_graph_nx
            )
        )
        path_len_with_pairs_list = [
            (node_i, node_j, path_len)
            for node_i, node_i_dict in apsp_dict.items()
            for node_j, path_len in node_i_dict.items()
        ]
        max_path_len = max(item[2] for item in path_len_with_pairs_list)
        pairs_with_max_path_len = [
            item[0:2] for item in path_len_with_pairs_list if item[2] == max_path_len
        ]
        map_node_name_to_index = {
            node_id: node_index
            for node_index, node_id in enumerate(result_graph_nx.nodes)
        }
        adj_matrix = nx.to_numpy_array(result_graph_nx)
        adj_matrix_power = np.linalg.matrix_power(
            adj_matrix, max_path_len
        ) / math.factorial(max_path_len)
        score_list = [
            adj_matrix_power[
                map_node_name_to_index[node_i], map_node_name_to_index[node_j]
            ]
            for node_i, node_j in pairs_with_max_path_len
        ]
        result_scores.append(_calculate_final_individual_edge_confidence(0, score_list))
    return result_scores


def _score_networkx_graphs_by_frobenius_norm(result_graphs_nx) -> List[float]:
    result_scores = []
    for result_graph_nx in result_graphs_nx:
        adj_matrix = nx.to_numpy_array(result_graph_nx)
        result_scores.append(float(np.linalg.norm(adj_matrix, ord="fro")))
    return result_scores


def _score_result_graphs_by_networkx_graph_scorer(
    edge_confidences: Dict[str, Any],
    qg_nx: nx.MultiDiGraph,
    results: List[Dict],
    nx_graph_scorer: Callable,
) -> List[float]:
    result_graphs_nx = _get_weighted_graphs_networkx_from_result_graphs(
        edge_confidences, qg_nx, results
    )
    return nx_graph_scorer(result_graphs_nx)


def _break_ties_and_preserve_order(scores):
    adjusted_scores = scores.copy()
    n = len(scores)
    # if there are more than 1,000 scores, apply the fix to the first 1000 scores and ignore the rest
    if n > 1000:
        n = 1000
    # set all scores below the 1000th to 0
    for i in range(n, len(adjusted_scores)):
        adjusted_scores[i] = 0

    # round all scores to 3 decimal places initially to make adjustment easier
    adjusted_scores = [round(score, 3) for score in adjusted_scores]

    # Adjust scores in descending order to ensure no tie or inversion
    for i in range(1, n):
        if adjusted_scores[i] >= adjusted_scores[i - 1]:
            new_score = adjusted_scores[i - 1] - 0.001
            adjusted_scores[i] = max(new_score, 0)

    # Final check to ensure all scores are within bounds
    adjusted_scores = [round(max(min(score, 1), 0), 3) for score in adjusted_scores]
    return adjusted_scores


def _to_builtin(value):
    """numpy scalars -> the equivalent Python scalar, so the envelope serializes."""
    if isinstance(value, np.generic):
        return value.item()
    return value


class ARAXRanker:
    def __init__(self):
        # how much we trust each of the edge attributes
        self.known_attributes_to_trust = {
            "probability": 0.8,
            "normalized_google_distance": 0.8,
            "jaccard_index": 0.5,
            "probability_treats": 0.8,
            "paired_concept_frequency": 0.5,
            "observed_expected_ratio": 0.8,
            "chi_square": 0.8,
            "chi_square_pvalue": 0.8,
            "MAGMA-pvalue": 1.0,
            "Genetics-quantile": 1.0,
            "pValue": 1.0,
            "fisher_exact_test_p-value": 0.8,
            "Richards-effector-genes": 0.5,
            "feature_coefficient": 1.0,
            "CMAP similarity score": 1.0,
        }
        # how much we trust each data source
        self.data_source_base_weights = {
            "infores:semmeddb": 0.5,  # downweight semmeddb
            "infores:text-mining-provider-targeted": 0.85,
            "infores:drugcentral": 0.93,
            "infores:drugbank": 0.99,
        }
        self.score_stats = dict()  # max's and min's of the edge attribute values
        # ARAX keeps confidences on the Edge objects as a non-serialized
        # attribute; here they live beside the KG instead of in it.
        self.edge_confidences: Dict[str, Any] = dict()

    def edge_attribute_score_combiner(self, edge_key: str, edge: Dict) -> float:
        edge_default_base = 0.5
        edge_attribute_score_list = []

        data_source = edge_key.split("--")[-1]

        if data_source in self.data_source_base_weights:
            base = self.data_source_base_weights[data_source]
        elif "infores" in data_source:  # default score for other data sources
            base = edge_default_base
        else:  # virtual edges or inferred edges
            base = 0

        attributes = edge.get("attributes")
        if attributes is not None:
            for edge_attribute in attributes:
                original_attribute_name = edge_attribute.get("original_attribute_name")
                attribute_type_id = edge_attribute.get("attribute_type_id")
                value = edge_attribute.get("value")

                if original_attribute_name is not None:
                    normalized_score = self.edge_attribute_score_normalizer(
                        original_attribute_name, value
                    )
                else:
                    normalized_score = self.edge_attribute_score_normalizer(
                        attribute_type_id, value
                    )
                if (
                    attribute_type_id == "biolink:publications"
                    and data_source == "infores:semmeddb"
                ):
                    normalized_score = self.edge_attribute_publication_normalizer(
                        attribute_type_id, value
                    )

                if self.known_attributes_to_trust.get(original_attribute_name, None):
                    if normalized_score > 0:
                        edge_attribute_score_list.append(
                            normalized_score
                            * self.known_attributes_to_trust[original_attribute_name]
                        )
                elif self.known_attributes_to_trust.get(attribute_type_id, None):
                    if normalized_score > 0:
                        edge_attribute_score_list.append(
                            normalized_score
                            * self.known_attributes_to_trust[attribute_type_id]
                        )
                elif (
                    attribute_type_id == "biolink:publications"
                    and data_source == "infores:semmeddb"
                ):
                    if normalized_score > 0:
                        edge_attribute_score_list.append(normalized_score)
                else:
                    continue

            if len(edge_attribute_score_list) == 0:
                edge_confidence = base
            else:
                edge_confidence = _calculate_final_individual_edge_confidence(
                    base, edge_attribute_score_list
                )
        else:
            edge_confidence = base

        return edge_confidence

    def edge_attribute_score_normalizer(
        self, edge_attribute_name, edge_attribute_value
    ) -> float:
        if edge_attribute_name not in self.known_attributes_to_trust:
            return -1
        else:
            if edge_attribute_value == "no value!":
                edge_attribute_value = 0
            try:
                # check to see if it's convertible to a float (will catch None's as well)
                edge_attribute_value = float(edge_attribute_value)
            except TypeError:
                return 0.0
            except ValueError:
                return 0.0
            if np.isnan(edge_attribute_value):
                return 0.0
            else:
                edge_attribute_name = re.sub(r"[- \:]", "_", edge_attribute_name)
                return getattr(self, "_normalize_" + edge_attribute_name)(
                    value=edge_attribute_value
                )

    def edge_attribute_publication_normalizer(
        self, attribute_type_id, edge_attribute_value
    ) -> float:
        if attribute_type_id != "biolink:publications":
            return -1

        if isinstance(edge_attribute_value, str):
            publications = [edge_attribute_value]
        elif isinstance(edge_attribute_value, list):
            publications = edge_attribute_value
        else:
            return -1  # the data format storing publications has changed

        n_publications = len(set(publications))
        if n_publications == 0:
            # As in ARAX, normalized_value is never bound on this branch, so an
            # empty publications list raises UnboundLocalError (D-20).
            pub_value = 0.0001  # noqa: F841
        else:
            pub_value = np.log(n_publications)
            max_value = 1.0
            curve_steepness = 3.16993
            logistic_midpoint = 1.60943  # log(5): 5 publications is the mid point
            normalized_value = max_value / float(
                1 + np.exp(-curve_steepness * (pub_value - logistic_midpoint))
            )
        return normalized_value

    def _normalize_probability_treats(self, value):
        max_value = 1
        curve_steepness = 15
        logistic_midpoint = 0.60
        return max_value / float(
            1 + np.exp(-curve_steepness * (value - logistic_midpoint))
        )

    def _normalize_normalized_google_distance(self, value):
        max_value = 1
        curve_steepness = -9
        logistic_midpoint = 0.60
        return max_value / float(
            1 + np.exp(-curve_steepness * (value - logistic_midpoint))
        )

    def _normalize_probability(self, value):
        max_value = 1
        curve_steepness = 20
        logistic_midpoint = 0.8
        return max_value / float(
            1 + np.exp(-curve_steepness * (value - logistic_midpoint))
        )

    def _normalize_jaccard_index(self, value):
        return value / self.score_stats["jaccard_index"]["maximum"]

    def _normalize_paired_concept_frequency(self, value):
        max_value = 1
        curve_steepness = 2000
        logistic_midpoint = 0.002
        return max_value / float(
            1 + np.exp(-curve_steepness * (value - logistic_midpoint))
        )

    def _normalize_observed_expected_ratio(self, value):
        max_value = 1
        curve_steepness = 2
        logistic_midpoint = 2  # Exp[2] more likely than chance
        return max_value / float(
            1 + np.exp(-curve_steepness * (value - logistic_midpoint))
        )

    def _normalize_chi_square(self, value):
        # -Log[p_value] approach
        value = -np.log(value)
        max_value = 1
        curve_steepness = 0.03
        logistic_midpoint = 200
        return max_value / float(
            1 + np.exp(-curve_steepness * (value - logistic_midpoint))
        )

    def _normalize_chi_square_pvalue(self, value):
        return self._normalize_chi_square(value)

    def _normalize_MAGMA_pvalue(self, value):
        value = -np.log(value)
        max_value = 1.0
        curve_steepness = 0.849
        logistic_midpoint = 4.97
        return max_value / float(
            1 + np.exp(-curve_steepness * (value - logistic_midpoint))
        )

    def _normalize_pValue(self, value):
        value = -np.log(value)
        max_value = 1.0
        curve_steepness = 0.849
        logistic_midpoint = 4.97
        return max_value / float(
            1 + np.exp(-curve_steepness * (value - logistic_midpoint))
        )

    def _normalize_Genetics_quantile(self, value):
        return value

    def _normalize_fisher_exact_test_p_value(self, value):
        try:
            if value <= np.finfo(float).eps:
                normalized_value = 1.0
            else:
                value = -np.log(value)
                max_value = 1.0
                curve_steepness = 3
                logistic_midpoint = 2.7
                normalized_value = max_value / float(
                    1 + np.exp(-curve_steepness * (value - logistic_midpoint))
                )
        except RuntimeWarning:  # value is 0 (or nearly so): award the max value
            normalized_value = 1.0
        return normalized_value

    def _normalize_CMAP_similarity_score(self, value):
        return abs(value / 100)

    def _normalize_Richards_effector_genes(self, value):
        return value

    def _normalize_feature_coefficient(self, value):
        log_abs_value = np.log(abs(value))
        max_value = 1
        curve_steepness = 2.75
        logistic_midpoint = 0.15
        return max_value / float(
            1 + np.exp(-curve_steepness * (log_abs_value - logistic_midpoint))
        )

    def aggregate_scores_dmk(self, envelope: Dict, logger: logging.Logger) -> None:
        """Score, sort and tie-break ``envelope['message']['results']`` in place.

        Also sets each result's ``row_data`` and the envelope's
        ``table_column_names``, as ARAX does.
        """
        logger.debug("Starting to rank results")
        message = envelope["message"]

        # Iterate through all the edges in the knowledge graph to collect some
        # min,max stats for edge attributes that we may need later
        score_stats = self.score_stats
        no_non_inf_float_flag = True
        for edge_key, edge in message["knowledge_graph"]["edges"].items():
            attributes = edge.get("attributes")
            if attributes is not None:
                for edge_attribute in attributes:
                    for attribute_name in self.known_attributes_to_trust:
                        if (
                            edge_attribute.get("original_attribute_name")
                            == attribute_name
                            or edge_attribute.get("attribute_type_id") == attribute_name
                        ):
                            if edge_attribute.get("value") == "no value!":
                                edge_attribute["value"] = 0
                                value = 0
                            else:
                                try:
                                    value = float(edge_attribute.get("value"))
                                except ValueError:
                                    continue
                                except TypeError:
                                    continue
                            if attribute_name not in score_stats:
                                score_stats[attribute_name] = {
                                    "minimum": None,
                                    "maximum": None,
                                }
                            if (
                                not np.isinf(value)
                                and not np.isinf(-value)
                                and not np.isnan(value)
                            ):
                                no_non_inf_float_flag = False
                                if not score_stats[attribute_name]["minimum"]:
                                    score_stats[attribute_name]["minimum"] = value
                                if not score_stats[attribute_name]["maximum"]:
                                    score_stats[attribute_name]["maximum"] = value
                                if value > score_stats[attribute_name]["maximum"]:
                                    score_stats[attribute_name]["maximum"] = value
                                if value < score_stats[attribute_name]["minimum"]:
                                    score_stats[attribute_name]["minimum"] = value

        if no_non_inf_float_flag:
            logger.warning(
                "No non-infinite value was encountered in any edge attribute in the knowledge graph."
            )
        logger.info(f"Summary of available edge metrics: {score_stats}")

        # Attach a confidence to every edge
        for edge_key, edge in message["knowledge_graph"]["edges"].items():
            attributes = edge.get("attributes")
            if attributes is not None:
                edge_attributes = {
                    x.get("original_attribute_name"): x.get("value") for x in attributes
                }
                for edge_attribute in attributes:
                    if (
                        edge_attribute.get("attribute_type_id") == "biolink:agent_type"
                        and edge_attribute.get("value") == "manual_agent"
                    ):
                        edge_attributes["confidence"] = EDGE_CONFIDENCE_MANUAL_AGENT
                        self.edge_confidences[edge_key] = EDGE_CONFIDENCE_MANUAL_AGENT
                        break
            else:
                edge_attributes = {}

            if edge_attributes.get("confidence", None):
                self.edge_confidences[edge_key] = edge_attributes["confidence"]
            else:
                self.edge_confidences[edge_key] = self.edge_attribute_score_combiner(
                    edge_key, edge
                )

        results = message["results"]

        qg_nx = _get_query_graph_networkx_from_query_graph(message["query_graph"])
        edge_confidences = self.edge_confidences

        ranks_list = list(
            map(
                _quantile_rank_list,
                map(
                    lambda scorer_func: _score_result_graphs_by_networkx_graph_scorer(
                        edge_confidences, qg_nx, results, scorer_func
                    ),
                    [
                        _score_networkx_graphs_by_max_flow,
                        _score_networkx_graphs_by_longest_path,
                        _score_networkx_graphs_by_frobenius_norm,
                    ],
                ),
            )
        )

        result_scores = sum(ranks_list) / float(len(ranks_list))

        for result, score in zip(results, result_scores):
            result["analyses"][0]["score"] = score  # only ever one Analysis per Result

            # Make all scores at least 0.001
            if result["analyses"][0]["score"] < 0.001:
                result["analyses"][0]["score"] += 0.001

            # Round to reasonable precision. Keep only 3 digits after the decimal
            score = int(result["analyses"][0]["score"] * 1000 + 0.5) / 1000.0

            result["row_data"] = [
                score,
                result.get("essence"),
                result.get("essence_category"),
            ]

        envelope["table_column_names"] = ["score", "essence", "essence_category"]

        # Re-sort the final results
        results.sort(key=lambda result: result["analyses"][0]["score"], reverse=True)
        # break ties and preserve order, round to 3 digits and make sure none are < 0
        scores_with_ties = [result["analyses"][0]["score"] for result in results]
        scores_without_ties = _break_ties_and_preserve_order(scores_with_ties)
        for result, score in zip(results, scores_without_ties):
            score = _to_builtin(score)
            result["analyses"][0]["score"] = score
            result["row_data"][0] = score
        logger.debug("Results have been ranked and sorted")


def arax_rank(envelope: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
    """Rank a TRAPI envelope in place with the ARAX ranker and return it."""
    ARAXRanker().aggregate_scores_dmk(envelope, logger)
    return envelope
