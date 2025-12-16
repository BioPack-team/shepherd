"""
ARAX Ranker Module

The ranker combines multiple scoring methods:
1. Edge confidence scoring based on data source and attributes
2. Max flow algorithm for result scoring
3. Longest path algorithm for result scoring  
4. Frobenius norm for result scoring

These are combined using quantile ranking to produce final result scores.
"""

import logging
import math
import re
from typing import Any, Dict, List

import networkx as nx
import numpy as np
import numpy.typing as npt
import scipy.stats


# Default confidence for manual agent edges
EDGE_CONFIDENCE_MANUAL_AGENT = 0.90


class ARAXRanker:
    """
    ARAX-style ranker for TRAPI messages.
    
    Adapted from RTX/code/ARAX/ARAXQuery/ARAX_ranker.py to work with
    dict-based TRAPI messages in Shepherd.
    """

    def __init__(self, logger: logging.Logger):
        """Initialize the ranker."""
        self.logger = logger
        
        # Trust weights for different edge attributes
        self.known_attributes_to_trust = {
            'probability': 0.8,
            'normalized_google_distance': 0.8,
            'jaccard_index': 0.5,
            'probability_treats': 0.8,
            'paired_concept_frequency': 0.5,
            'observed_expected_ratio': 0.8,
            'chi_square': 0.8,
            'chi_square_pvalue': 0.8,
            'MAGMA-pvalue': 1.0,
            'Genetics-quantile': 1.0,
            'pValue': 1.0,
            'fisher_exact_test_p-value': 0.8,
            'Richards-effector-genes': 0.5,
            'feature_coefficient': 1.0,
            'CMAP similarity score': 1.0,
        }
        
        # Base weights for different data sources
        self.data_source_base_weights = {
            'infores:semmeddb': 0.5,
            'infores:text-mining-provider-targeted': 0.85,
            'infores:drugcentral': 0.93,
            'infores:drugbank': 0.99
        }
        
        self.score_stats: Dict[str, Dict[str, float]] = {}
        self.kg_edge_id_to_edge: Dict[str, Dict] = {}

    def rank_results(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Rank all results in a TRAPI message.
        
        Args:
            message: TRAPI message dict with query_graph, knowledge_graph, and results
            
        Returns:
            Modified message with scored and sorted results
        """
        self.logger.info("Starting ARAX ranking")
        
        # Validate message structure
        if not message.get("message"):
            self.logger.warning("No message found in input")
            return message
            
        msg = message["message"]
        
        if not msg.get("results"):
            self.logger.info("No results to rank")
            return message
            
        if not msg.get("knowledge_graph") or not msg.get("query_graph"):
            self.logger.warning("Missing knowledge_graph or query_graph")
            return message

        # Build edge lookup and collect stats
        self._build_edge_lookup_and_stats(msg)
        
        # Score each edge
        self._score_all_edges(msg)
        
        # Score results using graph algorithms
        self._score_results(msg)
        
        # Sort results by score
        self._sort_results(msg)
        
        self.logger.info("ARAX ranking complete")
        return message

    def _build_edge_lookup_and_stats(self, msg: Dict) -> None:
        """Build edge lookup dict and collect attribute statistics."""
        kg = msg.get("knowledge_graph", {})
        edges = kg.get("edges", {})
        
        self.kg_edge_id_to_edge = {}
        self.score_stats = {}
        
        for edge_key, edge in edges.items():
            self.kg_edge_id_to_edge[edge_key] = edge
            
            if edge.get("attributes"):
                for attr in edge["attributes"]:
                    attr_name = attr.get("original_attribute_name") or attr.get("attribute_type_id")
                    if attr_name in self.known_attributes_to_trust:
                        value = attr.get("value")
                        if value == "no value!":
                            value = 0
                        try:
                            value = float(value)
                        except (ValueError, TypeError):
                            continue
                            
                        if not np.isinf(value) and not np.isnan(value):
                            if attr_name not in self.score_stats:
                                self.score_stats[attr_name] = {'minimum': value, 'maximum': value}
                            else:
                                if value > self.score_stats[attr_name]['maximum']:
                                    self.score_stats[attr_name]['maximum'] = value
                                if value < self.score_stats[attr_name]['minimum']:
                                    self.score_stats[attr_name]['minimum'] = value

        self.logger.debug(f"Built edge lookup with {len(self.kg_edge_id_to_edge)} edges")
        self.logger.debug(f"Score stats: {self.score_stats}")

    def _score_all_edges(self, msg: Dict) -> None:
        """Score all edges in the knowledge graph."""
        kg = msg.get("knowledge_graph", {})
        edges = kg.get("edges", {})
        
        for edge_key, edge in edges.items():
            # Check for manual agent
            is_manual_agent = False
            if edge.get("attributes"):
                for attr in edge["attributes"]:
                    if (attr.get("attribute_type_id") == "biolink:agent_type" and 
                        attr.get("value") == "manual_agent"):
                        is_manual_agent = True
                        break
            
            if is_manual_agent:
                edge["confidence"] = EDGE_CONFIDENCE_MANUAL_AGENT
            else:
                edge["confidence"] = self._calculate_edge_confidence(edge_key, edge)

    def _calculate_edge_confidence(self, edge_key: str, edge: Dict) -> float:
        """
        Calculate confidence score for a single edge.
        
        Uses data source base weight and combines attribute scores.
        """
        edge_default_base = 0.5
        attribute_scores = []
        
        # Get data source from edge key
        parts = edge_key.split('--')
        data_source = parts[-1] if len(parts) > 1 else ""
        
        # Determine base score
        if data_source in self.data_source_base_weights:
            base = self.data_source_base_weights[data_source]
        elif 'infores' in data_source: # default score for other data sources
            base = edge_default_base
        else:
            base = 0  # Virtual/inferred edges
        
        if edge.get("attributes"):
            for attr in edge["attributes"]:
                attr_name = attr.get("original_attribute_name") or attr.get("attribute_type_id")
                value = attr.get("value")
                
                # Normalize the attribute score
                normalized = self._normalize_attribute(attr_name, value)
                
                # Handle publications specially for semmeddb
                if attr.get("attribute_type_id") == "biolink:publications" and data_source == "infores:semmeddb":
                    pub_score = self._normalize_publications(value)
                    if pub_score > 0:
                        attribute_scores.append(pub_score)
                elif attr_name in self.known_attributes_to_trust and normalized > 0:
                    weighted_score = normalized * self.known_attributes_to_trust[attr_name]
                    attribute_scores.append(weighted_score)
        
        if not attribute_scores:
            return base
            
        return self._combine_scores(base, attribute_scores)

    def _normalize_attribute(self, attr_name: str, value: Any) -> float:
        """Normalize an edge attribute value to [0, 1]."""
        if attr_name not in self.known_attributes_to_trust:
            return -1
            
        if value == "no value!":
            value = 0
            
        try:
            value = float(value)
        except (TypeError, ValueError):
            return 0.0
            
        if np.isnan(value):
            return 0.0
            
        # Normalize attribute name for method dispatch
        method_name = re.sub(r'[- :]', '_', attr_name)
        normalize_method = getattr(self, f'_normalize_{method_name}', None)
        
        if normalize_method:
            return normalize_method(value)
        return 0.0

    def _normalize_publications(self, value: Any) -> float:
        """Normalize publication count using logistic function."""
        if isinstance(value, str):
            publications = [value]
        elif isinstance(value, list):
            publications = value
        else:
            return -1
            
        n_pubs = len(set(publications))
        if n_pubs == 0:
            return 0.0001
            
        log_pubs = np.log(n_pubs)
        max_value = 1.0
        curve_steepness = 3.16993
        logistic_midpoint = 1.60943  # log(5) = 1.60943 meaning having 5 publications is a mid point
        
        return max_value / (1 + np.exp(-curve_steepness * (log_pubs - logistic_midpoint)))

    def _normalize_probability_treats(self, value: float) -> float:
        """Normalize probability_treats attribute."""
        return 1.0 / (1 + np.exp(-15 * (value - 0.60)))

    def _normalize_normalized_google_distance(self, value: float) -> float:
        """Normalize NGD (lower is better, so invert)."""
        return 1.0 / (1 + np.exp(9 * (value - 0.60)))

    def _normalize_probability(self, value: float) -> float:
        """Normalize probability attribute."""
        return 1.0 / (1 + np.exp(-20 * (value - 0.8)))

    def _normalize_jaccard_index(self, value: float) -> float:
        """Normalize Jaccard index relative to max."""
        max_val = self.score_stats.get('jaccard_index', {}).get('maximum', 1.0)
        return value / max_val if max_val > 0 else value

    def _normalize_paired_concept_frequency(self, value: float) -> float:
        """Normalize paired concept frequency."""
        return 1.0 / (1 + np.exp(-2000 * (value - 0.002)))

    def _normalize_observed_expected_ratio(self, value: float) -> float:
        """Normalize observed/expected ratio (log scale)."""
        return 1.0 / (1 + np.exp(-2 * (value - 2)))

    def _normalize_chi_square(self, value: float) -> float:
        """Normalize chi-square p-value using -log transform."""
        if value <= 0:
            return 1.0
        log_val = -np.log(value)
        return 1.0 / (1 + np.exp(-0.03 * (log_val - 200)))

    def _normalize_chi_square_pvalue(self, value: float) -> float:
        """Alias for chi_square normalization."""
        return self._normalize_chi_square(value)

    def _normalize_MAGMA_pvalue(self, value: float) -> float:
        """Normalize MAGMA p-value."""
        if value <= 0:
            return 1.0
        log_val = -np.log(value)
        return 1.0 / (1 + np.exp(-0.849 * (log_val - 4.97)))

    def _normalize_pValue(self, value: float) -> float:
        """Normalize generic p-value."""
        return self._normalize_MAGMA_pvalue(value)

    def _normalize_Genetics_quantile(self, value: float) -> float:
        """Genetics quantile is already normalized."""
        return value

    def _normalize_fisher_exact_test_p_value(self, value: float) -> float:
        """Normalize Fisher exact test p-value."""
        if value <= np.finfo(float).eps:
            return 1.0
        log_val = -np.log(value)
        return 1.0 / (1 + np.exp(-3 * (log_val - 2.7)))

    def _normalize_CMAP_similarity_score(self, value: float) -> float:
        """Normalize CMAP similarity score."""
        return abs(value / 100)

    def _normalize_Richards_effector_genes(self, value: float) -> float:
        """Richards effector genes score passthrough."""
        return value

    def _normalize_feature_coefficient(self, value: float) -> float:
        """Normalize feature coefficient using log transform."""
        log_abs = np.log(abs(value)) if value != 0 else 0
        return 1.0 / (1 + np.exp(-2.75 * (log_abs - 0.15)))

    @staticmethod
    def _combine_scores(base: float, scores: List[float]) -> float:
        """Generate base score with attribute scores using loop algorithm."""
        result = base
        for score in sorted(scores, reverse=True):
            result = result + (1 - result) * score
        return result

    def _score_results(self, msg: Dict) -> None:
        """Score all results using graph algorithms."""
        qg = msg.get("query_graph", {})
        results = msg.get("results", [])
        
        if not results:
            return
            
        # Build query graph networkx
        qg_nx = self._build_query_graph_nx(qg)
        
        # Score using multiple methods
        scorers = [
            self._score_by_max_flow,
            self._score_by_longest_path,
            self._score_by_frobenius_norm
        ]
        
        all_ranks = []
        for scorer in scorers:
            scores = []
            for result in results:
                result_graph = self._build_result_graph_nx(qg_nx, result)
                scores.append(scorer(result_graph))
            ranks = self._quantile_rank(scores)
            all_ranks.append(ranks)
        
        # Combine ranks
        combined_scores = np.mean(all_ranks, axis=0)
        
        # Assign scores to results
        for result, score in zip(results, combined_scores):
            if result.get("analyses"):
                result["analyses"][0]["score"] = max(float(score), 0.001)
            else:
                result["analyses"] = [{"score": max(float(score), 0.001)}]

    def _build_query_graph_nx(self, qg: Dict) -> nx.MultiDiGraph:
        """Build NetworkX graph from query graph."""
        g = nx.MultiDiGraph()
        
        nodes = qg.get("nodes", {})
        edges = qg.get("edges", {})
        
        # Add nodes (excluding creative_ nodes)
        for key in nodes:
            if 'creative_' not in key:
                g.add_node(key)
        
        # Add edges (excluding creative_ edges)
        for key, edge in edges.items():
            if 'creative_' not in key:
                g.add_edge(edge["subject"], edge["object"], key=key, weight=0.0)
        
        return g

    def _build_result_graph_nx(self, qg_nx: nx.MultiDiGraph, result: Dict) -> nx.MultiDiGraph:
        """Build weighted result graph from query graph and result bindings."""
        res_graph = qg_nx.copy()
        
        # Get edge tuples from query graph
        qg_edge_tuples_list = tuple(qg_nx.edges(keys=True, data=True))
        qg_edge_tuples = {edge_tuple[2]: edge_tuple for edge_tuple in qg_edge_tuples_list}
        
        # Process edge bindings from analyses
        analyses = result.get("analyses", [])
        if not analyses:
            return res_graph
            
        for analysis in analyses:
            edge_bindings = analysis.get("edge_bindings", {})
            
            for qedge_key, bindings in edge_bindings.items():
                if 'creative_' in qedge_key:
                    continue
                    
                if qedge_key not in qg_edge_tuples:
                    continue
                    
                # Process edge bindings with duplicate edge handling
                # 1. Group edges by their ID suffix (after splitting by ':')
                # 2. Average scores within each group
                # 3. Then combine the averaged scores
                same_edge_ids = {}
                for binding in bindings:
                    full_edge_id = binding.get("id", "")
                    # Extract edge ID suffix to identify duplicate edges
                    # e.g., "infores:aragorn--MONDO:123:NCBIGene:456" -> "NCBIGene:456"
                    edge_id_suffix = full_edge_id.split(':', 2)[-1] if full_edge_id else full_edge_id
                    
                    edge = self.kg_edge_id_to_edge.get(full_edge_id, {})
                    confidence = edge.get("confidence", 0.5)
                    
                    if edge_id_suffix not in same_edge_ids:
                        same_edge_ids[edge_id_suffix] = []
                    same_edge_ids[edge_id_suffix].append(confidence)
                
                # Average scores for each unique edge, then collect
                averaged_scores = []
                for edge_id_suffix, confidences in same_edge_ids.items():
                    avg_score = sum(confidences) / len(confidences)
                    averaged_scores.append(avg_score)
                
                if averaged_scores:
                    # Combine averaged scores using loop algorithm
                    combined = self._combine_scores(0, averaged_scores)
                    edge_tuple = qg_edge_tuples[qedge_key]
                    res_graph[edge_tuple[0]][edge_tuple[1]][edge_tuple[2]]['weight'] = combined
        
        return res_graph

    def _score_by_max_flow(self, graph: nx.MultiDiGraph) -> float:
        """Score graph using max flow algorithm."""
        if len(graph) <= 1:
            return 1.0
            
        try:
            # Find pairs with maximum path length
            apsp = dict(nx.all_pairs_shortest_path_length(graph))
            path_pairs = [(i, j, length) for i, d in apsp.items() for j, length in d.items()]
            
            if not path_pairs:
                return 0.0
                
            max_len = max(p[2] for p in path_pairs)
            max_pairs = [(p[0], p[1]) for p in path_pairs if p[2] == max_len]
            
            # Collapse multigraph for flow calculation
            collapsed = self._collapse_multigraph(graph)
            
            # Calculate max flow for each pair
            flow_values = []
            for source, target in max_pairs:
                try:
                    flow = nx.maximum_flow_value(collapsed, source, target, capacity="weight")
                    flow_values.append(flow)
                except nx.NetworkXError:
                    continue
            
            if flow_values:
                return self._combine_scores(0, flow_values)
            return 0.0
            
        except Exception as e:
            self.logger.debug(f"Max flow calculation failed: {e}")
            return 0.0

    def _score_by_longest_path(self, graph: nx.MultiDiGraph) -> float:
        """Score graph using longest path algorithm."""
        if len(graph) <= 1:
            return 1.0
            
        try:
            apsp = dict(nx.all_pairs_shortest_path_length(graph))
            path_pairs = [(i, j, length) for i, d in apsp.items() for j, length in d.items()]
            
            if not path_pairs:
                return 0.0
                
            max_len = max(p[2] for p in path_pairs)
            max_pairs = [(p[0], p[1]) for p in path_pairs if p[2] == max_len]
            
            # Build adjacency matrix
            nodes = list(graph.nodes())
            node_idx = {n: i for i, n in enumerate(nodes)}
            adj = nx.to_numpy_array(graph)
            
            # Matrix power calculation
            adj_power = np.linalg.matrix_power(adj, max_len) / math.factorial(max_len)
            
            scores = []
            for n1, n2 in max_pairs:
                i, j = node_idx[n1], node_idx[n2]
                scores.append(adj_power[i, j])
            
            return self._combine_scores(0, scores)
            
        except Exception as e:
            self.logger.debug(f"Longest path calculation failed: {e}")
            return 0.0

    def _score_by_frobenius_norm(self, graph: nx.MultiDiGraph) -> float:
        """Score graph using Frobenius norm of adjacency matrix."""
        try:
            adj = nx.to_numpy_array(graph)
            return float(np.linalg.norm(adj, ord='fro'))
        except Exception as e:
            self.logger.debug(f"Frobenius norm calculation failed: {e}")
            return 0.0

    @staticmethod
    def _collapse_multigraph(graph: nx.MultiDiGraph) -> nx.DiGraph:
        """Collapse multigraph edges by summing weights."""
        collapsed = nx.DiGraph()
        for u, v, data in graph.edges(data=True):
            weight = data.get('weight', 1.0)
            if collapsed.has_edge(u, v):
                collapsed[u][v]['weight'] += weight
            else:
                collapsed.add_edge(u, v, weight=weight)
        return collapsed

    @staticmethod
    def _quantile_rank(scores: List[float]) -> npt.NDArray[np.float64]:
        """Compute quantile ranks (ascending order, ties averaged)."""
        ranks = scipy.stats.rankdata(scores, method='max')
        return ranks / len(ranks)

    def _sort_results(self, msg: Dict) -> None:
        """Sort results by score in descending order and break ties."""
        results = msg.get("results", [])
        
        # Sort by score
        results.sort(
            key=lambda r: r.get("analyses", [{}])[0].get("score", 0),
            reverse=True
        )
        
        # Break ties to preserve order
        scores = [r.get("analyses", [{}])[0].get("score", 0) for r in results]
        adjusted = self._break_ties(scores)
        
        for result, score in zip(results, adjusted):
            if result.get("analyses"):
                result["analyses"][0]["score"] = score

    @staticmethod
    def _break_ties(scores: List[float]) -> List[float]:
        """Break ties in scores while preserving order."""
        adjusted = [round(s, 3) for s in scores]
        n = min(len(adjusted), 1000)
        
        for i in range(1, n):
            if adjusted[i] >= adjusted[i - 1]:
                adjusted[i] = max(adjusted[i - 1] - 0.001, 0)
        
        # Set remaining scores to 0 if beyond 1000
        for i in range(n, len(adjusted)):
            adjusted[i] = 0
            
        return [round(max(min(s, 1), 0), 3) for s in adjusted]


def arax_rank(message: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
    """
    Convenience function to rank a TRAPI message using ARAX algorithms.
    
    Args:
        message: TRAPI message dict
        logger: Logger instance
        
    Returns:
        Message with ranked results
    """
    ranker = ARAXRanker(logger)
    return ranker.rank_results(message)

