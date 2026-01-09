"""
ARAX Ranker Module

This module is a direct replication of RTX/code/ARAX/ARAXQuery/ARAX_ranker.py,
adapted for Shepherd's dict-based TRAPI message structure.

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
from typing import Any, Dict, List, Optional

import networkx as nx
import numpy as np
import numpy.typing as npt
import scipy.stats


# Default confidence for manual agent edges (matches ARAX_ranker.py line 24)
EDGE_CONFIDENCE_MANUAL_AGENT = 0.90


class ARAXRanker:
    """
    ARAX-style ranker for TRAPI messages.
    
    Replication of RTX/code/ARAX/ARAXQuery/ARAX_ranker.py to work with
    dict-based TRAPI messages in Shepherd.
    
    Algorithm overview:
    1. Score each edge based on data source and attributes
    2. Build weighted NetworkX graphs for each result
    3. Score results using max flow, longest path, and Frobenius norm
    4. Combine scores using quantile ranking
    5. Break ties and sort results
    """

    def __init__(self, logger: logging.Logger):
        """
        Initialize the ranker.
        
        Sets up attribute trust weights and data source base weights
        exactly as in ARAX_ranker.py lines 271-293.
        """
        self.logger = logger
        
        # Trust weights for different edge attributes
        # Matches ARAX_ranker.py lines 271-286
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
        # Matches ARAX_ranker.py lines 288-293
        self.data_source_base_weights = {
            'infores:semmeddb': 0.5,
            'infores:text-mining-provider-targeted': 0.85,
            'infores:drugcentral': 0.93,
            'infores:drugbank': 0.99
        }
        
        # Statistics for relative scoring (like jaccard_index)
        self.score_stats: Dict[str, Dict[str, float]] = {}
        # Edge lookup for quick access
        self.kg_edge_id_to_edge: Dict[str, Dict] = {}

    def rank_results(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Rank all results in a TRAPI message.
        
        Main entry point that replicates aggregate_scores_dmk() from ARAX_ranker.py.
        
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

        # Step 1: Build edge lookup and collect attribute statistics
        # Matches ARAX_ranker.py lines 672-704
        self._build_edge_lookup_and_stats(msg)
        
        # Step 2: Score all edges based on attributes and data source
        # Matches ARAX_ranker.py lines 710-728
        self._score_all_edges(msg)
        
        # Step 3: Score results using graph algorithms
        # Matches ARAX_ranker.py lines 739-756
        self._score_results(msg)
        
        # Step 4: Sort results and break ties
        # Matches ARAX_ranker.py lines 774-783
        self._sort_results(msg)
        
        self.logger.info("ARAX ranking complete")
        return message

    def _build_edge_lookup_and_stats(self, msg: Dict) -> None:
        """
        Build edge lookup dict and collect attribute statistics.
        
        Replicates ARAX_ranker.py lines 672-704.
        Collects min/max values for attributes that need relative scoring.
        """
        kg = msg.get("knowledge_graph", {})
        edges = kg.get("edges", {})
        
        self.kg_edge_id_to_edge = {}
        self.score_stats = {}
        
        for edge_key, edge in edges.items():
            self.kg_edge_id_to_edge[edge_key] = edge
            
            if edge.get("attributes"):
                for attr in edge["attributes"]:
                    # Check both original_attribute_name and attribute_type_id
                    # Matches ARAX_ranker.py line 680
                    attr_name = attr.get("original_attribute_name")
                    attr_type_id = attr.get("attribute_type_id")
                    
                    # Check if either name is in known attributes
                    for check_name in [attr_name, attr_type_id]:
                        if check_name and check_name in self.known_attributes_to_trust:
                            value = attr.get("value")
                            
                            # Handle "no value!" case (ARAX_ranker.py lines 681-684)
                            if value == "no value!":
                                value = 0
                            
                            try:
                                value = float(value)
                            except (ValueError, TypeError):
                                continue
                            
                            # Ignore inf, -inf, and nan (ARAX_ranker.py line 694)
                            if np.isinf(value) or np.isinf(-value) or np.isnan(value):
                                continue
                            
                            # Initialize or update stats
                            if check_name not in self.score_stats:
                                self.score_stats[check_name] = {'minimum': value, 'maximum': value}
                            else:
                                if value > self.score_stats[check_name]['maximum']:
                                    self.score_stats[check_name]['maximum'] = value
                                if value < self.score_stats[check_name]['minimum']:
                                    self.score_stats[check_name]['minimum'] = value
                            break  # Only need to process once per attribute

        self.logger.debug(f"Built edge lookup with {len(self.kg_edge_id_to_edge)} edges")
        self.logger.info(f"Summary of available edge metrics: {self.score_stats}")

    def _score_all_edges(self, msg: Dict) -> None:
        """
        Score all edges in the knowledge graph.
        
        Replicates ARAX_ranker.py lines 710-728.
        """
        kg = msg.get("knowledge_graph", {})
        edges = kg.get("edges", {})
        
        for edge_key, edge in edges.items():
            # Check for manual agent (ARAX_ranker.py lines 713-720)
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
        
        Replicates edge_attribute_score_combiner() from ARAX_ranker.py lines 327-386.
        Uses data source base weight and combines attribute scores.
        """
        edge_default_base = 0.5
        edge_attribute_score_list = []
        
        # Get data source - try edge key first (ARAX format), then sources attribute (Aragorn/BTE)
        # ARAX_ranker.py line 339
        data_source = self._extract_data_source(edge_key, edge)
        
        # Determine base score (ARAX_ranker.py lines 342-347)
        if data_source in self.data_source_base_weights:
            base = self.data_source_base_weights[data_source]
        elif data_source and 'infores' in data_source:
            base = edge_default_base
        else:
            # Virtual/inferred edges get no base score
            base = 0
        
        if edge.get("attributes"):
            for attr in edge["attributes"]:
                # Get attribute identifiers
                original_attr_name = attr.get("original_attribute_name")
                attr_type_id = attr.get("attribute_type_id")
                value = attr.get("value")
                
                # Normalize using original_attribute_name if not None, else attribute_type_id
                # Matches ARAX_ranker.py lines 355-358
                if original_attr_name is not None:
                    normalized_score = self._normalize_attribute(original_attr_name, value)
                else:
                    normalized_score = self._normalize_attribute(attr_type_id, value)
                
                # Special handling for publications from semmeddb
                # Matches ARAX_ranker.py lines 359-361
                if attr_type_id == "biolink:publications" and data_source == "infores:semmeddb":
                    normalized_score = self._normalize_publications(value)
                
                # Collect scores from known attributes
                # Matches ARAX_ranker.py lines 364-377
                if original_attr_name and self.known_attributes_to_trust.get(original_attr_name):
                    if normalized_score > 0:
                        weighted = normalized_score * self.known_attributes_to_trust[original_attr_name]
                        edge_attribute_score_list.append(weighted)
                elif attr_type_id and self.known_attributes_to_trust.get(attr_type_id):
                    if normalized_score > 0:
                        weighted = normalized_score * self.known_attributes_to_trust[attr_type_id]
                        edge_attribute_score_list.append(weighted)
                elif attr_type_id == "biolink:publications" and data_source == "infores:semmeddb":
                    if normalized_score > 0:
                        edge_attribute_score_list.append(normalized_score)
        
        # If no attributes scored, return base score (ARAX_ranker.py lines 379-384)
        if len(edge_attribute_score_list) == 0:
            return base
        
        return self._combine_scores(base, edge_attribute_score_list)
    
    def _extract_data_source(self, edge_key: str, edge: Dict) -> str:
        """
        Extract data source from edge.
        
        Handles multiple formats:
        - ARAX format: edge_key contains "infores:provider--subject:object"
        - Aragorn/BTE format: sources attribute with primary_knowledge_source
        
        Args:
            edge_key: The edge identifier string
            edge: The edge object dict
            
        Returns:
            Data source string (e.g., "infores:semmeddb") or empty string
        """
        # Try ARAX-style edge key format (ARAX_ranker.py line 339)
        # e.g., "infores:rtx-kg2--MONDO:0005148--treats--CHEBI:12345"
        parts = edge_key.split('--')
        if len(parts) > 1:
            # Data source is typically the first part
            potential_source = parts[0]
            if 'infores:' in potential_source:
                return potential_source
            # Or might be the last part in some formats
            potential_source = parts[-1]
            if 'infores:' in potential_source:
                return potential_source
        
        # Fall back to sources attribute (Aragorn/BTE style)
        sources = edge.get("sources", [])
        for source in sources:
            if source.get("resource_role") == "primary_knowledge_source":
                resource_id = source.get("resource_id", "")
                if resource_id:
                    return resource_id
        
        # If no primary_knowledge_source, try any source with infores
        for source in sources:
            resource_id = source.get("resource_id", "")
            if resource_id and 'infores:' in resource_id:
                return resource_id
        
        return ""

    def _normalize_attribute(self, attr_name: Optional[str], value: Any) -> float:
        """
        Normalize an edge attribute value to [0, 1].
        
        Replicates edge_attribute_score_normalizer() from ARAX_ranker.py lines 388-413.
        
        Args:
            attr_name: The attribute name (original_attribute_name or attribute_type_id)
            value: The attribute value
            
        Returns:
            Normalized score in [0, 1], or -1 if attribute not recognized
        """
        if attr_name is None or attr_name not in self.known_attributes_to_trust:
            return -1
        
        # Handle "no value!" case (ARAX_ranker.py lines 396-397)
        if value == "no value!":
            value = 0
        
        try:
            value = float(value)
        except (TypeError, ValueError):
            return 0.0
        
        # Check for NaN (ARAX_ranker.py lines 405-407)
        if np.isnan(value):
            return 0.0
        
        # Normalize attribute name for method dispatch (ARAX_ranker.py line 411)
        # Replace hyphens, spaces, colons with underscores
        method_suffix = re.sub(r'[- :]', '_', attr_name)
        normalize_method = getattr(self, f'_normalize_{method_suffix}', None)
        
        if normalize_method:
            return normalize_method(value)
        
        return 0.0

    def _normalize_publications(self, value: Any) -> float:
        """
        Normalize publication count using logistic function.
        
        Replicates edge_attribute_publication_normalizer() from ARAX_ranker.py lines 415-438.
        
        Args:
            value: Publication list or string
            
        Returns:
            Normalized score in [0, 1]
        """
        if isinstance(value, str):
            publications = [value]
        elif isinstance(value, list):
            publications = value
        else:
            return -1
        
        n_pubs = len(set(publications))
        
        # Handle empty publications (ARAX_ranker.py line 431)
        if n_pubs == 0:
            return 0.0001
        
        # Logistic normalization (ARAX_ranker.py lines 433-437)
        pub_value = np.log(n_pubs)
        max_value = 1.0
        curve_steepness = 3.16993
        logistic_midpoint = 1.60943  # log(5) = 1.60943, 5 publications is midpoint
        
        return max_value / (1 + np.exp(-curve_steepness * (pub_value - logistic_midpoint)))

    def _normalize_probability_treats(self, value: float) -> float:
        """
        Normalize probability_treats attribute.
        
        Replicates __normalize_probability_treats() from ARAX_ranker.py lines 440-467.
        Values > 0.75 are considered "good" predictions.
        """
        max_value = 1
        curve_steepness = 15
        logistic_midpoint = 0.60
        return max_value / (1 + np.exp(-curve_steepness * (value - logistic_midpoint)))

    def _normalize_normalized_google_distance(self, value: float) -> float:
        """
        Normalize NGD (lower is better, so inverted curve).
        
        Replicates __normalize_normalized_google_distance() from ARAX_ranker.py lines 469-479.
        """
        max_value = 1
        curve_steepness = -9  # Negative because lower NGD is better
        logistic_midpoint = 0.60
        return max_value / (1 + np.exp(-curve_steepness * (value - logistic_midpoint)))

    def _normalize_probability(self, value: float) -> float:
        """
        Normalize probability attribute (drug->protein binding probabilities).
        
        Replicates __normalize_probability() from ARAX_ranker.py lines 481-493.
        """
        max_value = 1
        curve_steepness = 20
        logistic_midpoint = 0.8
        return max_value / (1 + np.exp(-curve_steepness * (value - logistic_midpoint)))

    def _normalize_jaccard_index(self, value: float) -> float:
        """
        Normalize Jaccard index relative to max observed value.
        
        Replicates __normalize_jaccard_index() from ARAX_ranker.py lines 495-502.
        """
        stats = self.score_stats.get('jaccard_index', {})
        max_val = stats.get('maximum', 1.0)
        if max_val is None or max_val == 0:
            return value
        return value / max_val

    def _normalize_paired_concept_frequency(self, value: float) -> float:
        """
        Normalize paired concept frequency from COHD.
        
        Replicates __normalize_paired_concept_frequency() from ARAX_ranker.py lines 504-524.
        """
        max_value = 1
        curve_steepness = 2000  # Steep because max values are small (e.g., 0.03)
        logistic_midpoint = 0.002
        return max_value / (1 + np.exp(-curve_steepness * (value - logistic_midpoint)))

    def _normalize_observed_expected_ratio(self, value: float) -> float:
        """
        Normalize observed/expected ratio (log scale).
        
        Replicates __normalize_observed_expected_ratio() from ARAX_ranker.py lines 526-537.
        Interpreted as Exp[value] times more likely than chance.
        """
        max_value = 1
        curve_steepness = 2
        logistic_midpoint = 2  # Exp[2] more likely than chance
        return max_value / (1 + np.exp(-curve_steepness * (value - logistic_midpoint)))

    def _normalize_chi_square(self, value: float) -> float:
        """
        Normalize chi-square p-value using -log transform.
        
        Replicates __normalize_chi_square() from ARAX_ranker.py lines 540-561.
        Uses -log(p_value) approach due to very small p-values from COHD.
        """
        # Handle edge case of zero or negative p-value
        if value <= 0:
            return 1.0
        
        # -Log[p_value] approach (ARAX_ranker.py lines 553-557)
        log_value = -np.log(value)
        max_value = 1
        curve_steepness = 0.03
        logistic_midpoint = 200
        return max_value / (1 + np.exp(-curve_steepness * (log_value - logistic_midpoint)))

    def _normalize_chi_square_pvalue(self, value: float) -> float:
        """
        Alias for chi_square normalization.
        
        Replicates __normalize_chi_square_pvalue() from ARAX_ranker.py lines 563-564.
        """
        return self._normalize_chi_square(value)

    def _normalize_MAGMA_pvalue(self, value: float) -> float:
        """
        Normalize MAGMA p-value from Genetics Provider.
        
        Replicates __normalize_MAGMA_pvalue() from ARAX_ranker.py lines 566-577.
        """
        # Handle edge case
        if value <= 0:
            return 1.0
        
        log_value = -np.log(value)
        max_value = 1.0
        curve_steepness = 0.849
        logistic_midpoint = 4.97
        return max_value / (1 + np.exp(-curve_steepness * (log_value - logistic_midpoint)))

    def _normalize_pValue(self, value: float) -> float:
        """
        Normalize generic p-value (same as MAGMA).
        
        Replicates __normalize_pValue() from ARAX_ranker.py lines 579-590.
        """
        return self._normalize_MAGMA_pvalue(value)

    def _normalize_Genetics_quantile(self, value: float) -> float:
        """
        Genetics quantile is already normalized.
        
        Replicates __normalize_Genetics_quantile() from ARAX_ranker.py lines 593-599.
        """
        return value

    def _normalize_fisher_exact_test_p_value(self, value: float) -> float:
        """
        Normalize Fisher exact test p-value.
        
        Replicates __normalize_fisher_exact_test_p_value() from ARAX_ranker.py lines 601-624.
        0.05 p-value corresponds to ~0.95 after normalization.
        """
        # Handle very small p-values (ARAX_ranker.py lines 613-614)
        if value <= np.finfo(float).eps:
            return 1.0
        
        try:
            log_value = -np.log(value)
            max_value = 1.0
            curve_steepness = 3
            logistic_midpoint = 2.7
            return max_value / (1 + np.exp(-curve_steepness * (log_value - logistic_midpoint)))
        except RuntimeWarning:
            # Case when value is 0 or nearly so
            return 1.0

    def _normalize_CMAP_similarity_score(self, value: float) -> float:
        """
        Normalize CMAP similarity score.
        
        Replicates __normalize_CMAP_similarity_score() from ARAX_ranker.py lines 626-628.
        """
        return abs(value / 100)

    def _normalize_Richards_effector_genes(self, value: float) -> float:
        """
        Richards effector genes score passthrough.
        
        Replicates __normalize_Richards_effector_genes() from ARAX_ranker.py lines 630-631.
        """
        return value

    def _normalize_feature_coefficient(self, value: float) -> float:
        """
        Normalize feature coefficient using log transform.
        
        Replicates __normalize_feature_coefficient() from ARAX_ranker.py lines 633-639.
        """
        # Handle zero value
        if value == 0:
            return 0.0
        
        log_abs_value = np.log(abs(value))
        max_value = 1
        curve_steepness = 2.75
        logistic_midpoint = 0.15
        return max_value / (1 + np.exp(-curve_steepness * (log_abs_value - logistic_midpoint)))

    @staticmethod
    def _combine_scores(base: float, scores: List[float]) -> float:
        """
        Combine base score with attribute scores using loop algorithm.
        
        Replicates _calculate_final_individual_edge_confidence() from ARAX_ranker.py lines 35-44.
        
        Algorithm: W_r = W_r + (1 - W_r) * W_i
        Scores are sorted in descending order before combining.
        
        Example:
            Given scores [0.994, 0.93, 0.85, 0.68]:
            Round 1: 0.994
            Round 2: 0.994 + (1-0.994) * 0.93 = 0.99958
            Round 3: 0.99958 + (1-0.99958) * 0.85 = 0.999937
            Round 4: 0.999937 + (1-0.999937) * 0.68 = 0.99997984
        """
        result = base
        for score in sorted(scores, reverse=True):
            result = result + (1 - result) * score
        return result

    def _score_results(self, msg: Dict) -> None:
        """
        Score all results using graph algorithms.
        
        Replicates ARAX_ranker.py lines 739-756.
        Uses three scoring methods combined via quantile ranking:
        1. Max flow
        2. Longest path
        3. Frobenius norm
        """
        qg = msg.get("query_graph", {})
        results = msg.get("results", [])
        
        if not results:
            return
        
        # Build query graph networkx (ARAX_ranker.py lines 27-32)
        qg_nx = self._build_query_graph_nx(qg)
        
        # Score using multiple methods (ARAX_ranker.py lines 743-750)
        scorers = [
            self._score_by_max_flow,
            self._score_by_longest_path,
            self._score_by_frobenius_norm
        ]
        
        ranks_list = []
        for scorer in scorers:
            scores = []
            for result in results:
                result_graph = self._build_result_graph_nx(qg_nx, result)
                scores.append(scorer(result_graph))
            ranks = self._quantile_rank(scores)
            ranks_list.append(ranks)
        
        # Combine ranks (ARAX_ranker.py line 752)
        result_scores = np.sum(ranks_list, axis=0) / len(ranks_list)
        
        # Assign scores to results (ARAX_ranker.py lines 755-763)
        for result, score in zip(results, result_scores):
            # Ensure minimum score of 0.001 (ARAX_ranker.py lines 761-763)
            final_score = float(score)
            if final_score < 0.001:
                final_score += 0.001
            
            if result.get("analyses"):
                result["analyses"][0]["score"] = final_score
            else:
                result["analyses"] = [{"score": final_score}]

    def _build_query_graph_nx(self, qg: Dict) -> nx.MultiDiGraph:
        """
        Build NetworkX graph from query graph.
        
        Replicates _get_query_graph_networkx_from_query_graph() from ARAX_ranker.py lines 27-32.
        Excludes creative_ nodes and edges.
        """
        g = nx.MultiDiGraph()
        
        nodes = qg.get("nodes", {})
        edges = qg.get("edges", {})
        
        # Add nodes (excluding creative_ nodes) - ARAX_ranker.py line 29
        for key in nodes:
            if 'creative_' not in key:
                g.add_node(key)
        
        # Add edges (excluding creative_ edges) - ARAX_ranker.py lines 30-31
        for key, edge in edges.items():
            if 'creative_' not in key:
                g.add_edge(edge["subject"], edge["object"], key=key, weight=0.0)
        
        return g

    def _build_result_graph_nx(self, qg_nx: nx.MultiDiGraph, result: Dict) -> nx.MultiDiGraph:
        """
        Build weighted result graph from query graph and result bindings.
        
        Replicates _get_weighted_graph_networkx_from_result_graph() from ARAX_ranker.py lines 101-128.
        Handles duplicate edge bindings by averaging their scores.
        """
        res_graph = qg_nx.copy()
        
        # Get edge tuples from query graph (ARAX_ranker.py lines 106-107)
        qg_edge_tuples = tuple(qg_nx.edges(keys=True, data=True))
        qg_edge_key_to_edge_tuple = {edge_tuple[2]: edge_tuple for edge_tuple in qg_edge_tuples}
        
        # Process edge bindings from analyses
        analyses = result.get("analyses", [])
        if not analyses:
            return res_graph
        
        # Collect valid edge info (ARAX_ranker.py lines 110-118)
        valid_edge_id_info = {}
        for analysis in analyses:
            edge_bindings = analysis.get("edge_bindings", {})
            
            for qedge_key, edge_binding_list in edge_bindings.items():
                if 'creative_' not in qedge_key:  # Ignore xDTD/xCRG supported edges
                    if qedge_key in qg_edge_key_to_edge_tuple:
                        qedge_tuple = qg_edge_key_to_edge_tuple[qedge_key]
                        valid_edge_id_info[qedge_key] = {
                            'edge_tuple': qedge_tuple,
                            'edge_binding_list': edge_binding_list
                        }
        
        # Process valid edge ids (ARAX_ranker.py lines 78-98)
        # This handles duplicate edges by averaging their scores
        for qedge_key, edge_info in valid_edge_id_info.items():
            qedge_tuple = edge_info['edge_tuple']
            edge_binding_list = edge_info['edge_binding_list']
            
            # Group edges by their ID suffix to handle duplicates
            # ARAX_ranker.py lines 87-92
            same_edge_ids: Dict[str, List[float]] = {}
            for binding in edge_binding_list:
                full_edge_id = binding.get("id", "")
                # Extract edge ID suffix: split by ':', take part after 2nd split
                # e.g., "infores:aragorn--MONDO:123:NCBIGene:456" -> "NCBIGene:456"
                edge_id_suffix = full_edge_id.split(':', 2)[-1] if full_edge_id else full_edge_id
                
                edge = self.kg_edge_id_to_edge.get(full_edge_id, {})
                confidence = edge.get("confidence", 0.5)
                
                if edge_id_suffix not in same_edge_ids:
                    same_edge_ids[edge_id_suffix] = []
                same_edge_ids[edge_id_suffix].append(confidence)
            
            # Average scores for each unique edge (ARAX_ranker.py lines 94-96)
            averaged_scores = []
            for edge_id_suffix, confidences in same_edge_ids.items():
                avg_score = sum(confidences) / len(confidences)
                averaged_scores.append(avg_score)
            
            # Combine averaged scores and assign to edge weight (ARAX_ranker.py line 126)
            if averaged_scores:
                combined_score = self._combine_scores(0, averaged_scores)
                res_graph[qedge_tuple[0]][qedge_tuple[1]][qedge_tuple[2]]['weight'] = combined_score
        
        return res_graph

    def _score_by_max_flow(self, graph: nx.MultiDiGraph) -> float:
        """
        Score graph using max flow algorithm.
        
        Replicates _score_networkx_graphs_by_max_flow() from ARAX_ranker.py lines 168-192.
        """
        if len(graph) <= 1:
            return 1.0
        
        try:
            # Find pairs with maximum path length (ARAX_ranker.py lines 173-178)
            apsp_dict = dict(nx.algorithms.shortest_paths.unweighted.all_pairs_shortest_path_length(graph))
            path_len_with_pairs_list = [
                (node_i, node_j, path_len) 
                for node_i, node_i_dict in apsp_dict.items() 
                for node_j, path_len in node_i_dict.items()
            ]
            
            if not path_len_with_pairs_list:
                return 0.0
            
            max_path_len = max(item[2] for item in path_len_with_pairs_list)
            pairs_with_max_path_len = [
                (item[0], item[1]) 
                for item in path_len_with_pairs_list 
                if item[2] == max_path_len
            ]
            
            # Collapse multigraph for flow calculation (ARAX_ranker.py line 180)
            collapsed = self._collapse_multigraph(graph)
            
            # Calculate max flow for each pair (ARAX_ranker.py lines 181-185)
            max_flow_values_for_node_pairs = []
            for source_node_id, target_node_id in pairs_with_max_path_len:
                try:
                    flow_value = nx.algorithms.flow.maximum_flow_value(
                        collapsed, source_node_id, target_node_id, capacity="weight"
                    )
                    max_flow_values_for_node_pairs.append(flow_value)
                except nx.NetworkXError:
                    continue
            
            # Combine flow values (ARAX_ranker.py lines 186-188)
            if len(max_flow_values_for_node_pairs) > 0:
                return self._combine_scores(0, max_flow_values_for_node_pairs)
            return 0.0
            
        except Exception as e:
            self.logger.debug(f"Max flow calculation failed: {e}")
            return 0.0

    def _score_by_longest_path(self, graph: nx.MultiDiGraph) -> float:
        """
        Score graph using longest path algorithm.
        
        Replicates _score_networkx_graphs_by_longest_path() from ARAX_ranker.py lines 195-212.
        Uses matrix power of adjacency matrix.
        """
        if len(graph) <= 1:
            return 1.0
        
        try:
            # Find pairs with maximum path length (ARAX_ranker.py lines 199-204)
            apsp_dict = dict(nx.algorithms.shortest_paths.unweighted.all_pairs_shortest_path_length(graph))
            path_len_with_pairs_list = [
                (node_i, node_j, path_len) 
                for node_i, node_i_dict in apsp_dict.items() 
                for node_j, path_len in node_i_dict.items()
            ]
            
            if not path_len_with_pairs_list:
                return 0.0
            
            max_path_len = max(item[2] for item in path_len_with_pairs_list)
            pairs_with_max_path_len = [
                (item[0], item[1]) 
                for item in path_len_with_pairs_list 
                if item[2] == max_path_len
            ]
            
            # Build node index mapping (ARAX_ranker.py line 205)
            map_node_name_to_index = {
                node_id: node_index 
                for node_index, node_id in enumerate(graph.nodes)
            }
            
            # Matrix power calculation (ARAX_ranker.py lines 206-207)
            adj_matrix = nx.to_numpy_array(graph)
            adj_matrix_power = np.linalg.matrix_power(adj_matrix, max_path_len) / math.factorial(max_path_len)
            
            # Get scores for max path pairs (ARAX_ranker.py lines 208-209)
            score_list = [
                adj_matrix_power[map_node_name_to_index[node_i], map_node_name_to_index[node_j]] 
                for node_i, node_j in pairs_with_max_path_len
            ]
            
            # Combine scores (ARAX_ranker.py line 210)
            return self._combine_scores(0, score_list)
            
        except Exception as e:
            self.logger.debug(f"Longest path calculation failed: {e}")
            return 0.0

    def _score_by_frobenius_norm(self, graph: nx.MultiDiGraph) -> float:
        """
        Score graph using Frobenius norm of adjacency matrix.
        
        Replicates _score_networkx_graphs_by_frobenius_norm() from ARAX_ranker.py lines 215-222.
        """
        try:
            adj_matrix = nx.to_numpy_array(graph)
            return float(np.linalg.norm(adj_matrix, ord='fro'))
        except Exception as e:
            self.logger.debug(f"Frobenius norm calculation failed: {e}")
            return 0.0

    @staticmethod
    def _collapse_multigraph(graph: nx.MultiDiGraph) -> nx.DiGraph:
        """
        Collapse multigraph edges by summing weights.
        
        Replicates _collapse_nx_multigraph_to_weighted_graph() from ARAX_ranker.py lines 144-157.
        """
        ret_graph = nx.DiGraph()
        for u, v, data in graph.edges(data=True):
            w = data.get('weight', 1.0)
            if ret_graph.has_edge(u, v):
                ret_graph[u][v]['weight'] += w
            else:
                ret_graph.add_edge(u, v, weight=w)
        return ret_graph

    @staticmethod
    def _quantile_rank(scores: List[float]) -> npt.NDArray[np.float64]:
        """
        Compute quantile ranks (ascending order, ties get same average rank).
        
        Replicates _quantile_rank_list() from ARAX_ranker.py lines 163-165.
        """
        y = scipy.stats.rankdata(scores, method='max')
        return y / len(y)

    def _sort_results(self, msg: Dict) -> None:
        """
        Sort results by score in descending order and break ties.
        
        Replicates ARAX_ranker.py lines 774-783.
        """
        results = msg.get("results", [])
        
        # Sort by score descending (ARAX_ranker.py line 774)
        results.sort(
            key=lambda r: r.get("analyses", [{}])[0].get("score", 0),
            reverse=True
        )
        
        # Break ties to preserve order (ARAX_ranker.py lines 776-783)
        scores_with_ties = [r.get("analyses", [{}])[0].get("score", 0) for r in results]
        scores_without_ties = self._break_ties(scores_with_ties)
        
        # Reinsert adjusted scores
        for result, score in zip(results, scores_without_ties):
            if result.get("analyses"):
                result["analyses"][0]["score"] = score

    @staticmethod
    def _break_ties(scores: List[float]) -> List[float]:
        """
        Break ties in scores while preserving order.
        
        Replicates _break_ties_and_preserve_order() from ARAX_ranker.py lines 237-260.
        Ensures strictly descending order by decreasing tied scores.
        """
        adjusted_scores = scores.copy()
        n = len(scores)
        
        # Only process first 1000 scores (ARAX_ranker.py lines 241-242)
        if n > 1000:
            n = 1000
        
        # Set scores beyond 1000 to 0 (ARAX_ranker.py lines 244-245)
        for i in range(n, len(adjusted_scores)):
            adjusted_scores[i] = 0
        
        # Round to 3 decimal places (ARAX_ranker.py line 248)
        adjusted_scores = [round(score, 3) for score in adjusted_scores]
        
        # Adjust to ensure strictly descending order (ARAX_ranker.py lines 251-255)
        for i in range(1, n):
            if adjusted_scores[i] >= adjusted_scores[i - 1]:
                new_score = adjusted_scores[i - 1] - 0.001
                adjusted_scores[i] = max(new_score, 0)
        
        # Final bounds check (ARAX_ranker.py line 258)
        return [round(max(min(score, 1), 0), 3) for score in adjusted_scores]


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
