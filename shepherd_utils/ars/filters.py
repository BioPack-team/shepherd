"""Result filters for the /ars/api/filter endpoint.

Ported verbatim from NCATSTranslator/Relay @ 3e65975 tr_sys/tr_ars/utils.py:
hop_level_filter, score_filter, node_type_filter, specific_node_filter.
node_type_filter and specific_node_filter mutate ``results`` in place and
return it, exactly like upstream.
"""


def hop_level_filter(results, hop_limit):
    filtered_result = list(
        filter(
            lambda result: (len(result["node_bindings"].keys())) < hop_limit,
            results,
        )
    )
    return filtered_result


def score_filter(results, range):
    norm_score_results = list(
        filter(lambda result: result.get("normalized_score") is not None, results)
    )
    filtered_result = list(
        filter(
            lambda result: range[0] < result["normalized_score"] < range[1],
            norm_score_results,
        )
    )
    return filtered_result


def node_type_filter(kg_nodes, results, forbidden_category):
    forbidden_nodes = []
    for node, value in kg_nodes.items():
        present_category = []
        for entity in value["categories"]:
            if "biolink:" in entity:
                present_category.append(entity.split(":")[1])
            else:
                present_category.append(entity)
        if any(item in forbidden_category for item in present_category):
            forbidden_nodes.append(node)

    for result in list(results):
        ids = []
        for res_node, res_value in result["node_bindings"].items():
            for val in res_value:
                ids.append(str(val["id"]))
        if any(item in ids for item in forbidden_nodes):
            results.remove(result)
    return results


def specific_node_filter(results, forbbiden_node):
    for result in list(results):
        ids = []
        for res_node, res_value in result["node_bindings"].items():
            for val in res_value:
                ids.append(str(val["id"]))
        if any(item in ids for item in forbbiden_node):
            results.remove(result)
    return results
