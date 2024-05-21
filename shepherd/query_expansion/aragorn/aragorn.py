"""Aragorn sub module."""
import copy
import json
from pathlib import Path
from string import Template


def get_infer_parameters(input_message):
    """Given an infer input message, return the parameters needed to run the infer.
    input_id: the curie of the input node
    predicate: the predicate of the inferred edge
    qualifiers: the qualifiers of the inferred edge
    source: the query node id of the source node
    target: the query node id of the target node
    source_input: True if the source node is the input node, False if the target node is the input node"""
    for edge_id, edge in input_message["message"]["query_graph"]["edges"].items():
        source = edge["subject"]
        target = edge["object"]
        query_edge = edge_id
        predicate = edge["predicates"][0]
        qc = edge.get("qualifier_constraints", [])
        if len(qc) == 0:
            qualifiers = {}
        else:
            qualifiers = {"qualifier_constraints": qc}
    if ("ids" in input_message["message"]["query_graph"]["nodes"][source]) \
            and (input_message["message"]["query_graph"]["nodes"][source]["ids"] is not None):
        input_id = input_message["message"]["query_graph"]["nodes"][source]["ids"][0]
        source_input = True
    else:
        input_id = input_message["message"]["query_graph"]["nodes"][target]["ids"][0]
        source_input = False
    #key = get_key(predicate, qualifiers)
    return input_id, predicate, qualifiers, source, source_input, target, query_edge


def get_rule_key(predicate, qualifiers):
    keydict = {'predicate': predicate}
    keydict.update(qualifiers)
    return json.dumps(keydict,sort_keys=True)


def expand_aragorn_query(input_message):
    # Contract: 
    # 1. there is a single edge in the query graph 
    # 2. The edge is marked inferred.
    # 3. Either the source or the target has IDs, but not both.
    # 4. The number of ids on the query node is 1.
    input_id, predicate, qualifiers, source, source_input, target, qedge_id = get_infer_parameters(input_message)
    key = get_rule_key(predicate, qualifiers)
    #We want to run the non-inferred version of the query as well
    qg = copy.deepcopy(input_message["message"]["query_graph"])
    for eid, edge in qg["edges"].items():
        del edge["knowledge_type"]
    with open(Path(__file__).parent / "rules_with_types_cleaned_finalized.json", "r") as file:
        AMIE_EXPANSIONS = json.load(file)
    messages = [{"message": {"query_graph":qg}, "parameters": input_message.get("parameters") or {}}]
    #If we don't have any AMIE expansions, this will just generate the direct query
    for rule_def in AMIE_EXPANSIONS.get(key,[]):
        query_template = Template(json.dumps(rule_def["template"]))
        #need to do a bit of surgery depending on what the input is.
        if source_input:
            qs = query_template.substitute(source=source,target=target,source_id = input_id, target_id='')
        else:
            qs = query_template.substitute(source=source, target=target, target_id=input_id, source_id='')
        query = json.loads(qs)
        if source_input:
            del query["query_graph"]["nodes"][target]["ids"]
        else:
            del query["query_graph"]["nodes"][source]["ids"]
        message = {"message": query, "parameters": input_message.get("parameters") or {}}
        if "log_level" in input_message:
            message["log_level"] = input_message["log_level"]
        messages.append(message)
    return messages
