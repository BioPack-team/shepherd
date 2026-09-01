"""TRAPI message merging, ported from NCATSTranslator/Relay @ dd1e71b
tr_sys/tr_ars/utils.py (TranslatorMessage & co, mergeMessages,
mergeMessagesRecursive, mergeDicts, get_msg_stats).

This is a deliberately faithful port: control flow, special-cased keys, the
early returns inside mergeDicts, the ``for``/``else`` in the node_bindings
branch, and the swallow-and-continue exception handling are all upstream
behavior that the golden parity suite (tests/unit/ars/test_golden_parity.py)
pins against the original implementation. Do not "fix" oddities here without
regenerating goldens and consciously accepting the divergence.
"""

import copy
import json
import logging
import typing

logger = logging.getLogger(__name__)


class QueryGraph:
    def __init__(self, qg):
        if qg is None:
            return
        self.__rawGraph = qg
        self.__nodes = qg["nodes"]
        self.__edges = qg["edges"] if "edges" in qg else []
        self.__paths = qg["paths"] if "paths" in qg else []

    def getEdges(self):
        return self.__edges

    def getNodes(self):
        return self.__nodes

    def getPaths(self):
        return self.__paths

    def getAllCuries(self):
        nodes = self.getNodes()
        curies = []
        for node in nodes:
            if "curie" in node:
                curies.append(node["curie"])
        return curies

    def getRawGraph(self):
        return self.__rawGraph

    def __json__(self):
        return json.dumps(self.getRawGraph())


class KnowledgeGraph:
    def __init__(self, kg):
        if kg is None:
            return
        self.rawGraph = kg
        self.__nodes = kg["nodes"]
        self.__edges = kg["edges"]

    def getEdges(self):
        return self.__edges

    def getNodes(self):
        return self.__nodes

    def getAllIds(self):
        nodes = self.getNodes()
        ids = []
        for node in nodes:
            ids.append(node)
        return ids

    def getNodeById(self, id):
        nodes = self.getNodes()
        node = nodes.get(id)
        return node

    def getRaw(self):
        return self.rawGraph

    def getEdgeById(self, id):
        edges = self.getEdges()
        edge = edges.get(id)
        return edge

    def __json__(self):
        return json.dumps(self.getRaw())


class Results:
    def __init__(self, results):
        if results is None:
            return
        self.__results = results

    def getEdgeBindings(self):
        edgeBindings = []
        for result in self.__results:
            try:
                bindings = result["edge_bindings"]
                edgeBindings.append(bindings)
            except Exception as e:
                logger.error(f"Unexpected error 3: {e}")
        return edgeBindings

    def getNodeBindings(self):
        nodeBindings = []
        for result in self.__results:
            nodeBindings.append(result["node_bindings"])
        return nodeBindings

    def getRaw(self):
        return self.__results


class TranslatorMessage:
    def __init__(self, message):
        if "results" in message:
            self.__results = Results(message["results"])
        else:
            self.__results = None

        if "knowledge_graph" in message:
            self.__kg = KnowledgeGraph(message["knowledge_graph"])
        else:
            self.__kg = None

        if "query_graph" in message:
            self.__qg = QueryGraph(message["query_graph"])
        else:
            self.__qg = None

        if "auxiliary_graphs" in message:
            self.__ag = message["auxiliary_graphs"]
        else:
            self.__ag = None
        self.__sharedResults = None

    def getResults(self):
        return self.__results

    def getQueryGraph(self):
        return self.__qg

    def getKnowledgeGraph(self):
        return self.__kg

    def getAuxiliaryGraphs(self):
        return self.__ag

    def getResultMap(self):
        """{frozenset(single-binding node ids): result} -- multi-binding
        nodes are excluded from the key, exactly like upstream."""
        map = {}
        results = self.getResults()
        if results is not None:
            results = results.getRaw()
        else:
            return None
        for result in results:
            nodes = set()
            nb = result["node_bindings"]
            for nodeid in nb.keys():
                binding = nb.get(nodeid)
                if len(binding) > 1:
                    logger.debug("Multiple bindings found for a single node")
                else:
                    binding = binding[0]
                    nodes.add(binding["id"])
            map[frozenset(nodes)] = result
        return map

    def setQueryGraph(self, qg):
        self.__qg = qg

    def setKnowledgeGraph(self, kg):
        self.__kg = kg

    def setResults(self, results):
        self.__results = results

    def setAuxGraphs(self, aux_graphs):
        self.__ag = aux_graphs

    def to_dict(self):
        d = {}
        if self.getQueryGraph() is not None:
            d["query_graph"] = self.getQueryGraph().getRawGraph()
        else:
            d["query_graph"] = {}
        if self.getKnowledgeGraph() is not None:
            d["knowledge_graph"] = self.getKnowledgeGraph().rawGraph
        else:
            d["knowledge_graph"] = {}
        if self.getResults() is not None:
            d["results"] = self.getResults().getRaw()
        else:
            d["results"] = {}
        if self.getAuxiliaryGraphs() is not None:
            d["auxiliary_graphs"] = self.getAuxiliaryGraphs()
        else:
            d["auxiliary_graphs"] = {}
        return {"message": d}


def mergeMessages(messageList, pk):
    messageListCopy = copy.deepcopy(messageList)
    message = messageListCopy.pop()
    merged = mergeMessagesRecursive(message, messageListCopy, pk)
    return merged


def mergeMessagesRecursive(mergedMessage, messageList, pk):
    if len(messageList) == 0:
        try:
            results = mergedMessage.getResults()
            if results is not None:
                try:
                    results = results.getRaw()
                    for result in results:
                        if "normalized_score" in result.keys():
                            ns = result["normalized_score"]
                            if isinstance(ns, list) and len(ns) > 0:
                                result["normalized_score"] = sum(ns) / len(ns)
                except Exception as e:
                    logger.debug(f"normalized score averaging failed: {e}")
        except Exception as e:
            logger.debug(f"{e}")
        if mergedMessage is not None:
            mergedMessage.status = "Done"
            mergedMessage.code = 200
        return mergedMessage
    else:
        currentMessage = messageList.pop()
        ckg = currentMessage.getKnowledgeGraph().getRaw()
        mkg = mergedMessage.getKnowledgeGraph().getRaw()
        mergedKnowledgeGraph = mergeDicts(ckg, mkg)

        currentResultMap = currentMessage.getResultMap()
        mergedResultMap = mergedMessage.getResultMap()
        mergeDicts(currentResultMap, mergedResultMap)

        currentAux = currentMessage.getAuxiliaryGraphs()
        mergedAux = mergedMessage.getAuxiliaryGraphs()
        mergedAux = mergeDicts(currentAux, mergedAux)

        values = mergedResultMap.values()
        newResults = Results(list(values))
        mergedMessage.setResults(newResults)
        mergedMessage.setKnowledgeGraph(KnowledgeGraph(mergedKnowledgeGraph))
        mergedMessage.setAuxGraphs(mergedAux)

        return mergeMessagesRecursive(mergedMessage, messageList, pk)


def mergeDicts(dcurrent, dmerged):
    if dcurrent is None:
        dcurrent = {}
    if dmerged is None:
        dmerged = {}
    for key in dcurrent.keys():
        cv = dcurrent[key]
        if key in dmerged.keys():
            mv = dmerged[key]
            if key == "node_bindings":
                cvv = [
                    {node_key: node_value[0]}
                    for node_key, node_value in cv.items()
                    if "id" in node_value[0]
                ]
                mvv = [
                    {node_key: node_value[0]}
                    for node_key, node_value in mv.items()
                    if "id" in node_value[0]
                ]
                if all(isinstance(x, dict) for x in mvv) and all(
                    isinstance(y, dict) for y in cvv
                ):
                    cmap = {}
                    mmap = {}
                    for cd in cvv:
                        for cd_key, cd_val in cd.items():
                            if "id" in cd_val:
                                cmap[cd_val["id"]] = cd_val
                    for md in mvv:
                        for md_key, md_val in md.items():
                            if "id" in md_val:
                                mmap[md_val["id"]] = md_val

                    # NOTE: upstream has a for/else here (the else belongs to
                    # the for loop, not the if) -- after the loop finishes,
                    # the LAST ck is re-assigned into mmap. Faithfully kept.
                    for ck in cmap.keys():
                        if ck in mmap.keys():
                            mmap[ck] = mergeDicts(cmap[ck], mmap[ck])
                    else:
                        mmap[ck] = cmap[ck]  # noqa: F821 -- upstream for/else

            # attributes are another special case. We largely want to append,
            # but combine values of matching attributes whose value are lists.
            elif key == "attributes":
                for current_attribute in cv:
                    if (
                        "attribute_type_id" in current_attribute.keys()
                        and "value" in current_attribute.keys()
                    ):
                        current_type_id = current_attribute["attribute_type_id"]
                        occurence_count = 0
                        for merged_attribute in mv:
                            if (
                                "attribute_type_id" in merged_attribute.keys()
                                and merged_attribute["attribute_type_id"]
                                == current_type_id
                            ):
                                occurence_count += 1
                                if occurence_count > 1:
                                    break

                        if (
                            occurence_count > 1
                            or occurence_count == 0
                            or not isinstance(current_attribute["value"], list)
                        ):
                            mv.append(current_attribute)
                        else:
                            try:
                                for merged_attribute in mv:
                                    if (
                                        merged_attribute["attribute_type_id"]
                                        == current_type_id
                                    ):
                                        new_value = list(
                                            set(
                                                merged_attribute["value"]
                                                + current_attribute["value"]
                                            )
                                        )
                                        merged_attribute["value"] = new_value
                                        break
                            except Exception as e:
                                logger.error(f"attribute merge failure: {e}")

                # upstream returns early here, skipping any remaining keys
                return dmerged
            # analyses are a special case: append at the result level
            elif key == "analyses":
                dmerged[key] = mv + cv
                return dmerged
            elif isinstance(cv, dict) and isinstance(mv, dict):
                dmerged[key] = mergeDicts(cv, mv)
            elif isinstance(mv, list) and not isinstance(cv, list):
                mv.append(cv)
            elif isinstance(mv, list) and isinstance(cv, list):
                try:
                    if all(isinstance(x, dict) for x in mv) and all(
                        isinstance(y, dict) for y in cv
                    ):
                        cmap = {}
                        mmap = {}
                        for cd in cv:
                            if "resource_id" in cd.keys():
                                cmap[cd["resource_id"]] = cd
                            elif "qualifier_type_id" in cd.keys():
                                cmap[cd["qualifier_type_id"]] = cd
                            else:
                                pass
                        for md in mv:
                            if "resource_id" in md.keys():
                                mmap[md["resource_id"]] = md
                            # upstream bug kept verbatim: qualifier dicts are
                            # keyed by md["resource_id"], which KeyErrors and
                            # is swallowed below -- so qualifier lists never
                            # actually merge (mv is left as-is).
                            elif "qualifier_type_id" in md.keys():
                                mmap[md["resource_id"]] = md
                            else:
                                pass

                        for ck in cmap.keys():
                            if ck in mmap.keys():
                                mmap[ck] = mergeDicts(cmap[ck], mmap[ck])
                            else:
                                mmap[ck] = cmap[ck]
                        dmerged[key] = list(mmap.values())

                    elif all(isinstance(x, typing.Hashable) for x in mv) and all(
                        isinstance(y, typing.Hashable) for y in cv
                    ):
                        dmerged[key] = mv + list(set(cv) - set(mv))
                    else:
                        dmerged[key] = mv + cv

                except Exception as e:
                    logger.debug(f"list merge fallback: {e}")
            else:
                try:
                    if (
                        (
                            isinstance(mv, typing.Hashable)
                            and isinstance(cv, typing.Hashable)
                            and mv == cv
                        )
                        or cv is None
                        or mv is None
                    ):
                        continue
                    else:
                        if key == "score":
                            del dmerged[key]
                            dmerged["scores"] = [mv, cv]
                        elif key == "query_ids":
                            dmerged["query_ids"] = [mv, cv]
                        elif key == "name":
                            # kg node names can't be a list
                            continue
                        else:
                            dmerged[key] = [mv, cv]
                except Exception as e:
                    logger.debug(f"scalar merge fallback: {e}")
        else:
            dmerged[key] = cv
    return dmerged


def get_msg_stats(mesg_dict):
    """Component counts for the parent's params.stats, ported verbatim."""
    from .premerge import get_safe

    stats = {}
    for component in mesg_dict["message"].keys():
        if component == "knowledge_graph":
            for subComp in ["nodes", "edges"]:
                stats[f"{component}_{subComp}"] = len(
                    get_safe(mesg_dict, "message", f"{component}", f"{subComp}")
                )
        else:
            stats[component] = len(get_safe(mesg_dict, "message", f"{component}"))
    return stats
