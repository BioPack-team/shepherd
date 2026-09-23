"""TRAPI message merging, ported from NCATSTranslator/Relay @ 3e65975
tr_sys/tr_ars/utils.py (TranslatorMessage & co, mergeMessages,
mergeMessagesRecursive, mergeDicts, get_msg_stats).

Control flow, special-cased keys and the swallow-and-continue exception
handling follow upstream, and the golden parity suite
(tests/unit/ars/test_golden_parity.py) pins the outputs. These upstream bugs
are deliberately NOT reproduced (see docs/ARS_PARITY_REGISTER.md):

  - ``mergeDicts`` returned out of the ``attributes`` and ``analyses``
    branches, abandoning every key it had not reached yet;
  - the ``node_bindings`` branch hung its ``else`` off the ``for`` instead
    of the ``if``, so it kept only the last current-only binding -- in a
    local map it never wrote back -- and ignored bindings past the first;
  - ``TranslatorMessage.to_dict`` emitted ``"results": {}`` for a message
    with no results, which is not valid TRAPI;
  - the ``attributes`` branch deduped a merged value list with ``set()``,
    which raises on a list of objects and silently dropped the current
    agent's values for that attribute (see ``_union_values``);
  - the list-of-objects branch keyed entries on ``resource_id`` /
    ``qualifier_type_id`` and dropped every object carrying neither, from
    both sides, so such a list merged to ``[]``.

TRAPI 2.0 (Shepherd speaks 2.0; upstream is 1.5, and the goldens are 2.0
translations of the Relay goldens -- see the register):

  - node bindings are one ``{"ids": [...]}`` object per query node; the
    result map keys on single-id bindings and the ``node_bindings`` branch
    unions ``ids``;
  - a conflict on a single-valued TRAPI member (``knowledge_level``,
    ``agent_type``, ``predicate``, ``qualifier_value`` ...) keeps the first
    value (a provided knowledge level / agent type beats ``not_provided``)
    instead of becoming ``[merged, current]`` (``_resolve_scalar_conflict``);
  - ``to_dict`` omits an absent query graph / knowledge graph / auxiliary
    graphs rather than emitting ``{}``;
  - the 1.x-only paths (result-level ``edge_bindings``, the node-binding
    ``query_ids`` special case, the legacy ``curie`` query-node lookup) are
    gone.

Any further change here needs the goldens re-recorded and the divergence
written down.
"""

import copy
import json
import logging
import typing

from shepherd_utils.trapi import binding_ids

logger = logging.getLogger(__name__)


class QueryGraph:
    def __init__(self, qg):
        # upstream returned early on None, leaving every attribute unset so
        # the next getter raised AttributeError instead of reading empty
        self.__rawGraph = qg if qg is not None else {}
        qg = self.__rawGraph
        self.__nodes = qg.get("nodes", {})
        self.__edges = qg.get("edges", [])
        self.__paths = qg.get("paths", [])

    def getEdges(self):
        return self.__edges

    def getNodes(self):
        return self.__nodes

    def getPaths(self):
        return self.__paths

    def getRawGraph(self):
        return self.__rawGraph

    def __json__(self):
        return json.dumps(self.getRawGraph())


class KnowledgeGraph:
    def __init__(self, kg):
        # as QueryGraph: empty rather than unset
        self.rawGraph = kg if kg is not None else {"nodes": {}, "edges": {}}
        kg = self.rawGraph
        self.__nodes = kg.get("nodes", {})
        self.__edges = kg.get("edges", {})

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
        # as QueryGraph: empty rather than unset
        self.__results = results if results is not None else []

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
        """{frozenset(single-binding node ids): result} -- a query node bound
        to more than one id is excluded from the key, exactly like upstream.

        TRAPI 2.0: a node binding is one ``{"ids": [...]}`` object per query
        node, so "single-binding" means a single id (upstream: a one-element
        list of ``{"id"}`` objects)."""
        map = {}
        results = self.getResults()
        if results is not None:
            results = results.getRaw()
        else:
            return None
        for result in results:
            nodes = set()
            nb = result.get("node_bindings") or {}
            for nodeid in nb.keys():
                ids = binding_ids(nb.get(nodeid))
                if len(ids) > 1:
                    logger.debug("Multiple bindings found for a single node")
                elif ids:
                    nodes.add(ids[0])
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
        """The message as a TRAPI 2.0 ``{"message": ...}`` dict.

        Upstream filled every absent component with ``{}`` (and ``results``
        too, which is an array). In TRAPI 2.0 an empty query graph or
        knowledge graph is invalid (``nodes`` is required) and
        ``auxiliary_graphs`` has minProperties 1, so an absent or empty
        component is omitted instead; ``results`` is ``[]`` when there are
        none, as 2.0 asks of a response.
        """
        d = {}
        qg = self.getQueryGraph()
        if qg is not None and qg.getRawGraph():
            d["query_graph"] = qg.getRawGraph()
        kg = self.getKnowledgeGraph()
        if kg is not None and kg.getRaw():
            kg.getRaw().setdefault("nodes", {})
            d["knowledge_graph"] = kg.getRaw()
        if self.getResults() is not None:
            d["results"] = self.getResults().getRaw()
        else:
            d["results"] = []
        if self.getAuxiliaryGraphs():
            d["auxiliary_graphs"] = self.getAuxiliaryGraphs()
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
        # knowledge_graph and results are optional in TRAPI 2.0; upstream
        # dereferenced both unconditionally (AttributeError / TypeError)
        ckg = _raw_kg(currentMessage)
        mkg = _raw_kg(mergedMessage)
        mergedKnowledgeGraph = mergeDicts(ckg, mkg)

        currentResultMap = currentMessage.getResultMap()
        mergedResultMap = mergedMessage.getResultMap()
        if mergedResultMap is None:
            mergedResultMap = {}
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


def _raw_kg(message):
    kg = message.getKnowledgeGraph()
    return kg.getRaw() if kg is not None else None


def _object_key(item):
    """What mergeDicts matches two objects in a list on, or None."""
    for field in ("resource_id", "qualifier_type_id"):
        if field in item:
            return (field, item[field])
    return None


def _value_key(value):
    """A stable identity for one attribute value, hashable or not.

    Scalars are keyed by (type, value) so 1, True and "1" stay distinct;
    anything else is keyed by its sorted-key serialization, which gives
    dicts and lists an identity a set can hold.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return (type(value).__name__, value)
    try:
        return ("json", json.dumps(value, sort_keys=True, default=repr))
    except Exception:
        return ("repr", repr(value))


def _as_value_list(value):
    if isinstance(value, list):
        return value
    return [] if value is None else [value]


def _union_values(merged_value, current_value):
    """Union two attribute value lists, preserving order and dropping repeats.

    Upstream did ``list(set(merged + current))``. A value list of OBJECTS --
    publications carrying metadata, for instance -- raised
    ``TypeError: unhashable type: 'dict'``; the generic except swallowed it,
    the ``break`` never ran, and because this is the else-branch of "append
    the whole attribute", the current agent's values were dropped from the
    merged message entirely. Serializing unhashable members gives them an
    identity to dedupe on, and keeping insertion order makes the result
    independent of the hash seed (upstream's set union was not).
    """
    out = []
    seen = set()
    for value in _as_value_list(merged_value) + _as_value_list(current_value):
        key = _value_key(value)
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


#: TRAPI 2.0 members whose schema type is a single string / boolean / enum,
#: which is to say every scalar member of Edge, Node (``name`` is handled on
#: its own, as upstream did), RetrievalSource, Qualifier and Attribute, plus
#: Analysis's. Upstream turned ANY conflicting scalar into ``[merged,
#: current]``; for these that produces an invalid document (a list where a
#: string is required), so a conflict keeps one value instead -- see
#: ``_resolve_scalar_conflict``. Free-form members (``Attribute.value``) and
#: extra properties the ARS or an ARA hangs on a result keep upstream's
#: list-of-both behaviour.
_SINGLE_VALUED_MEMBERS = frozenset(
    {
        # Edge
        "subject",
        "object",
        "predicate",
        "knowledge_level",
        "agent_type",
        # Node
        "is_set",
        # RetrievalSource
        "resource_id",
        "resource_role",
        # Qualifier
        "qualifier_type_id",
        "qualifier_value",
        # Attribute
        "attribute_type_id",
        "value_type_id",
        "original_attribute_name",
        "value_url",
        "attribute_source",
        "description",
        # Analysis
        "scoring_method",
    }
)

#: The biolink value that says a knowledge_level / agent_type is unknown.
NOT_PROVIDED = "not_provided"


def _resolve_scalar_conflict(key, merged_value, current_value):
    """Pick the one value a conflicting single-valued member keeps.

    The value that got there first wins: ``current_value`` (``dcurrent``).
    In the ARS fold ``merge_received`` folds the accumulated merged version
    (``dcurrent``) INTO the newly arriving ARA's message (``dmerged``), so
    that is the value every earlier merged version already served -- a
    later ARA cannot flip an edge's knowledge level under a client that has
    read it. (For ``mergeMessages([a, b])`` generally: ``a``'s value.)

    ``knowledge_level`` / ``agent_type`` add one precedence: a provided
    value beats biolink's ``not_provided`` (the value TOM's 1.x -> 2.0
    conversion fills in when an ARA said nothing), so the merge never loses
    information to a default. There is no biolink ordering between two
    provided knowledge levels that would make a better tie-break than
    arrival order.
    """
    if key in ("knowledge_level", "agent_type"):
        if current_value == NOT_PROVIDED and merged_value != NOT_PROVIDED:
            return merged_value
    return current_value


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
                # Union the two results' bindings per query-graph node.
                #
                # Upstream built parallel {node: first_binding} maps, merged
                # the ids present in both, and then -- because its ``else``
                # hung off the ``for`` rather than the ``if`` -- copied only
                # the LAST current-only id, into a local dict it never wrote
                # back. So bindings past the first were ignored and
                # current-only bindings were dropped.
                #
                # TRAPI 2.0: one {"ids": [...]} object per query node, so the
                # union is of ids (order-stable, merged side first); any other
                # member a binding carries is folded with mergeDicts.
                for node_key, current_binding in cv.items():
                    merged_binding = mv.get(node_key)
                    if not isinstance(merged_binding, dict):
                        mv[node_key] = current_binding
                        continue
                    if not isinstance(current_binding, dict):
                        continue
                    ids = list(
                        dict.fromkeys(
                            binding_ids(merged_binding) + binding_ids(current_binding)
                        )
                    )
                    rest = {k: v for k, v in current_binding.items() if k != "ids"}
                    if rest:
                        mergeDicts(rest, merged_binding)
                    merged_binding["ids"] = ids
                dmerged[key] = mv

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
                            folded = False
                            try:
                                for merged_attribute in mv:
                                    if (
                                        merged_attribute.get("attribute_type_id")
                                        == current_type_id
                                    ):
                                        merged_attribute["value"] = _union_values(
                                            merged_attribute.get("value"),
                                            current_attribute["value"],
                                        )
                                        folded = True
                                        break
                            except Exception as e:
                                logger.error(f"attribute merge failure: {e}")
                            if not folded:
                                # never silently drop a contribution: if the
                                # fold did not happen, keep the attribute
                                mv.append(current_attribute)

                # upstream returned here, abandoning every key it had not
                # reached yet -- so whatever followed "attributes" in the
                # current dict was silently never merged
                continue
            # analyses are a special case: append at the result level
            elif key == "analyses":
                # likewise: upstream returned instead of continuing
                dmerged[key] = mv + cv
                continue
            elif isinstance(cv, dict) and isinstance(mv, dict):
                dmerged[key] = mergeDicts(cv, mv)
            elif isinstance(mv, list) and not isinstance(cv, list):
                mv.append(cv)
            elif isinstance(mv, list) and isinstance(cv, list):
                try:
                    if all(isinstance(x, dict) for x in mv) and all(
                        isinstance(y, dict) for y in cv
                    ):
                        # Objects are matched on resource_id / qualifier_type_id.
                        # Upstream dropped every object carrying NEITHER field --
                        # from both sides, since it replaced the merged list with
                        # the keyed map's values -- so a list of objects of any
                        # other shape merged to []. They are carried through here
                        # instead, deduped, after the keyed ones.
                        mmap = {}
                        unkeyed = []
                        for md in mv:
                            mkey = _object_key(md)
                            if mkey is None:
                                unkeyed.append(md)
                            else:
                                mmap[mkey] = md
                        for cd in cv:
                            ckey = _object_key(cd)
                            if ckey is None:
                                unkeyed.append(cd)
                            elif ckey in mmap:
                                mmap[ckey] = mergeDicts(cd, mmap[ckey])
                            else:
                                mmap[ckey] = cd
                        dmerged[key] = list(mmap.values()) + _union_values(
                            None, unkeyed
                        )

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
                        elif key == "name":
                            # kg node names can't be a list
                            continue
                        elif key in _SINGLE_VALUED_MEMBERS:
                            # a TRAPI-typed scalar can't be a list either
                            dmerged[key] = _resolve_scalar_conflict(key, mv, cv)
                        else:
                            dmerged[key] = [mv, cv]
                except Exception as e:
                    logger.debug(f"scalar merge fallback: {e}")
        else:
            dmerged[key] = cv
    return dmerged


#: Components every stats block reports, 0 when absent. Upstream's to_dict
#: always emitted all four (as ``{}`` when empty); the 2.0 to_dict omits an
#: absent one, and the stats shape subscribers see must not change with it.
_STATS_COMPONENTS = ("query_graph", "knowledge_graph", "results", "auxiliary_graphs")


def get_msg_stats(mesg_dict):
    """Component counts for the parent's params.stats (upstream's shape).

    Tolerates absent / null components, which TRAPI 2.0 allows."""
    message = mesg_dict.get("message") or {}
    stats = {}
    for component in dict.fromkeys((*_STATS_COMPONENTS, *message.keys())):
        value = message.get(component) or {}
        if component == "knowledge_graph":
            for subComp in ["nodes", "edges"]:
                stats[f"{component}_{subComp}"] = len(value.get(subComp) or {})
        else:
            stats[component] = len(value)
    return stats
