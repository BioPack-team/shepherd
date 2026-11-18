"""Node annotation (upstream utils.annotate_nodes) via the in-process
biothings_annotator package, shared by the two places it can run:

  - ``ars_merge``'s post-process, over every merged version (upstream's
    placement; ``ars_annotation_mode = "merge"``), and
  - ``ars_premerge``'s pool child, over each ARA response before it is
    handed to the merge (``ars_annotation_mode = "premerge"``). Premerge runs
    per ARA and in parallel, so the annotator's BioThings round trips come
    off the per-parent merge lock instead of serializing every later merge
    behind them.

Nodes that already carry a ``biothings_annotations`` attribute are skipped
either way -- which is also what makes annotations shipped in the graph
itself (e.g. from tier0) free at run time.
"""

import re

from biothings_annotator import annotator
from biothings_annotator.annotator import settings as annotator_settings
from opentelemetry import trace

from shepherd_utils.ars.premerge import add_attribute, get_safe
from shepherd_utils.config import settings

ANNOTATION_MODES = ("merge", "premerge", "off")

# The package only reaches Elasticsearch through named presets with
# hardcoded hosts, so settings.annotator_es_url is registered as one more.
SHEPHERD_ES_CONNECTION = "shepherd"

CURIE_PATTERN = re.compile(r"[\w\.]+:[\w\.]+")

tracer = trace.get_tracer(__name__)


def _separate_annotated_nodes(nodes, logger):
    """sperate_annotated_nodes [sic]: curies lacking a biothings_annotations
    attribute."""
    unannotated = []
    try:
        for curie, value in nodes.items():
            if "attribute" in value.keys() and value["attributes"] == []:
                unannotated.append(curie)
            else:
                annotated = False
                for attribute in value.get("attributes") or []:
                    if (
                        "attribute_type_id" in attribute.keys()
                        and attribute["attribute_type_id"] == "biothings_annotations"
                    ):
                        annotated = True
                if not annotated:
                    unannotated.append(curie)
    except Exception as e:
        logger.debug(f"separate_annotated_nodes: {e}")
    return unannotated


def _new_annotator():
    """An Annotator for this deployment. With ``annotator_es_url`` set it
    queries that Elasticsearch, overriding the package's env vars; otherwise
    the package configures itself from the environment, as upstream."""
    es_url = settings.annotator_es_url.strip()
    if not es_url:
        return annotator.Annotator()
    annotator_settings.ELASTICSEARCH_CONNECTIONS[SHEPHERD_ES_CONNECTION] = {
        "host": es_url,
        "headers": {},
    }
    atr = annotator.Annotator(query_backend="elasticsearch")
    atr.elasticsearch_connection = SHEPHERD_ES_CONNECTION
    return atr


def _annotator_host(atr):
    """Where this Annotator's queries go, for the trace span."""
    if atr.query_backend == "elasticsearch":
        connection = annotator_settings.ELASTICSEARCH_CONNECTIONS.get(
            atr.elasticsearch_connection, {}
        )
        return str(connection.get("host", atr.elasticsearch_connection))
    return str(atr.api_host)


async def annotate_nodes(data, agent_name, logger):
    """utils.annotate_nodes via the in-process biothings_annotator package,
    as upstream. The consumption loop is verbatim, quirks included: a
    non-dict or empty-list value crashes the notfound check (-> the caller's
    E/444), and annotated values index the node dict directly."""
    nodes = get_safe(data, "message", "knowledge_graph", "nodes")
    if nodes is None:
        return
    curie_list = _separate_annotated_nodes(nodes, logger)
    invalid_nodes = {}
    for key in list(curie_list):
        if not CURIE_PATTERN.match(str(key)):
            invalid_nodes[key] = nodes[key]
    for key in invalid_nodes.keys():
        curie_list.remove(key)
    if not curie_list:
        return
    logger.info(f"annotating {len(curie_list)} curie ids in-process")
    # A named span, as upstream's annotate_nodes wraps its package call: the
    # annotator is in-process (no separate Jaeger service), so this is what
    # makes the stage findable -- the package's outbound BioThings requests
    # appear as httpx client POST spans nested underneath.
    with tracer.start_as_current_span("annotator") as span:
        span.set_attribute("annotator.curie_count", len(curie_list))
        span.set_attribute("agent", agent_name)
        atr = _new_annotator()
        span.set_attribute("annotator.query_backend", str(atr.query_backend))
        span.set_attribute("annotator.api_host", _annotator_host(atr))
        rj = await atr.annotate_curie_list(curie_list)
        annotated = 0
        for key, value in rj.items():
            if (
                isinstance(value, list)
                and "notfound" in value[0].keys()
                and value[0]["notfound"] == True  # noqa: E712 -- upstream verbatim
            ):
                pass
            elif isinstance(value, dict) and value == {}:
                pass
            else:
                attribute = {
                    "attribute_type_id": "biothings_annotations",
                    "value": value,
                }
                add_attribute(
                    data["message"]["knowledge_graph"]["nodes"][key], attribute
                )
                annotated += 1
        span.set_attribute("annotator.annotated_count", annotated)
        if len(invalid_nodes) > 0:
            data["message"]["knowledge_graph"]["nodes"].update(invalid_nodes)
