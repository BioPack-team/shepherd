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

With ``settings.annotator_url`` set, the same annotate_curie_list call runs
on a deployed annotator service instead (POST /curie), for stacks outside the
cluster that cannot reach the Elasticsearch behind it.
"""

import re

import httpx
from biothings_annotator import annotator
from opentelemetry import trace

from shepherd_utils.ars.premerge import add_attribute, get_safe
from shepherd_utils.config import settings

ANNOTATION_MODES = ("merge", "premerge", "off")

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


async def _annotate_via_service(base_url, curie_list):
    """annotate_curie_list run by a deployed annotator service: POST /curie
    calls it there with the same defaults as the in-process call. Only
    query_backend is sent -- the service reads raw, include_extra and fields
    as strings, so e.g. raw=false would arrive truthy and skip the
    transforms."""
    async with httpx.AsyncClient(timeout=settings.annotator_timeout_sec) as client:
        response = await client.post(
            f"{base_url}/curie",
            params={"query_backend": "elasticsearch"},
            json={"ids": curie_list},
        )
    if response.status_code >= 400:
        # the service puts the underlying exception in the body; keep it
        raise RuntimeError(
            f"annotator service {response.status_code} from {response.url}: "
            f"{response.text[:1000]}"
        )
    return response.json()


async def _annotate_curie_list(curie_list, span):
    """The annotation call: a configured annotator service, or the package
    in-process (configured by its own env vars)."""
    base_url = settings.annotator_url.strip().rstrip("/")
    if base_url:
        span.set_attribute("annotator.transport", "service")
        span.set_attribute("annotator.api_host", base_url)
        return await _annotate_via_service(base_url, curie_list)
    atr = annotator.Annotator()
    span.set_attribute("annotator.transport", "in_process")
    span.set_attribute("annotator.query_backend", str(atr.query_backend))
    span.set_attribute("annotator.api_host", str(atr.api_host))
    return await atr.annotate_curie_list(curie_list)


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
    logger.info(f"annotating {len(curie_list)} curie ids")
    # A named span, as upstream's annotate_nodes wraps its package call: the
    # annotator is in-process (no separate Jaeger service), so this is what
    # makes the stage findable -- the package's outbound BioThings requests
    # appear as httpx client POST spans nested underneath.
    with tracer.start_as_current_span("annotator") as span:
        span.set_attribute("annotator.curie_count", len(curie_list))
        span.set_attribute("agent", agent_name)
        rj = await _annotate_curie_list(curie_list, span)
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
