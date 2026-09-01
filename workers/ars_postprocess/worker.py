"""ARS post-process worker.

Runs the post-merge pipeline on a freshly merged message: blocklist removal,
null-attribute scrubbing, node annotation, the Appraiser call, Sugeno
scoring, and score stats. Port of NCATSTranslator/Relay @ dd1e71b utils.py
post_process + the tail of merge_and_post_process, with the exact
stage-failure codes: cleanup stages (blocklist / scrub / annotate / stats)
mark the merged child 'E'/444 but keep going (and the 444 sticks to the
end); appraise or scoring failures mark it 'E'/422 and stop. On success the
202 shell flips to 'D'/200. The merged_version_available notification and
the parent completion check run regardless of outcome, as upstream does.

Node annotation calls the annotator's HTTP API (settings.tr_annotator)
instead of importing the biothings_annotator package -- same service, same
resulting biothings_annotations attributes (parity register R2).
"""

import asyncio
import json
import logging
import re
import uuid

import httpx
import zstandard

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
import shepherd_utils.ars.scoring as scoring
from shepherd_utils.ars.blocklist import load_blocklist, remove_blocked
from shepherd_utils.ars.notify import notify_subscribers
from shepherd_utils.ars.premerge import (
    ScoreStatCalc,
    add_attribute,
    add_log_entry,
    get_safe,
    scrub_null_attributes,
    timestamp_hms,
)
from shepherd_utils.broker import mark_task_as_complete
from shepherd_utils.config import settings
from shepherd_utils.db import save_logs
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks

STREAM = "ars.postprocess"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

CURIE_PATTERN = re.compile(r"[\w\.]+:[\w\.]+")


def _separate_annotated_nodes(nodes):
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
        LOGGER.debug(f"separate_annotated_nodes: {e}")
    return unannotated


async def annotate_nodes(data, agent_name, logger):
    """utils.annotate_nodes via the annotator HTTP API."""
    nodes = get_safe(data, "message", "knowledge_graph", "nodes")
    if nodes is None:
        return
    curie_list = _separate_annotated_nodes(nodes)
    invalid_nodes = {}
    for key in list(curie_list):
        if not CURIE_PATTERN.match(str(key)):
            invalid_nodes[key] = nodes[key]
    for key in invalid_nodes.keys():
        curie_list.remove(key)
    if not curie_list:
        return
    logger.info(
        f"sending {len(curie_list)} curie ids to the annotator "
        f"{settings.tr_annotator}"
    )
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(
            settings.tr_annotator,
            json={"ids": curie_list},
            headers={"Content-type": "application/json"},
        )
    r.raise_for_status()
    rj = r.json()
    for key, value in rj.items():
        if (
            isinstance(value, list)
            and value
            and isinstance(value[0], dict)
            and value[0].get("notfound") is True
        ):
            pass
        elif isinstance(value, dict) and value == {}:
            pass
        elif key in data["message"]["knowledge_graph"]["nodes"]:
            attribute = {
                "attribute_type_id": "biothings_annotations",
                "value": value,
            }
            add_attribute(data["message"]["knowledge_graph"]["nodes"][key], attribute)
    if len(invalid_nodes) > 0:
        data["message"]["knowledge_graph"]["nodes"].update(invalid_nodes)


async def appraise(merged_pk, data, agent_name, logger):
    """utils.appraise: zstd request/response, 600s budget; raises on any
    failure (the caller applies the default-ordering fallback)."""
    copy_for_max = json.loads(json.dumps(data, default=str))
    copy_for_max["pk"] = str(merged_pk)
    headers = {"Accept-Encoding": "zstd", "Content-Encoding": "zstd"}
    payload = zstandard.ZstdCompressor().compress(
        json.dumps(copy_for_max, default=str).encode("utf-8")
    )
    logger.info(
        f"sending data for agent: {agent_name} to APPRAISER URL: "
        f"{settings.tr_appraise}"
    )
    async with httpx.AsyncClient(timeout=600) as client:
        r = await client.post(settings.tr_appraise, content=payload, headers=headers)
    if r.status_code != 200:
        logger.error(
            f"Received Error state from appraiser for agent {agent_name} and "
            f"pk {merged_pk}. Code {r.status_code}"
        )
        raise Exception(f"appraiser status {r.status_code}")
    content = r.content
    if content[:4] == b"\x28\xb5\x2f\xfd":
        rj = json.loads(zstandard.ZstdDecompressor().decompress(content))
    else:
        rj = json.loads(content)
    data["message"]["results"] = rj["message"]["results"]


async def ars_postprocess(task, logger: logging.Logger):
    merged_pk = task[1]["merged_pk"]
    parent_pk = task[1]["parent_pk"]
    agent_name = task[1]["agent_name"]
    try:
        stats = json.loads(task[1].get("stats", "{}"))
    except (json.JSONDecodeError, TypeError):
        stats = {}

    merged = await ars_db.get_message_row(merged_pk)
    if merged is None:
        logger.error(f"Postprocess: merged message {merged_pk} not found")
        return
    data = await ars_db.load_message_data(merged_pk, logger)
    if data is None:
        data = {}

    # local code/status mirror upstream's sticky variables
    code = None
    status = None
    row_code = merged["code"]

    # 1. blocklist
    try:
        await asyncio.to_thread(remove_blocked, data, load_blocklist(), str(merged_pk))
        await ars_db.save_message_data(merged_pk, data, logger)
    except Exception as e:
        status = "E"
        code = 444
        logger.exception(
            f"Problem with block list removal for agent: {agent_name} pk: "
            f"{merged_pk}: {e}"
        )
        await ars_db.update_message(merged_pk, status="E", code=444)
        row_code = 444

    # 2. scrub
    try:
        await asyncio.to_thread(scrub_null_attributes, data)
    except Exception:
        status = "E"
        code = 444
        logger.exception(
            f"Problem with the second scrubbing of null attributes for agent: "
            f"{agent_name} pk: {merged_pk}"
        )
        add_log_entry(
            data,
            [
                "Error in second scrubbing of null attributes",
                timestamp_hms(),
                "DEBUG",
            ],
        )
        await ars_db.update_message(merged_pk, status="E", code=444)
        row_code = 444

    # 3. annotate
    try:
        await annotate_nodes(data, agent_name, logger)
        logger.info(
            f"node annotation successful for agent {agent_name} and pk: " f"{merged_pk}"
        )
    except Exception as e:
        status = "E"
        code = 444
        add_log_entry(
            data,
            [
                f"node annotation internal error: {str(e)}",
                timestamp_hms(),
                "DEBUG",
            ],
        )
        logger.exception(
            f"problem with node annotation for agent: {agent_name} pk: {merged_pk}"
        )
        await ars_db.update_message(merged_pk, status="E", code=444)
        row_code = 444

    # 4. appraise
    try:
        await appraise(merged_pk, data, agent_name, logger)
    except Exception as e:
        logger.error(
            f"Problem with appraiser for agent {agent_name} and pk {merged_pk} "
            f"of type {type(e).__name__}"
        )
        logger.error(
            f"Adding default ordering_components for agent {agent_name} and "
            f"pk {merged_pk}"
        )
        results = get_safe(data, "message", "results")
        default_ordering_component = {
            "novelty": 0,
            "confidence": 0,
            "clinical_evidence": 0,
        }
        if results is not None:
            for result in results:
                if "ordering_components" not in result.keys():
                    result["ordering_components"] = default_ordering_component
        add_log_entry(data, ["Error in Appraiser " + str(e), timestamp_hms(), "ERROR"])
        await ars_db.save_message_data(merged_pk, data, logger)
        await ars_db.update_message(merged_pk, status="E", code=422)
        row_code = 422
        code = 422
        status = "E"

    result_count = None
    result_stat = None
    if row_code == 422:
        # appraise failure: return early with E/422 (upstream post_process)
        pass
    else:
        try:
            results = get_safe(data, "message", "results")
            if results is not None:
                new_res = await asyncio.to_thread(scoring.compute_from_results, results)
                data["message"]["results"] = new_res
            else:
                logger.error("results from appraiser returns None, cant do the scoring")
                new_res = []
        except Exception as e:
            status = "E"
            code = 422
            add_log_entry(
                data,
                ["Error in f-score calculation: " + str(e), timestamp_hms(), "ERROR"],
            )
            logger.exception("Error in f-score calculation")
            await ars_db.save_message_data(merged_pk, data, logger)
            await ars_db.update_message(
                merged_pk, skip_coercion=True, status="E", code=422
            )
            row_code = 422

        if row_code != 422:
            try:
                result_count = len(new_res)
                result_stat = ScoreStatCalc(new_res)
            except Exception:
                logger.exception("Error in ScoreStatCalculation or result count")
                add_log_entry(
                    data,
                    ["Error in score stat calculation", timestamp_hms(), "DEBUG"],
                )
                status = "E"
                code = 444
                await ars_db.save_message_data(merged_pk, data, logger)
                await ars_db.update_message(merged_pk, status="E", code=444)
                row_code = 444

            if row_code not in (422, 444) and code is None:
                # clean run: the 202 shell flips to Done/200
                code = 200
                status = "D"
            await ars_db.save_message_data(merged_pk, data, logger)

    # final state + notification + completion, regardless of outcome
    final_updates = {"status": status or "E", "code": code or row_code}
    if result_count is not None:
        final_updates["result_count"] = result_count
        final_updates["result_stat"] = result_stat
    await ars_db.update_message(merged_pk, skip_coercion=False, **final_updates)
    await ars_db.persist_data_copy(merged_pk, logger)

    parent = await ars_db.get_message_row(parent_pk)
    if parent is not None:
        await notify_subscribers(
            parent,
            {
                "event_type": "merged_version_available",
                "complete": False,
                "merged_version": str(merged_pk),
                "merged_versions_list": (
                    parent.get("merged_versions_list")
                    if parent.get("merged_versions_list") is not None
                    else []
                ),
                "stats": stats,
            },
            logger,
        )
    await lifecycle.check_parent_completion(parent_pk, logger)


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    with tracer.start_as_current_span(STREAM, context=parent_ctx):
        try:
            await ars_postprocess(task, logger)
        except Exception as e:
            logger.error(f"Task {task[0]} failed: {e}", exc_info=True)
        finally:
            try:
                await mark_task_as_complete(STREAM, GROUP, task[0], logger)
            except Exception as e:
                logger.error(f"Task {task[0]}: failed to ack: {e}")
            await save_logs(task[1].get("parent_pk", "ars"), logger)
            limiter.release()


async def poll_for_tasks():
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, TASK_LIMIT
            ):
                asyncio.create_task(process_task(task, parent_ctx, logger, limiter))
        except asyncio.CancelledError:
            LOGGER.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())
