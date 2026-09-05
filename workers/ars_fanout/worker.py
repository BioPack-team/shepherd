"""ARS fan-out worker.

Broadcasts a submitted query to every actor whose channels intersect the
parent's, creating one child message per actor and dispatching the query to
the remote ARA/KP. Port of NCATSTranslator/Relay @ 3e65975:
  - signals.py message_post_save (actor matching)
  - pubsub.py send_messages (skip rules: self, empty path, empty agent uri,
    inactive)
  - tasks.py send_message (child creation, callback injection, and the full
    response state machine)

Differences from upstream, all behavior-preserving (see the parity
register): the query is POSTed directly to the remote resolved via SmartAPI
instead of bouncing through the ARS's own /ara-*/api/runquery proxy view
(the remote sees the same body), and the async-200 self-GET race check is
dropped (upstream persists nothing on that path either way).
"""

import asyncio
import copy
import html
import json
import logging
import uuid

import httpx

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
import shepherd_utils.smartapi as smartapi
from shepherd_utils.ars.premerge import (
    ScoreStatCalc,
    get_safe,
    pre_merge_process,
    remove_phantom_support_graphs,
)
from shepherd_utils.ars.trapi import validate
from shepherd_utils.broker import add_task, mark_task_as_complete
from shepherd_utils.config import settings
from shepherd_utils.db import save_logs
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks

STREAM = "ars.fanout"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)


def _matching_actors(parent_actor, actors):
    """signals.py: any actor with a channel entry present in the parent's."""
    matching = []
    for actor in actors:
        for ch in actor.get("channel") or []:
            if ch in (parent_actor.get("channel") or []):
                matching.append(actor)
                break
    return matching


def _skip_actor(actor, parent_actor) -> bool:
    """pubsub.send_messages skip rules."""
    if actor["id"] == parent_actor["id"]:
        return True
    if len(actor.get("path") or "") == 0:
        return True
    if len(actor.get("agent_uri") or "") == 0:
        return True
    if not actor.get("active") or actor.get("active") == "0":
        return True
    return False


async def _finalize_child(child_pk, parent_pk, payload, updates, logger):
    """Persist a child transition; run the completion check when terminal."""
    if payload is not None:
        await ars_db.save_message_data(child_pk, payload, logger)
    updated = await ars_db.update_message(child_pk, **updates)
    if updated and updated["status"] in ("D", "S", "E", "U"):
        await ars_db.persist_data_copy(child_pk, logger)
        await lifecycle.check_parent_completion(parent_pk, logger)


async def send_to_actor(actor, parent, parent_data, logger):
    """tasks.send_message, one actor."""
    child = await ars_db.create_message(
        actor_id=actor["id"],
        status="Running",
        code=202,
        name=parent.get("name", ""),
        ref=parent["id"],
        params=parent.get("params"),
    )
    child_pk = child["id"]
    inforesid = actor.get("inforesid")
    agent_name = str(actor.get("agent_name"))
    data = copy.deepcopy(parent_data) if parent_data else {}
    actor_url = f"{actor.get('agent_uri', '')}{actor.get('path', '')}"
    callback = None
    if not actor_url.startswith("/ara-explanatory/api/runquery"):
        callback = f"{settings.ars_public_host}/ars/api/messages/{child_pk}"
        data["callback"] = callback

    endpoint = smartapi.endpoint(inforesid)
    url = smartapi.url_remote_from_inforesid(inforesid)
    rdata = data
    status = "U"
    try:
        if url is None:
            # upstream's proxy view 500s when the remote can't be resolved,
            # which send_message records as an error
            logger.warning(f"could not configure inforesid={inforesid}")
            await _finalize_child(
                child_pk, parent["id"], rdata, {"status": "E", "code": 500}, logger
            )
            return
        async with httpx.AsyncClient(timeout=settings.ars_query_timeout) as client:
            resp = await client.post(url, json=data)
        status_code = resp.status_code
        final_url = str(resp.url)
        if resp.status_code == 200:
            try:
                rdata = resp.json()
            except json.decoder.JSONDecodeError:
                rdata = {}
            if endpoint == "asyncquery":
                # results arrive on the callback; upstream persists nothing
                # here and the child stays R/202 from creation
                logger.info(f"[{child_pk}] {inforesid} accepted async query")
                return
            # synchronous (query) actor: process the response inline
            results = get_safe(rdata, "message", "results")
            result_count = None
            result_stat = None
            if results is not None and len(results) > 0:
                result_count = len(rdata["message"]["results"])
                result_stat = ScoreStatCalc(results)
                await asyncio.to_thread(
                    pre_merge_process, rdata, str(child_pk), agent_name, inforesid
                )
            updates = {"status": "D", "code": 200, "url": final_url}
            if result_count is not None:
                updates["result_count"] = result_count
                updates["result_stat"] = result_stat
            await ars_db.save_message_data(child_pk, rdata, logger)
            if results is not None and len(results) > 0:
                params = parent.get("params") or {}
                if "validate" in params.keys() and not params["validate"]:
                    valid = True
                else:
                    await asyncio.to_thread(remove_phantom_support_graphs, rdata)
                    valid = await asyncio.to_thread(validate, rdata)
                if valid:
                    if agent_name.startswith("ara-"):
                        await ars_db.save_message_data(child_pk, rdata, logger)
                        await add_task(
                            "ars.merge",
                            {
                                "parent_pk": str(parent["id"]),
                                "child_pk": str(child_pk),
                                "agent_name": agent_name,
                                "query_id": str(parent["id"]),
                                "otel": "{}",
                            },
                            logger,
                        )
                else:
                    logger.debug(
                        f"Validation problem found for agent {agent_name} "
                        f"with pk {parent['id']}"
                    )
                    updates = {
                        "status": "E",
                        "code": 422,
                        "url": final_url,
                        "result_count": result_count,
                        "result_stat": result_stat,
                    }
            updated = await ars_db.update_message(child_pk, **updates)
            if updated and updated["status"] in ("D", "S", "E", "U"):
                await ars_db.persist_data_copy(child_pk, logger)
                await lifecycle.check_parent_completion(parent["id"], logger)
            return
        if resp.status_code == 202:
            aresponse_url = (
                final_url[: final_url.rfind("/")] + "/aresponse/" + resp.text
            )
            await ars_db.save_message_data(child_pk, rdata, logger)
            await ars_db.update_message(
                child_pk, status="R", code=202, url=aresponse_url
            )
            return
        # >= 400 (and any other unexpected status)
        if "tr_ars.message.status" in resp.headers:
            status = resp.headers["tr_ars.message.status"]
        if resp.status_code >= 400:
            if resp.status_code != 503:
                status = "E"
            rdata["logs"] = []
            rdata["logs"].append(html.escape(resp.text))
        await _finalize_child(
            child_pk,
            parent["id"],
            rdata,
            {"status": status, "code": status_code, "url": final_url},
            logger,
        )
    except Exception as e:
        logger.error(
            f"Can't send message to actor {url} for pk: {child_pk}: {e}",
            exc_info=True,
        )
        await _finalize_child(
            child_pk, parent["id"], rdata, {"status": "E", "code": 500}, logger
        )


async def ars_fanout(task, logger: logging.Logger):
    parent_pk = task[1]["parent_pk"]
    parent = await ars_db.get_message_row(parent_pk)
    if parent is None:
        logger.error(f"Fanout: parent {parent_pk} not found")
        return
    parent_actor = await ars_db.get_actor(parent["actor"])
    if parent_actor is None:
        logger.error(f"Fanout: actor {parent['actor']} not found")
        return
    actors = await ars_db.list_actors()
    matching = _matching_actors(parent_actor, actors)
    parent_data = await ars_db.load_message_data(parent_pk, logger)
    targets = [a for a in matching if not _skip_actor(a, parent_actor)]
    logger.info(
        f"Fanning out {parent_pk} to {len(targets)} actor(s): "
        f"{[a['agent_name'] for a in targets]}"
    )
    await asyncio.gather(
        *(send_to_actor(actor, parent, parent_data, logger) for actor in targets)
    )


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Hand-rolled lifecycle: this stream is not a TRAPI workflow hop."""
    with tracer.start_as_current_span(STREAM, context=parent_ctx):
        try:
            await ars_fanout(task, logger)
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
