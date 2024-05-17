"""Query Retrieval."""
import asyncio
import httpx
import json
import os

retriever_url = "http://localhost:3000/v1/asyncquery"


async def send_query(query_id: str, query, semaphore):
    """Send a single query to Retriever."""
    try:
        async with semaphore:
            # had an open spot in the queue
            callback_host = os.environ.get("CALLBACK_HOST", "http://127.0.0.1:5439")
            # callback_host = "http://localhost:3000"

            # callback_url = f"{callback_host}/query"
            callback_url = f"{callback_host}/callback/{query_id}"
            query["callback"] = callback_url
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    retriever_url,
                    json=query,
                )
                response.raise_for_status()
    except Exception as e:
        print(e.response.text)


async def retrieve(query_id, expanded_queries, options):
    """Send all queries to Retriever."""
    semaphore = asyncio.Semaphore(options.get("concurrency", 1))
    queries = [asyncio.create_task(send_query(query_id, query, semaphore)) for query in expanded_queries]
    await asyncio.gather(*queries)
