"""Aragorn scoring."""
import httpx


async def send_to_aragorn_ranker(message):
    """Use Aragorn Ranker for Aragorn message scoring."""
    async with httpx.AsyncClient(timeout=120) as client:
        response = await client.post(
            "https://aragorn-ranker.renci.org/score",
            json=message,
        )
        response.raise_for_status()
    return response.json()
