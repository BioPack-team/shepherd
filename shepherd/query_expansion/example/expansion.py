from typing import Any


# function can be async or not
async def expand_example_query(query_body: dict[str, Any]) -> list[Any]:
    # query_body has already been validated to meet reasoner-pydantic's Query.
    # that is, a standard TRAPI query.

    # If you're interested in using reasoner-pydantic for stricter static typing,
    # see the indented block below. Otherwise, skip to the bottom.

    #     You can convert back to a Query:

    #     import Query from reasoner_pydantic
    #     query = Query.parse_obj(query_body)

    #     Then, when you have an expanded query still in reasoner-pydantic type,
    #     you can convert it back:

    #     return [expanded_query_1.dict()]

    # Query expansion must return a list of valid TRAPI queries.
    # If no expansion could be done, an empty list may be returned.
    # Let's just turn the given query into a lookup for now:
    q_edge = next(iter(query_body["message"]["query_graph"]["edges"].values()))
    q_edge.pop('knowledge_type', None)
    # These two are temporary because of retriever validator problems
    del query_body["message"]["knowledge_graph"]
    del query_body["workflow"]
    return [query_body]
