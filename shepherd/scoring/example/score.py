from typing import Any
import random


# function can be async or not
async def example_scoring(response_body: dict[str, Any]) -> dict[str, Any]:
    # response_body is a valid TRAPI response

    # If you're interested in using reasoner-pydantic for stricter static typing,
    # see the indented block below. Otherwise, skip to the bottom.

    #    You can convert to a Response:

    #    import Response from reasoner_pydantic
    #    response = Response.parse_obj(response_body)

    #    Then, when you've scored the results, you can convert it back to return:

    #    return response.dict()

    # Scoring must return a valid TRAPI response with all results scored.
    # Don't do other transformations (such as trimming results/etc.), these are
    # handled after ARA scoring.
    # For an example, let's make every score random.
    for result in response_body["message"]["results"]:
        for analysis in result["analyses"]:
            analysis["score"] = random.uniform(0, 1)
    return response_body
