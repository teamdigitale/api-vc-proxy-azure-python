import logging
import json

import azure.functions as func
from .vocabularies import get_jsonize


def problem(title=None, status=500, headers=None, **kwds):
    kwds.update(title=title, status=status)
    body = json.dumps(kwds)
    return func.HttpResponse(body, status_code=status, headers=headers)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    category = req.params.get("category")
    classification = req.params.get("classification")
    if not category:
        return problem(
            title="bad request", detail="Required query parameter: category", status=400
        )

    if not classification:
        return problem(
            title="bad request",
            detail="Required query parameter: classification",
            status=400,
        )

    ret = get_jsonize(category, classification)
    logging.debug("%r", ret)

    url = f"https://w3id.org/italia/controlled-vocabulary/{category}/{classification}"
    rel = "via"
    title = "Core vocabulary definition"
    headers = {"Link": f'<{url}>; rel={rel}; title="{title}"'}
    return func.HttpResponse(json.dumps(ret), headers=headers)
