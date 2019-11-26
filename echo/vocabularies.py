import collections
import http.client as http_client
import json
import logging
import string
from collections import defaultdict
from os.path import basename
from pathlib import Path

import yaml

from pyld import jsonld

log = logging.getLogger()
# logging.basicConfig(level=logging.DEBUG)

url = "https://ontopia-lodview.prod.pdnd.italia.it/onto/CPV/Person"

nulla = lambda *a, **kw: None

http_client.HTTPConnection.debuglevel = 1

# You must initialize logging, otherwise you'll not see debug output.
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True


def session_document_loader(secure=False, **kwargs):
    """
    Create a Requests document loader.

    Can be used to setup extra Requests args such as verify, cert, timeout,
    or others.

    :param secure: require all requests to use HTTPS (default: False).
    :param **kwargs: extra keyword args for Requests get() call.

    :return: the RemoteDocument loader function.
    """
    from pyld.jsonld import (
        JsonLdError,
        urllib_parse,
        parse_link_header,
        LINK_HEADER_REL,
    )

    import requests

    if True:
        import requests_cache

        requests_cache.install_cache("demo_cache")
        session = requests_cache.CachedSession()

    if False:
        from requests_futures import sessions

        session = sessions.FuturesSession()

    if False:
        # enable http/2
        from hyper.contrib import HTTP20Adapter

        adapter = HTTP20Adapter()
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        session.mount("https://", adapter)
        # session.mount('https://w3id.org', adapter)
        # session.mount('https://ontopia-lodview.pdnd.italia.it', adapter)

    def loader(url):
        """
        Retrieves JSON-LD at the given URL.

        :param url: the URL to retrieve.

        :return: the RemoteDocument.
        """
        try:
            # validate URL
            pieces = urllib_parse.urlparse(url)
            if (
                not all([pieces.scheme, pieces.netloc])
                or pieces.scheme not in ["http", "https"]
                or set(pieces.netloc)
                > set(string.ascii_letters + string.digits + "-.:")
            ):
                raise JsonLdError(
                    'URL could not be dereferenced; only "http" and "https" '
                    "URLs are supported.",
                    "jsonld.InvalidUrl",
                    {"url": url},
                    code="loading document failed",
                )
            if secure and pieces.scheme != "https":
                raise JsonLdError(
                    "URL could not be dereferenced; secure mode enabled and "
                    'the URL\'s scheme is not "https".',
                    "jsonld.InvalidUrl",
                    {"url": url},
                    code="loading document failed",
                )
            headers = {"Accept": "application/ld+json, application/json"}
            response = session.get(url, headers=headers, **kwargs)

            doc = {
                "contextUrl": None,
                "documentUrl": response.url,
                "document": response.json(),
            }
            content_type = response.headers.get("content-type")
            link_header = response.headers.get("link")
            if link_header and content_type != "application/ld+json":
                link_header = parse_link_header(link_header).get(LINK_HEADER_REL)
                # only 1 related link header permitted
                if isinstance(link_header, list):
                    raise JsonLdError(
                        "URL could not be dereferenced, it has more than one "
                        "associated HTTP Link Header.",
                        "jsonld.LoadDocumentError",
                        {"url": url},
                        code="multiple context link headers",
                    )
                if link_header:
                    doc["contextUrl"] = link_header["target"]
            return doc
        except JsonLdError as e:
            raise e
        except Exception as cause:
            raise JsonLdError(
                "Could not retrieve a JSON-LD document from the URL.",
                "jsonld.LoadDocumentError",
                code="loading document failed",
                cause=cause,
            )

    return loader


jsonld.set_document_loader(session_document_loader())
# jsonld.set_document_loader(jsonld.aiohttp_document_loader())


def dict_merge(dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.items():
        if isinstance(dct.get(k), dict) and isinstance(
            merge_dct[k], collections.Mapping
        ):
            dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]


def jsonld_expand(url):
    url = url.replace("w3id.org/italia", "ontopia-lodview.pdnd.italia.it")
    return jsonld.expand(url)


def _get_id(x, label):
    ret = x.get(label)[0]["@id"]
    if "italia" in ret:
        components[basename(ret)] = ret
    return ret


def parse_vc(item=None, vc_url=None, **kwargs):
    ret = {}

    if item and not vc_url:
        if "https://w3id.org/italia/onto/l0/controlledVocabulary" in item:
            vc_url = _get_id(
                item, "https://w3id.org/italia/onto/l0/controlledVocabulary"
            )

    if not vc_url:
        return ret

    vc = jsonld_expand(vc_url)
    labels = vc[0]["http://www.w3.org/2004/02/skos/core#hasTopConcept"]

    for item in labels:
        dict_merge(ret, parse_vc_label(item, "it", **kwargs))

    ret["vc"] = vc_url

    return ret


def filter_vc_keys(item):

    standard_keys = [
        "@id",
        "@type",
        "https://w3id.org/italia/onto/CLV/hasRankOrder",
        "http://purl.org/dc/terms/identifier",
        "http://www.w3.org/2004/02/skos/core#inScheme",
        "http://www.w3.org/2004/02/skos/core#notation",
        "http://www.w3.org/2004/02/skos/core#definition",
    ]

    _id = basename(item["@id"])
    ret = {}
    for k, v in item.items():
        if k in standard_keys:
            continue
        for row in v:
            row_language = row.get("@language", "default")
            if k not in ret:
                ret[k] = {row_language: {_id: row}}
            elif row_language not in ret[k]:
                ret[k][row_language] = {_id: row}
            else:
                ret[k][row_language][_id] = row

    return ret


def parse_vc_label(item, backref=None):

    url = item["@id"]
    schema = jsonld_expand(url)[0]

    if backref not in schema:
        backref = "http://www.w3.org/2004/02/skos/core#prefLabel"

    ret = {}
    vc_columns = filter_vc_keys(schema)
    ret["columns"] = vc_columns

    if ret:
        return ret
    return {}


def test_vc_l_1():
    item = {
        "@id": "https://w3id.org/italia/controlled-vocabulary/classifications-for-people/person-title/1"
    }
    ret = parse_vc_label(item)
    assert (
        ret["columns"]["http://www.w3.org/2004/02/skos/core#prefLabel"]["en"]["1"][
            "@value"
        ]
        == "Mrs"
    )


def test_vc_1():
    ret = parse_vc(
        vc_url="https://w3id.org/italia/controlled-vocabulary/classifications-for-people/person-title"
    )
    print(json.dumps(ret))


def get_jsonize(category, classification):
    base_url = Path("w3id.org/italia/controlled-vocabulary/")
    url = base_url / category / classification

    log.warning("Querying base url: %r", base_url)
    ret = parse_vc(vc_url="https://" + str(url))
    return ret


def get_status():
    return {"title": "ok"}
