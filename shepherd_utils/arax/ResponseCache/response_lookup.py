# Ported from RTXteam/RTX @ 9485431, code/ARAX/ResponseCache/response_cache.py:
# ResponseCache.get_response() (API-07) and store_callback() (API-08).
# Changes from upstream (DEC-3 in docs/ARAX_PORT_BASELINE.md):
#   - responses are read from Shepherd's storage, not MySQL + S3 + local files.
#     ARAX's local ids are integers; Shepherd's are its response ids, so any id
#     that is not URL-like or ARS-like (len > 30) is looked up there
#   - a short id with the UI's X prefix (the UI adds it to every non-numeric id)
#     is looked up without it: Shepherd's response ids are hex, not ARAX's
#     integers, so the UI's load-by-id, ?r= links and history would otherwise
#     miss every Shepherd response (DEC-19)
#   - ARS PKs/UUIDs are read from Shepherd's own hosted ARS (/ars), not from
#     ars-prod / ars.test / ars.ci / ars-dev: the fetchers are injected, and
#     ars_host is Shepherd's host
#   - the component cache (json_cache/, used by the X and Z prefixes) is kept in
#     Shepherd's data store with a TTL instead of a directory cleared at
#     startup, so every server process sees it
#   - the control flow is async so the fetches run on the server's event loop;
#     the validation / stripping steps (CPU-bound) are sync functions run
#     through an injected runner (the server runs them in a process pool)
#   - actor_name_lookup also names the agents Shepherd's ARS records on its
#     ARAs' child messages (ara-shepherd-aragorn / -arax / -bte)
#   - store_callback keeps the posted body in Shepherd's data store (capped at
#     5000 entries, as upstream caps its files) instead of data/callbacks/
# Everything else -- the branch order, the validator calls and result shapes,
# the error tuples, the actor lookups, the attribute stripping, the size
# strings -- is upstream's, including its quirks (DEC-1): the URL branch refers
# to validation_messages_text before assigning it, so its validation always
# ends in the "validator crashed" FAIL result.
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
import json
import re
import uuid
from datetime import datetime
from importlib import metadata

from shepherd_utils.arax.ARAX_attribute_parser import ARAXAttributeParser
from shepherd_utils.db import _get_sync_data_db, decode_message, encode_message


def eprint(*args, **kwargs):
    import sys
    print(*args, file=sys.stderr, **kwargs)


# only certain versions of TRAPI can be validated; place default in position [0]
valid_trapi_versions = ['1.6.0', '1.5.0']
biolink_version = '4.4.2'

try:
    validator_version = f"{metadata.version('reasoner-validator')}"
except metadata.PackageNotFoundError:
    validator_version = ""

# upstream's json_cache/ directory lives until the next server start
COMPONENT_CACHE_TTL_SEC = 7 * 24 * 3600
CALLBACKS_KEY = "arax_callbacks"
MAX_CALLBACKS = 5000


def component_cache_key(component_id: str) -> str:
    return f"arax_json_cache:{component_id}"


def component_cache_put(component_id: str, obj) -> None:
    _get_sync_data_db().set(
        component_cache_key(component_id), encode_message(obj), ex=COMPONENT_CACHE_TTL_SEC
    )


def component_cache_get(component_id: str):
    blob = _get_sync_data_db().get(component_cache_key(component_id))
    return None if blob is None else decode_message(blob)


def _TRAPIResponseValidator(**kwargs):
    from reasoner_validator.validator import TRAPIResponseValidator
    return TRAPIResponseValidator(**kwargs)


def _size_string(content_size):
    if content_size < 1000:
        content_size = '{:.2f} kB'.format(content_size/1000)
    elif content_size < 1000000:
        content_size = '{:.0f} kB'.format(content_size/1000)
    elif content_size < 10000000000:
        content_size = '{:.1f} MB'.format(content_size/1000000)
    else:
        content_size = '{:.0f} MB'.format(content_size/1000000)
    return content_size


def not_found(response_id):
    return( { "status": 404, "title": "Response not found", "detail": "There is no response corresponding to response_id="+str(response_id), "type": "about:blank" }, 404)


##################################################################################################
#### Local response: validation + provenance (upstream's integer-id branch)
def finish_local_response(envelope):

    #### Perform a validation on it
    enable_validation = True
    schema_version = valid_trapi_versions[0]
    if 'schema_version' in envelope and envelope['schema_version'] in valid_trapi_versions:
        schema_version = envelope['schema_version']
    if enable_validation:
        #if True:
        try:

            #### Perform the validation
            eprint(f"Validating TRAPI with version {schema_version} and {biolink_version}")
            validator = _TRAPIResponseValidator(trapi_version=schema_version, biolink_version=biolink_version)
            validator.check_compliance_of_trapi_response(envelope)
            validation_messages_text = validator.dumps()
            validation_messages_text = validation_messages_text[:120] + '...truncated'
            raw_messages: Dict[str, List[Dict[str,str]]] = validator.get_all_messages()
            messages = raw_messages['Validate TRAPI Response']['Standards Test']
            #eprint(json.dumps(messages, indent=2, sort_keys=True))

            envelope['validation_result'] = { 'status': '?', 'version': schema_version, 'message': 'Internal error', 'validation_messages': messages, 'validation_messages_text': validation_messages_text, 'validator_version': validator_version }
            critical_errors = 0
            errors = 0
            if 'critical' in messages and len(messages['critical']) > 0:
                critical_errors = len(messages['critical'])
            if 'error' in messages and len(messages['error']) > 0:
                errors = len(messages['error'])
            if critical_errors > 0:
                envelope['validation_result']['status'] = 'FAIL'
                envelope['validation_result']['message'] = 'There were critical validator errors'
            elif errors > 0:
                envelope['validation_result']['status'] = 'ERROR'
                envelope['validation_result']['message'] = 'There were validator errors'
            else:
                envelope['validation_result']['status'] = 'PASS'
                envelope['validation_result']['message'] = ''

        #else:
        except Exception as error:
            timestamp = str(datetime.now().isoformat())
            if 'logs' not in envelope or envelope['logs'] is None:
                envelope['logs'] = []
            envelope['logs'].append( { "code": 'ValidatorFailed', "level": "ERROR", "message": "TRAPI validator crashed with error: " + str(error),
                "timestamp": timestamp } )
            if 'description' not in envelope or envelope['description'] is None:
                envelope['description'] = ''
            envelope['validation_result'] = { 'status': 'FAIL', 'version': schema_version, 'message': 'TRAPI validator crashed with error: ' + str(error) + ' --- ' + envelope['description'] }

    else:
        envelope['validation_result'] = { 'status': 'DISABLED', 'version': schema_version, 'message': 'Validation disabled.', 'validation_messages': { "critical": {}, "error": {}, "warning": {}, "info": { "message": 'Validation has been temporarily disabled due to various problems running it. It may return if the problems can be resolved.' } } }


    #### Count provenance information
    attribute_parser = ARAXAttributeParser(envelope,envelope['message'])
    envelope['validation_result']['provenance_summary'] = attribute_parser.summarize_provenance_info()

    return envelope


def load_and_finish_local_response(response_id):
    """Read a stored response from Shepherd's data store and finish it (runs in
    a pool child, so only the id crosses into it)."""
    from shepherd_utils.db import get_message_sync
    try:
        envelope = get_message_sync(response_id)
    except KeyError:
        return not_found(response_id)
    if not isinstance(envelope, dict) or 'message' not in envelope:
        return not_found(response_id)
    return finish_local_response(envelope)


##################################################################################################
#### A response fetched by URL (upstream's CQ / $$ / http branch)
def finish_url_response(response_id, url, status_code, content, debug=True):

    if status_code != 200:
        if debug:
            eprint("Cannot fetch url "+str(url))
            eprint(str(content))
        return( { "status": 404, "title": "Response not found", "detail": "Cannot fetch from ARS a response corresponding to response_id="+str(response_id), "type": "about:blank" }, 404)

    #if True:
    try:
        envelope = json.loads(content)
    #else:
    except:
        eprint(f"ERROR: Unable to convert {url} to JSON")
        return( { "status": 404, "title": "Response not found", "detail": "There is no response corresponding to response_id="+str(response_id), "type": "about:blank" }, 404)


    #### Perform a validation on it
    enable_validation = True
    schema_version = valid_trapi_versions[0]
    if 'schema_version' in envelope and envelope['schema_version'] in valid_trapi_versions:
        schema_version = envelope['schema_version']
    try:
        if enable_validation:

            validator = _TRAPIResponseValidator(trapi_version=schema_version, biolink_version=biolink_version)
            validator.check_compliance_of_trapi_response(envelope)
            raw_messages: Dict[str, List[Dict[str,str]]] = validator.get_all_messages()
            messages = raw_messages['Validate TRAPI Response']['Standards Test']

            critical_errors = 0
            errors = 0
            if 'critical' in messages and len(messages['critical']) > 0:
                critical_errors = len(messages['critical'])
            if 'error' in messages and len(messages['error']) > 0:
               errors = len(messages['error'])
            if critical_errors > 0:
                envelope['validation_result'] = { 'status': 'FAIL', 'version': schema_version, 'message': 'There were critical validator errors', 'validation_messages': messages, 'validation_messages_text': validation_messages_text }
            elif errors > 0:
                envelope['validation_result'] = { 'status': 'ERROR', 'version': schema_version, 'message': 'There were validator errors', 'validation_messages': messages, 'validation_messages_text': validation_messages_text }
            else:
                envelope['validation_result'] = { 'status': 'PASS', 'version': schema_version, 'message': '', 'validation_messages': messages, 'validation_messages_text': validation_messages_text }

        else:
            envelope['validation_result'] = { 'status': 'PASS', 'version': schema_version, 'message': 'Validation disabled. too many dependency failures', 'validation_messages': { "errors": [], "warnings": [], "information": [ 'Validation has been temporarily disabled due to problems with dependencies. Will return again soon.' ] } }
    except Exception as error:
        timestamp = str(datetime.now().isoformat())
        if 'logs' not in envelope or envelope['logs'] is None:
            envelope['logs'] = []
        envelope['logs'].append( { "code": 'ValidatorFailed', "level": "ERROR", "message": "TRAPI validator crashed with error: " + str(error),
            "timestamp": timestamp } )
        if 'description' not in envelope or envelope['description'] is None:
            envelope['description'] = ''
        envelope['validation_result'] = { 'status': 'FAIL', 'version': schema_version, 'message': 'TRAPI validator crashed with error: ' + str(error) + ' --- ' + envelope['description'] }

    #### Count provenance information
    attribute_parser = ARAXAttributeParser(envelope,envelope['message'])
    envelope['validation_result']['provenance_summary'] = attribute_parser.summarize_provenance_info()

    return envelope
    # Never runs. Upstream's get_response assigns validation_messages_text in
    # another branch of the same function, so this branch's use of it raises
    # UnboundLocalError ("cannot access local variable ..."), not NameError;
    # the assignment keeps the message the validator-crash result carries.
    validation_messages_text = None


##################################################################################################
#### An ordinary child ARS message (upstream's ARS branch, after the fetch)
def finish_ars_child_response(response_id, original_response_id, attribute_caching, response_dict, content_length):

    content_size = _size_string(content_length)
    envelope = response_dict['fields']['data']
    if envelope is None:
        envelope = {}
        return envelope
    actual_response = str(envelope)
    if not isinstance(envelope,dict):
        envelope = { 'detail': envelope }

    #### Actor lookup
    actor_lookup = {
        '1': 'Aragorn',
        '2': 'ARAX',
        '3': 'BTE',
        '4': 'NCATS',
        '5': 'Robokop',
        '6': 'Unsecret',
        '7': 'Genetics',
        '8': 'MolePro',
        '10': 'Explanatory',
        '11': 'ImProving',
        '12': 'Cam',
        '13': 'TextMining'
    }

    #### Actor lookup by name
    actor_name_lookup = {
        'ara-aragorn': 'Aragorn',
        'ara-arax': 'ARAX',
        'ara-bte': 'BTE',
        'ara-ncats': 'NCATS',
        'ara-robokop': 'Robokop',
        'ara-unsecret': 'Unsecret',
        'kp-genetics': 'Genetics',
        'kp-molecular': 'MolePro',
        'ara-explanatory': 'Explanatory',
        'ara-improving': 'ImProving',
        'kp-cam': 'Cam',
        'kp-chp': 'CHP',
        'kp-icees': 'ICEES',
        'kp-openpredict': 'OpenPredict',
        'kp-textmining': 'TextMining',
        # Shepherd's ARS names its ARAs' child messages ara-shepherd-<name>
        'ara-shepherd-aragorn': 'Aragorn',
        'ara-shepherd-arax': 'ARAX',
        'ara-shepherd-bte': 'BTE',
    }

    is_trapi = True
    if 'message' in envelope:
        if 'logs' in envelope and isinstance(envelope['logs'],list) and len(envelope['logs']) > 0:
            if isinstance(envelope['logs'][0], str):
               is_trapi = False
               actual_response = envelope['logs'][0]
               for i in range(len(envelope['logs'])):
                   if isinstance(envelope['logs'][i],str):
                       envelope['logs'][i] = { 'level': 'INFO', 'message': 'ARS info: ' + envelope['logs'][i] }

               try:
                   import html
                   actual_response = html.unescape(actual_response)
                   actual_response_dict = json.loads(actual_response)
                   if 'message' in actual_response_dict:
                       is_trapi = True
                       envelope = actual_response_dict
               except:
                   eprint("WARNING: tried to convert the response to JSON and it did not work")
                   eprint(f"It was: {envelope['logs'][0]}")

    else:
        is_trapi = False

    if not is_trapi:
        envelope['validation_result'] = { 'status': 'NA', 'version': valid_trapi_versions[0], 'size': content_size, 'message': 'Returned response is not TRAPI: ' + actual_response }
        return envelope


    #### Perform a validation on it
    enable_validation = True
    schema_version = valid_trapi_versions[0]
    if 'schema_version' in envelope and envelope['schema_version'] in valid_trapi_versions:
        schema_version = envelope['schema_version']
    try:
        if enable_validation:

            #### Set up the validator
            validator = _TRAPIResponseValidator(trapi_version=schema_version, biolink_version=biolink_version)

            eprint(f"Validating response with trapi_version={schema_version}, biolink_version={biolink_version}")
            validator.check_compliance_of_trapi_response(envelope)

            raw_messages: Dict[str, List[Dict[str,str]]] = validator.get_all_messages()
            messages = raw_messages['Validate TRAPI Response']['Standards Test']
            validation_messages_text = validator.dumps()
            validation_messages_text = validation_messages_text[:120] + '...truncated'

            envelope['validation_result'] = { 'status': '?', 'version': schema_version, 'size': content_size, 'message': 'Internal error', 'validation_messages': messages, 'validation_messages_text': validation_messages_text, 'validator_version': validator_version }
            critical_errors = 0
            errors = 0
            if 'critical' in messages and len(messages['critical']) > 0:
                critical_errors = len(messages['critical'])
            if 'error' in messages and len(messages['error']) > 0:
                errors = len(messages['error'])
            if critical_errors > 0:
                envelope['validation_result']['status'] = 'FAIL'
                envelope['validation_result']['message'] = 'There were critical validator errors'
            elif errors > 0:
                envelope['validation_result']['status'] = 'ERROR'
                envelope['validation_result']['message'] = 'There were validator errors'
            else:
                envelope['validation_result']['status'] = 'PASS'
                envelope['validation_result']['message'] = ''

        else:
            envelope['validation_result'] = { 'status': 'DISABLED', 'version': schema_version, 'message': 'Validation disabled.', 'validation_messages': { "critical": {}, "error": {}, "warning": {}, "info": { "message": 'Validation has been temporarily disabled due to various problems running it. It may return if the problems can be resolved.' } } }

    except Exception as error:
        timestamp = str(datetime.now().isoformat())
        if 'logs' not in envelope or envelope['logs'] is None:
            envelope['logs'] = []
        envelope['logs'].append( { "code": 'ValidatorFailed', "level": "ERROR", "message": "TRAPI validator crashed with error: " + str(error),
            "timestamp": timestamp } )
        if 'description' not in envelope or envelope['description'] is None:
            envelope['description'] = ''
        envelope['validation_result'] = { 'status': 'FAIL', 'version': schema_version, 'size': content_size, 'message': 'TRAPI validator crashed with error: ' + str(error) + ' --- ' + envelope['description'] }

    #### Try to add the resource_id
    if 'name' in response_dict['fields'] and response_dict['fields']['name'] is not None:
        actor = str(response_dict['fields']['name'])
        if actor in actor_name_lookup:
            if 'message' in envelope and 'results' in envelope['message'] and envelope['message']['results'] is not None:
                for result in envelope['message']['results']:
                    if 'resource_id' in result and result['resource_id'] is not None:
                        pass
                    else:
                        result['resource_id'] = actor_name_lookup[actor]

    elif 'actor' in response_dict['fields'] and response_dict['fields']['actor'] is not None:
        actor = str(response_dict['fields']['actor'])
        if actor in actor_lookup:
            if 'message' in envelope and 'results' in envelope['message'] and envelope['message']['results'] is not None:
                for result in envelope['message']['results']:
                    if 'resource_id' in result and result['resource_id'] is not None:
                        pass
                    else:
                        result['resource_id'] = actor_lookup[actor]

    if 'message' in envelope and 'knowledge_graph' in envelope['message'] and envelope['message']['knowledge_graph'] is not None:
        n_nodes = None
        if 'nodes' in envelope['message']['knowledge_graph'] and envelope['message']['knowledge_graph']['nodes'] is not None:
            n_nodes = len(envelope['message']['knowledge_graph']['nodes'])
        n_edges = None
        if 'edges' in envelope['message']['knowledge_graph'] and envelope['message']['knowledge_graph']['edges'] is not None:
            n_edges = len(envelope['message']['knowledge_graph']['edges'])
        envelope['validation_result']['n_nodes'] = n_nodes
        envelope['validation_result']['n_edges'] = n_edges

        #### Count provenance information
        attribute_parser = ARAXAttributeParser(envelope,envelope['message'])
        envelope['validation_result']['provenance_summary'] = attribute_parser.summarize_provenance_info()

        #### Strip highly verbose information
        if attribute_caching is True and 'nodes' in envelope['message']['knowledge_graph'] and envelope['message']['knowledge_graph']['nodes'] is not None:
            for node_key, node in envelope['message']['knowledge_graph']['nodes'].items():
                component_uuid = 'Z' + str(uuid.uuid4())
                component_cache_put(component_uuid, node)
                node['attributes'] = None
                node['detail_lookup'] = component_uuid
        eprint(f"attribute_caching={attribute_caching}")
        if attribute_caching is True and 'edges' in envelope['message']['knowledge_graph'] and envelope['message']['knowledge_graph']['edges'] is not None:
            for edge_key, edge in envelope['message']['knowledge_graph']['edges'].items():
                if 'attributes' in edge and edge['attributes'] is not None:
                    for attribute in edge['attributes']:
                        if 'attribute_type_id' in attribute and attribute['attribute_type_id'] is not None and attribute['attribute_type_id'] == 'biolink:support_graphs':
                            edge['has_these_support_graphs'] = attribute['value']
                component_uuid = 'Z' + str(uuid.uuid4())
                component_cache_put(component_uuid, edge)
                edge['detail_lookup'] = component_uuid
                edge['attributes'] = None
                edge['sources'] = None

        content_size = len(json.dumps(envelope,indent=2))
        envelope['validation_result']['size'] = _size_string(content_size)
        component_cache_put(original_response_id, envelope)

    return envelope


##################################################################################################
#### Fetch a stored response (upstream's get_response control flow)
async def get_response(response_id, *, fetch_url, fetch_ars, ars_host, run):
    """
    fetch_url(url) -> (status_code, content bytes); raises on a connection error
    fetch_ars(pk, trace) -> (status_code, content bytes) from Shepherd's ARS
    run(fn, *args) -> awaitable running a sync step off the event loop
    """

    if response_id is None:
        return( { "status": 400, "title": "response_id missing", "detail": "Required attribute response_id is missing from URL", "type": "about:blank" }, 400)

    response_id = str(response_id)

    #### Otherwise, see if it is a URL
    if response_id.startswith('CQ') or response_id.startswith('$$') or response_id.startswith('http'):
        debug = True
        url = 'xx'

        if response_id.startswith('http'):
            url = response_id.replace('$', '/')

        if response_id.startswith('$$'):
            url = 'https:' + response_id.replace('$', '/')

        if response_id.startswith('CQ'):
            url = f"https://peptideatlas.org/tmp/{response_id}"

        if debug:
            eprint(f"Trying {url}...")
        try:
            status_code, content = await fetch_url(url)
        except Exception as e:
            return( { "status": 404, "title": f"Remote URL {url} unavailable", "detail": f"Connection attempts to {url} triggered an exception: {e}", "type": "about:blank" }, 404)
        if debug:
            eprint(f"--- Fetch of {url} yielded {status_code}")
        return await run(finish_url_response, response_id, url, status_code, content)

    #### Otherwise, see if it is an ARS style response_id
    if len(response_id) > 30:
        debug = False

        #### See if this thing is cached already
        cached = await run(component_cache_get, response_id)
        if cached is not None:
            return cached

        #### If it started with Z, this is a special temporary cache, and if it's not there, all is lost
        if response_id.startswith('Z'):
            return( { "status": 404, "title": f"Cached component not found", "detail": f"The component cache has been cleared since the initial request. Refresh the entire response", "type": "about:blank" }, 404)

        #### If the UUID starts with X, then enable attribute stripping mode and attribute caching, which makes the GUI faster
        attribute_caching = False
        original_response_id = response_id
        if response_id.startswith('X'):
            attribute_caching = True
            response_id = response_id[1:]

        try:
            status_code, content = await fetch_ars(response_id, False)
        except Exception as e:
            return( { "status": 404, "title": f"Remote host {ars_host} unavailable", "detail": f"Connection attempts to {ars_host} triggered an exception: {e}", "type": "about:blank" }, 404)

        if status_code != 200:
            return( { "status": 404, "title": "Response not found", "detail": "Cannot fetch from ARS a response corresponding to response_id="+str(response_id), "type": "about:blank" }, 404)

        #### Unpack the response content into a dict
        try:
            response_dict = json.loads(content)
        except:
            return( { "status": 404, "title": "Error decoding Response", "detail": "Cannot decode ARS response_id="+str(response_id)+" to a Translator Response", "type": "about:blank" }, 404)

        is_parent_pk = False
        if 'fields' in response_dict:
            if 'name' in response_dict['fields'] and response_dict['fields']['name'] != '':
                if response_dict['fields']['name'] == 'ars-default-agent' or response_dict['fields']['name'] == 'ars-workflow-agent':
                    is_parent_pk = True
                else:
                    is_parent_pk = False
            elif 'actor' in response_dict['fields'] and ( str(response_dict['fields']['actor']) == '9' or str(response_dict['fields']['actor']) == '19' ):
                is_parent_pk = True
        if is_parent_pk == True:
            status_code, content = await fetch_ars(response_id, True)

            if status_code != 200:
                return( { "status": 404, "title": "Response not found", "detail": "Failed attempting to fetch trace=y from ARS with response_id="+str(response_id), "type": "about:blank" }, 404)

            #### Unpack the response content into a dict and dump
            try:
                response_dict = json.loads(content)
            except:
                return( { "status": 404, "title": "Error decoding Response", "detail": "Cannot decode ARS response_id="+str(response_id)+" to a Translator Response", "type": "about:blank" }, 404)

            response_dict['ars_host'] = ars_host
            if "-dev" in ars_host:
                response_dict['ui_host'] = 'transltr-bma-ui-dev.ncats.io'
            else:
                response_dict['ui_host'] = ars_host.replace('ars','ui').replace('-prod','')

            return response_dict

        if not is_parent_pk and 'fields' in response_dict and 'data' in response_dict['fields']:
            return await run(finish_ars_child_response, response_id, original_response_id, attribute_caching, response_dict, len(content))
        return( { "status": 404, "title": "Cannot find Response (in 'fields' and 'data') in ARS response packet", "detail": "Cannot decode ARS response_id="+str(response_id)+" to a Translator Response", "type": "about:blank" }, 404)

    #### Otherwise it is a Shepherd response id (ARAX's integer local ids)
    #### The UI prefixes every id that is not a number with X (isNaN(id) ? "X"+id : id),
    #### meant for ARS PKs; Shepherd's ids are hex (no X), so a short X... id is
    #### the UI's X on one of them (DEC-19)
    if response_id.startswith('X'):
        response_id = response_id[1:]
    return await run(load_and_finish_local_response, response_id)


##################################################################################################
#### Store a received callback content
def store_callback(body):
    client = _get_sync_data_db()
    if client.llen(CALLBACKS_KEY) >= MAX_CALLBACKS:
        eprint(f"ERROR: store_callback counter has reach 5000. Time to clean up or there is a runaway")
        return
    client.rpush(CALLBACKS_KEY, encode_message(body))
    eprint(f"INFO: Received a response and stored it in {CALLBACKS_KEY}")
