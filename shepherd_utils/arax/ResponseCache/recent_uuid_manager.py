# Ported from RTXteam/RTX @ 9485431, code/ARAX/ResponseCache/recent_uuid_manager.py.
# Changes from upstream (DEC-3 in docs/ARAX_PORT_BASELINE.md):
#   - the ARS is Shepherd's own hosted ARS, read in-process through injected
#     async fetchers (latest_pk, messages, messages?trace=y) instead of
#     requests.get to https://{ars_host}/ars/api/...; ars_host is Shepherd's host
#     whatever the caller names (the UI passes an ARS host as `authorization`)
#   - get_recent_uuids / get_uuid are async so the fetches run on the server's
#     event loop; summarize_uuid_data (which calls NodeNorm through the
#     NodeSynonymizer) is unchanged and runs through an injected runner
#   - main() is dropped
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
import sys
def eprint(*args, **kwargs): print(*args, file=sys.stderr, **kwargs)
import json
import copy

from shepherd_utils.arax.NodeSynonymizer.node_synonymizer import NodeSynonymizer


class RecentUUIDManager:

    def __init__(self, fetch_latest_pks, fetch_ars, run):
        """
        fetch_latest_pks(n) -> (status_code, content bytes) of /ars/api/latest_pk/{n}
        fetch_ars(pk, trace) -> (status_code, content bytes) of /ars/api/messages/{pk}[?trace=y]
        run(fn, *args) -> awaitable running a sync step off the event loop
        """
        self.synonymizer = None
        self.fetch_latest_pks = fetch_latest_pks
        self.fetch_ars = fetch_ars
        self.run = run


    async def get_recent_uuids(self, ars_host='ars.ci.transltr.io', top_n_pks=20):

        debug = False
        #top_n_pks = 20
        response = { 'agents_list': [], 'pks': {} }

        ars_hosts = [ ars_host ]
        for ars_host in ars_hosts:
            if debug:
                eprint(f"Trying {ars_host}...")
            try:
                status_code, content = await self.fetch_latest_pks(top_n_pks)
            except Exception as e:
                return( { "status": 404, "title": f"Remote host {ars_host} unavailable", "detail": f"Connection attempts to {ars_host} triggered an exception: {e}", "type": "about:blank" }, 404)
            if debug:
                eprint(f"--- Fetch of UUIDs from {ars_host} yielded {status_code}")
            if status_code == 200:
                if debug:
                    eprint(f"Got 200 from {ars_host}...")
                break

        if status_code != 200:
            error_message = f"Cannot fetch recent pks from any ARS hosts"
            if debug:
                eprint(error_message)
                eprint(str(content))
            return( { "status": 404, "title": "Response not found", "detail": error_message, "type": "about:blank" }, 404)


        #### Unpack the response content into a dict
        try:
            response_dict = json.loads(content)
        except:
            return( { "status": 404, "title": "Error decoding Response", "detail": f"Cannot decode recent PK list from ARS {ars_host}", "type": "about:blank" }, 404)

        #### Debugging
        if debug:
            temp = copy.deepcopy(response_dict)
            eprint(json.dumps(temp,indent=2,sort_keys=True))

        container_key = f"latest_{top_n_pks}_pks"
        if container_key not in response_dict:
            return( { "status": 404, "title": "Error decoding Response", "detail": f"Cannot decode recent PK list from ARS {ars_host}: cannot find {container_key}", "type": "about:blank" }, 404)

        have_timestamps = True
        uuid_list = []
        for uuid in response_dict[container_key]:
            uuid_data = await self.get_uuid(ars_host, uuid)
            result = await self.run(self.summarize_uuid_data, ars_host, uuid_data)
            if 'timestamp' not in result:
                have_timestamps = False
            else:
                uuid_list.append( { 'pk': uuid, 'timestamp': result['timestamp'] } )
            response['pks'][uuid] = result
            response['agents_list'] = result['agents_list']
            del(result['agents_list'])
            response['pks'][uuid]['ars_host'] = ars_host

        if have_timestamps:
            uuid_list.sort(key=lambda x: x['timestamp'])
            response['sorted_pk_list'] = []
            for item in uuid_list:
                response['sorted_pk_list'].append(item['pk'])

        return response


    ###################################################################
    async def get_uuid(self, ars_host, uuid):

        debug = False

        if debug:
            eprint(f"Trying to fetch {uuid} from {ars_host}...")
        try:
            status_code, content = await self.fetch_ars(uuid, False)
        except Exception as e:
            return( { "status": 404, "title": f"Remote host {ars_host} unavailable", "detail": f"Connection attempts to {ars_host} triggered an exception: {e}", "type": "about:blank" }, 404)

        if debug:
            eprint(f"--- Fetch of {uuid} from {ars_host} yielded {status_code}")

        if status_code != 200:
            if debug:
                eprint("Cannot fetch from ARS the UUID {uuid}")
                eprint(str(content))
            return( { "status": 404, "title": "Response not found", "detail": f"Cannot fetch from ARS a UUID {uuid}", "type": "about:blank" }, 404)


        #### Unpack the response content into a dict
        try:
            response_dict = json.loads(content)
        except:
            return( { "status": 404, "title": "Error decoding Response", "detail": f"Cannot decode UUID {uuid} data from {ars_host}", "type": "about:blank" }, 404)


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
            if debug:
                eprint(f"INFO: This is a parent UUID. Fetching trace=y for {uuid}")
            status_code, content = await self.fetch_ars(uuid, True)

            if status_code != 200:
                return( { "status": 404, "title": "Response not found", "detail": "Failed attempting to fetch trace=y from ARS with UUID {uuid}", "type": "about:blank" }, 404)

            #### Unpack the response content into a dict and dump
            try:
                response_dict = json.loads(content)
            except:
                return( { "status": 404, "title": "Error decoding Response", "detail": f"Cannot decode UUID {uuid} data from {ars_host}", "type": "about:blank" }, 404)

            return response_dict

        if not is_parent_pk and 'fields' in response_dict and 'data' in response_dict['fields']:
            envelope = response_dict['fields']['data']
            if debug:
                eprint(f"INFO: This is an ordinary child UUID. Don't know what to do with it...")
            return( { "status": 404, "title": "No Parent PK", "detail": "This is an ordinary child UUID. Don't know what to do with it.", "type": "about:blank" }, 404)


        return response_dict


    ###################################################################
    def summarize_uuid_data(self, ars_host, uuid_data):

        summary = { 'agents': {} }
        agents = {}

        if 'status' in uuid_data:
            summary['status'] = uuid_data['status']

        if 'timestamp' in uuid_data:
            summary['timestamp'] = uuid_data['timestamp']

        if 'children' in uuid_data:
            for actor_response in uuid_data['children']:
                code = '-'
                status = '-'
                result_count = 0
                agent = '?'
                if 'actor' in actor_response:
                    if 'agent' in actor_response['actor']:
                        agent = actor_response['actor']['agent']
                if not agent.startswith('ara'):
                    continue
                agent = agent.replace('ara-', '')
                agents[agent] = True
                if 'code' in actor_response:
                    code = actor_response['code']
                if 'status' in actor_response:
                    status = actor_response['status']
                if 'result_count' in actor_response:
                    result_count = actor_response['result_count']
                    if result_count is None:
                        result_count = 0
                code_str = ''
                if code != 200:
                    code_str = f"={code}"
                summary['agents'][agent] = { 'status': f"{status}{code_str}", 'n_results': result_count }

        if 'query_graph' in uuid_data:
            predicate ='?'
            object_id ='?'
            if 'edges' in uuid_data['query_graph']:
                n_edges = len(uuid_data['query_graph']['edges'])
                for edge_name,edge in uuid_data['query_graph']['edges'].items():
                    if 'predicates' in edge and edge['predicates'] is not None and len(edge['predicates']) > 0:
                        predicate = edge['predicates'][0]
            if 'nodes' in uuid_data['query_graph']:
                n_nodes = len(uuid_data['query_graph']['nodes'])
                for node_name,node in uuid_data['query_graph']['nodes'].items():
                    if 'ids' in node and node['ids'] is not None and len(node['ids']) > 0:
                        object_id = node['ids'][0]
            predicate = predicate.replace('biolink:', '')

            if self.synonymizer is None:
                self.synonymizer = NodeSynonymizer()
            results = self.synonymizer.get_normalizer_results(entities=object_id)
            try:
                name = results[object_id]['id']['name']
                if name is not None and len(name) > 1:
                    object_id = name
            except:
                eprint(f"ERROR: Unable to name name for {object_id} from synonymizer")

            summary['query'] = f"___ {predicate} {object_id}"

        summary['agents_list'] = sorted(list(agents.keys()))

        return summary
