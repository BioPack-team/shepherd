# Ported from RTXteam/RTX @ 9485431, code/ARAX/ARAXQuery/Overlay/add_node_pmids.py.
# Changes from upstream:
#   - import paths / sys.path hacks only
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
# This class will overlay the normalized google distance on a message (all edges)
#!/bin/env python3
import sys
import os
import traceback
import numpy as np

# relative imports
from shepherd_utils.arax.openapi_server.models.attribute import Attribute as NodeAttribute

from shepherd_utils.arax.reasoningtool.kg_construction.NormGoogleDistance import NormGoogleDistance as NGD

from shepherd_utils.arax.Overlay.compute_ngd import ComputeNGD


class AddNodePMIDS:

    #### Constructor
    def __init__(self, response, message, parameters):
        self.response = response
        self.message = message
        self.parameters = parameters
        self.ngd = ComputeNGD(response, message, parameters)

    def add_node_pmids(self):
        """
        Iterate over all the nodes in the knowledge graph, decorate with PMID's from pubmed abstracts
        :return: response
        """
        self.response.debug(f"Adding node PMIDs")
        self.response.info(f"Adding pubmed ID's to nodes based on occurrence in PubMed abstracts")
        name = "pubmed_ids"
        type = "EDAM-DATA:0971"
        value = ""
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
        ncbi_warning_flag = True

        # iterate over KG edges, add the information
        try:
            canonicalized_curie_lookup = self.ngd._get_canonical_curies_map(list(set(self.message.knowledge_graph.nodes.keys())))
            self.ngd.load_curie_to_pmids_data(canonicalized_curie_lookup.values())
            for key, node in self.message.knowledge_graph.nodes.items():
                # Make sure the attributes are not None
                if not node.attributes:
                    node.attributes = []  # should be an array, but why not a list?
                # now go and actually get the NGD
                node_curie = key
                node_name = node.name
                pmids = self.ngd.curie_to_pmids_map.get(node_curie)
                if pmids is None or len(pmids) < 1:
                    if ncbi_warning_flag:
                        self.response.warning(f"Utilizing API calls to NCBI eUtils, so this may take a while...")
                        ncbi_warning_flag = False
                    try:
                        pmids = NGD.get_pmids_for_all([node_curie], [node_name])[0]  # since the function was designed for multiple inputs, but I only want the first
                    except:
                        self.response.warning(f"There was an error retrieving the PMIDs for {node_curie} from NCBIeUtils.")
                        pmids = []
                else:
                    pmids = [f"PMID:{str(pmid)}" for pmid in pmids]
                if 'max_num' in self.parameters:
                    pmids = pmids[0:self.parameters['max_num']]
                value = pmids
                ngd_edge_attribute = NodeAttribute(attribute_type_id=type, original_attribute_name=name, value=value, value_url=url)  # populate the NGD edge attribute
                node.attributes.append(ngd_edge_attribute)  # append it to the list of attributes
        except:
            tb = traceback.format_exc()
            error_type, error, _ = sys.exc_info()
            self.response.error(tb, error_code=error_type.__name__)
            self.response.error(f"Something went wrong adding the PubMed ID attributes")
        else:
            self.response.info(f"PubMed ID's successfully added to nodes")

        return self.response
