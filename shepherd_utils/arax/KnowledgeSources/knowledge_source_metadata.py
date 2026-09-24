# Ported from RTXteam/RTX @ 9485431, code/ARAX/KnowledgeSources/knowledge_source_metadata.py.
# Changes from upstream:
#   - import paths / sys.path hacks only (including class-name strings)
#   - DEC-11: the base meta-KG is Retriever's /meta_knowledge_graph (at
#     settings.sync_kg_retrieval_url) instead of PloverDB's, and the KPInfoCacher
#     merge of every SmartAPI KP's meta map is removed (_merge_kp_info): every query
#     goes to Retriever (DEC-4). The rest -- the knowledge_types / attributes
#     fill-in, the standard attribute constraints, the simple format, the 1 h cache
#     and the backups -- is upstream's
#   - the JSON backups are kept in settings.arax_dbs_dir (Shepherd's writable ARAX data
#     volume) instead of next to this file
#   - get_kg_predicates (no callers; it read KG2C_allowed_predicate_triples.csv) and
#     main() are removed (dead code, DEC-5)
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
import sys
def eprint(*args, **kwargs): print(*args, file=sys.stderr, **kwargs)

import os
import json
import ast
import re
import inspect
import csv
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any



from shepherd_utils.arax.RTXConfiguration import RTXConfiguration
from shepherd_utils.config import settings


def _backup_dir() -> str:
    os.makedirs(settings.arax_dbs_dir, exist_ok=True)
    return settings.arax_dbs_dir


def _retriever_meta_kg_url() -> str:
    url = settings.sync_kg_retrieval_url.rstrip("/")
    base = url[: -len("/query")] if url.endswith("/query") else url
    return f"{base}/meta_knowledge_graph"


class KnowledgeSourceMetadata:

    #### Define a class variable to cache the meta_knowledge_graph between objects
    cached_meta_knowledge_graph = None
    cached_simplified_meta_knowledge_graph = None
    cache_timestamp = None
    cache_duration = timedelta(hours=1)  # Refresh every hour

    #### Constructor
    def __init__(self):
        self.predicates = None
        self.meta_knowledge_graph = KnowledgeSourceMetadata.cached_meta_knowledge_graph
        self.simplified_meta_knowledge_graph = KnowledgeSourceMetadata.cached_simplified_meta_knowledge_graph
        self.RTXConfig = RTXConfiguration()

    def _is_cache_valid(self) -> bool:
        """Check if the cached meta knowledge graph is still valid"""
        if (KnowledgeSourceMetadata.cached_meta_knowledge_graph is None or 
            KnowledgeSourceMetadata.cache_timestamp is None):
            return False
        
        return datetime.now() - KnowledgeSourceMetadata.cache_timestamp < self.cache_duration

    def _fetch_retriever_meta_kg(self) -> Optional[Dict[str, Any]]:
        """Fetch meta knowledge graph from Retriever"""
        try:
            response = requests.get(_retriever_meta_kg_url(), timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                eprint(f"ERROR: Retriever returned status {response.status_code}")
                return None
        except Exception as e:
            eprint(f"ERROR: Failed to fetch from Retriever: {e}")
            return None

    def _add_reasoner_api_info(self, meta_kg: Dict[str, Any]) -> Dict[str, Any]:
        """Add information from ReasonerAPI documentation"""
        # Only add missing TRAPI 1.4 fields, don't overwrite existing data
        
        # Ensure all edges have knowledge_types (TRAPI 1.4 requirement)
        for edge in meta_kg.get('edges', []):
            if 'knowledge_types' not in edge:
                edge['knowledge_types'] = ['lookup']
        
        # Ensure all nodes have attributes field (TRAPI 1.4 optional but useful)
        for node_data in meta_kg.get('nodes', {}).values():
            if 'attributes' not in node_data:
                node_data['attributes'] = []
        
        # Only add standard attributes to edges that don't have them
        # This preserves KP-specific attributes
        standard_attributes = [
            {
                "attribute_type_id": "biolink:original_predicate",
                "constraint_name": "kg2 ids",
                "constraint_use": True
            },
            {
                "attribute_type_id": "biolink:knowledge_level",
                "constraint_name": "knowledge level",
                "constraint_use": True
            },
            {
                "attribute_type_id": "biolink:agent_type",
                "constraint_name": "agent type",
                "constraint_use": True
            }
        ]
        
        # Add standard attributes only to edges that don't have them
        for edge in meta_kg.get('edges', []):
            if 'attributes' not in edge:
                edge['attributes'] = []
            
            # Only add standard attributes if they're not already present
            existing_attr_types = {attr.get('attribute_type_id') for attr in edge['attributes']}
            for attr in standard_attributes:
                if attr['attribute_type_id'] not in existing_attr_types:
                    edge['attributes'].append(attr)
        
        return meta_kg

    def _get_backup_meta_kg_path(self) -> str:
        """Get the path for the backup meta knowledge graph file"""
        backup_dir = _backup_dir()
        timestamp = datetime.now().strftime("%m%d%Y_%H%M")
        return os.path.join(backup_dir, f"meta_kg_{timestamp}.json")

    def _save_backup_meta_kg(self, meta_kg: Dict[str, Any]) -> bool:
        """Save the meta knowledge graph as a backup file"""
        try:
            backup_path = self._get_backup_meta_kg_path()
            with open(backup_path, 'w') as f:
                json.dump(meta_kg, f, indent=2)
            eprint(f"Backup saved to: {backup_path}")
            
            # Clean up old backups (keep only 3 latest backups)
            self._cleanup_old_backups(keep_count=3)
            
            return True
        except Exception as e:
            eprint(f"ERROR: Failed to save backup: {e}")
            return False

    def _cleanup_old_backups(self, keep_count: int = 3):
        """Clean up old backup files, keeping only the last N backups"""
        try:
            backup_dir = _backup_dir()
            # Only consider files with the exact date-time pattern: meta_kg_MMDDYYYY_HHMM.json
            backup_files = [f for f in os.listdir(backup_dir) if f.startswith("meta_kg_") and f.endswith(".json") and re.match(r"meta_kg_\d{8}_\d{4}\.json$", f)]
            
            if len(backup_files) <= keep_count:
                return  # Keep all if we have fewer than the limit
            
            # Sort by timestamp (newest first, then reverse to get oldest first)
            backup_files.sort(reverse=True)
            
            # Remove old files (keep the most recent ones)
            files_to_remove = backup_files[keep_count:]
            for old_file in files_to_remove:
                old_file_path = os.path.join(backup_dir, old_file)
                try:
                    os.remove(old_file_path)
                    eprint(f"Removed old backup: {old_file}")
                except Exception as e:
                    eprint(f"Failed to remove old backup {old_file}: {e}")
                    
        except Exception as e:
            eprint(f"ERROR: Failed to cleanup old backups: {e}")

    def _load_latest_backup_meta_kg(self) -> Optional[Dict[str, Any]]:
        """Load the most recent backup meta knowledge graph"""
        try:
            backup_dir = _backup_dir()
            # Only consider files with the exact date-time pattern: meta_kg_MMDDYYYY_HHMM.json
            backup_files = [f for f in os.listdir(backup_dir) if f.startswith("meta_kg_") and f.endswith(".json") and re.match(r"meta_kg_\d{8}_\d{4}\.json$", f)]
            
            if not backup_files:
                eprint("No backup files found")
                return None
            
            # Sort by timestamp (newest first)
            backup_files.sort(reverse=True)
            latest_backup = os.path.join(backup_dir, backup_files[0])
            
            with open(latest_backup, 'r') as f:
                backup_data = json.load(f)
            
            eprint(f"Loaded backup from: {latest_backup}")
            return backup_data
            
        except Exception as e:
            eprint(f"ERROR: Failed to load backup: {e}")
            return None

    def _build_dynamic_meta_kg(self) -> Optional[Dict[str, Any]]:
        """Build the meta knowledge graph dynamically from multiple sources"""
        eprint("Building dynamic meta knowledge graph...")
        
        # Step 1: Fetch from Retriever
        retriever_meta_kg = self._fetch_retriever_meta_kg()
        if not retriever_meta_kg:
            eprint("WARNING: Failed to fetch meta knowledge graph from Retriever, using backup")
            # Try to load from backup
            backup_meta_kg = self._load_latest_backup_meta_kg()
            if backup_meta_kg:
                eprint("Using backup meta knowledge graph")
                return backup_meta_kg
            eprint("ERROR: No backup available, cannot provide meta knowledge graph")
            return None
        
        # Step 2: (upstream merged every KP's meta map here; Retriever covers them, DEC-11)

        # Step 3: Add ReasonerAPI information (only adds missing TRAPI 1.4 fields)
        final_meta_kg = self._add_reasoner_api_info(retriever_meta_kg)

        # Step 4: Save backup
        self._save_backup_meta_kg(final_meta_kg)
        
        # Step 5: Cache the result
        KnowledgeSourceMetadata.cached_meta_knowledge_graph = final_meta_kg
        KnowledgeSourceMetadata.cache_timestamp = datetime.now()
        
        eprint("Successfully built dynamic meta knowledge graph")
        return final_meta_kg



    #### Get a list of all supported subjects, predicates, and objects and return in /meta_knowledge_graph format
    def get_meta_knowledge_graph(self, format_='full'):
        method_name = inspect.stack()[0][3]

        # Check if we have a valid cached version
        if self._is_cache_valid():
            if format_ == 'simple':
                return self.simplified_meta_knowledge_graph
            else:
                return self.meta_knowledge_graph

        # Build the dynamic meta knowledge graph
        self.meta_knowledge_graph = self._build_dynamic_meta_kg()
        
        if self.meta_knowledge_graph is None:
            eprint(f"ERROR [{method_name}]: Failed to build meta knowledge graph and no backup available")
            return None
        
        # Create simplified version
        self.create_simplified_meta_knowledge_graph()
        KnowledgeSourceMetadata.cached_simplified_meta_knowledge_graph = self.simplified_meta_knowledge_graph
        
        if format_ == 'simple':
            return self.simplified_meta_knowledge_graph
        else:
            return self.meta_knowledge_graph

    #### Get a list of all supported subjects, predicates, and objects and return in /meta_knowledge_graph format
    def create_simplified_meta_knowledge_graph(self):
        method_name = inspect.stack()[0][3]

        self.simplified_meta_knowledge_graph = {
            'predicates_by_categories': {},
            'supported_predicates': {}
        }

        for edge in self.meta_knowledge_graph['edges']:
            if edge['subject'] not in self.simplified_meta_knowledge_graph['predicates_by_categories']:
                self.simplified_meta_knowledge_graph['predicates_by_categories'][edge['subject']] = {}
            if edge['object'] not in self.simplified_meta_knowledge_graph['predicates_by_categories'][edge['subject']]:
                self.simplified_meta_knowledge_graph['predicates_by_categories'][edge['subject']][edge['object']] = {}
            self.simplified_meta_knowledge_graph['predicates_by_categories'][edge['subject']][edge['object']][edge['predicate']] = True
            self.simplified_meta_knowledge_graph['supported_predicates'][edge['predicate']] = True

        for subject_category,subject_dict in self.simplified_meta_knowledge_graph['predicates_by_categories'].items():
            for object_category,object_dict in subject_dict.items():
                self.simplified_meta_knowledge_graph['predicates_by_categories'][subject_category][object_category] = sorted(list(object_dict))

        self.simplified_meta_knowledge_graph['supported_predicates'] = sorted(list(self.simplified_meta_knowledge_graph['supported_predicates']))

