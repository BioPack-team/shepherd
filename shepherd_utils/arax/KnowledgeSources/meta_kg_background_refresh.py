# Ported from RTXteam/RTX @ 9485431, code/ARAX/KnowledgeSources/meta_kg_background_refresh.py.
# Changes from upstream:
#   - import paths / sys.path hacks only (including class-name strings)
#   - function mode only: signal_handler and the standalone main() loop are removed;
#     Shepherd's server runs refresh_meta_kg() hourly (the ARAX background tasker's job)
#   - the backup check looks in the backup dir (settings.arax_dbs_dir). It still looks
#     for upstream's "meta_kg_backup_" prefix, which the backups never have (DEC-1)
# See docs/ARAX_PORT_BASELINE.md and shepherd_utils/arax/README.md.
"""
Meta Knowledge Graph Background Refresh Module

This module provides two modes of operation:

1. FUNCTION MODE (Primary use in ARAX):
   - The refresh_meta_kg() function is called by ARAXBackgroundTasker
   - No signal handling or main loop - managed by ARAXBackgroundTasker
   - Used in production ARAX deployments

2. STANDALONE MODE (Testing/Debugging):
   - The main() function runs as a standalone service
   - Includes signal handling and infinite loop
   - Useful for testing meta KG refresh independently
   - Can be run as: python meta_kg_background_refresh.py

USAGE:
- In ARAX: ARAXBackgroundTasker calls refresh_meta_kg() function
- Standalone: Run this script directly for testing/debugging
- Cron/Systemd: Can be configured as a standalone service
"""

import os
import sys
import time
from datetime import datetime

# Add the necessary paths

from shepherd_utils.arax.KnowledgeSources.knowledge_source_metadata import KnowledgeSourceMetadata, _backup_dir

def refresh_meta_kg():
    """
    Refresh the meta knowledge graph cache
    
    This function is the primary entry point used by ARAXBackgroundTasker.
    It performs a single refresh operation and returns success/failure.
    
    Returns:
        bool: True if refresh was successful, False otherwise
    """
    try:
        print(f"[{datetime.now()}] Starting meta knowledge graph refresh...")
        ksm = KnowledgeSourceMetadata()
        
        # Force refresh by clearing cache
        KnowledgeSourceMetadata.cached_meta_knowledge_graph = None
        KnowledgeSourceMetadata.cache_timestamp = None
        
        # Build the meta knowledge graph
        meta_kg = ksm.get_meta_knowledge_graph()
        
        if meta_kg:
            print(f"[{datetime.now()}] Successfully refreshed meta knowledge graph")
            print(f"[{datetime.now()}] Meta KG contains {len(meta_kg.get('edges', []))} edges and {len(meta_kg.get('nodes', {}))} node types")
            
            # Check if backup was created
            backup_dir = _backup_dir()
            backup_files = [f for f in os.listdir(backup_dir) if f.startswith("meta_kg_backup_") and f.endswith(".json")]
            if backup_files:
                latest_backup = sorted(backup_files, reverse=True)[0]
                print(f"[{datetime.now()}] Backup created: {latest_backup}")
        else:
            print(f"[{datetime.now()}] ERROR: Failed to refresh meta knowledge graph")
            return False
            
    except Exception as e:
        print(f"[{datetime.now()}] ERROR: Exception during meta KG refresh: {e}")
        return False
    
    return True

