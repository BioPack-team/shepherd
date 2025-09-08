
# code to filter a input graph to a smaller graph
# takes in common. graph object and spits out other graph object
#
# FYI: most of this is chatGPT generated

# imports
import networkx as nx
from typing import Callable
import random

# constants


# methods
# TODO - not tested
def add_hub_node(G: nx.Graph, hub_name: str = "Hub") -> nx.Graph:
    """
    Takes in a NetworkX Graph, adds a hub node connected to all existing nodes,
    and returns the modified graph.
    
    Parameters:
        G (nx.Graph): Input graph
        hub_name (str): Name of the hub node (default "Hub")
    
    Returns:
        nx.Graph: Modified graph with the hub node
    """
    # Make a copy so we don’t mutate the original graph
    H = G.copy()
    
    # Add the hub node
    H.add_node(hub_name)
    
    # Connect hub to all other nodes
    for node in H.nodes():
        if node != hub_name:
            H.add_edge(hub_name, node)
    
    return H


def filter_graph_by_weight(G: nx.Graph, weight_cutoff: float) -> nx.Graph:
    """
    Returns a new graph containing only the edges with weight >= weight_cutoff.
    
    Parameters:
        G (nx.Graph): Input graph (must have 'weight' attribute on edges)
        weight_cutoff (float): Minimum weight threshold to keep an edge
    
    Returns:
        nx.Graph: Filtered graph
    """
    # Create a new graph of the same type
    H = G.__class__()
    H.add_nodes_from(G.nodes(data=True))  # preserve nodes and their attributes
    
    # Add edges that meet cutoff
    for u, v, data in G.edges(data=True):
        if data.get("weight", 0) >= weight_cutoff:
            H.add_edge(u, v, **data)
    
    return H


# TODO - not tested
def apply_to_graph(G: nx.Graph, func: Callable[[nx.Graph], nx.Graph]) -> nx.Graph:
    """
    Applies a user-provided function to a copy of the input graph and returns the result.
    
    Parameters:
        G (nx.Graph): Input graph
        func (Callable): A function that takes a nx.Graph and returns a nx.Graph
    
    Returns:
        nx.Graph: The modified graph
    """
    G_copy = G.copy()
    return func(G_copy)



# main (mostly for very basic testing)
# TODO -> unit testing
if __name__ == "__main__":
    # Step 1: Create a random graph with 10 nodes
    G = nx.erdos_renyi_graph(n=10, p=0.4, seed=42)  # 40% chance of edge

    # Step 2: Assign random weights between 0 and 1
    for u, v in G.edges():
        G[u][v]["weight"] = round(random.random(), 2)

    print("Original graph edges with weights:")
    for u, v, d in G.edges(data=True):
        print(f"{u}-{v}: {d['weight']}")

    # Step 3: Filter by weight cutoff
    cutoff = 0.5
    H = filter_graph_by_weight(G, cutoff)

    print(f"\nFiltered graph edges (weight >= {cutoff}):")
    for u, v, d in H.edges(data=True):
        print(f"{u}-{v}: {d['weight']}")



# sample output
# Original graph edges with weights:
# 0-2: 0.83
# 0-3: 0.7
# 0-4: 0.44
# 0-8: 0.43
# 1-2: 0.74
# 1-3: 0.34
# 1-5: 0.92
# 1-6: 0.43
# 1-9: 0.79
# 2-5: 0.3
# 2-8: 0.45
# 2-9: 0.68
# 3-5: 0.11
# 3-6: 0.98
# 3-7: 0.75
# 4-9: 0.74
# 6-9: 0.19
# 7-8: 0.72
# 7-9: 0.54
# 8-9: 0.42

# Filtered graph edges (weight >= 0.5):
# 0-2: 0.83
# 0-3: 0.7
# 1-2: 0.74
# 1-5: 0.92
# 1-9: 0.79
# 2-9: 0.68
# 3-6: 0.98
# 3-7: 0.75
# 4-9: 0.74
# 7-8: 0.72
# 7-9: 0.54
