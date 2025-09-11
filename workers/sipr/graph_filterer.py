
# code to filter a input graph to a smaller graph
# takes in common. graph object and spits out other graph object
#
# FYI: most of this is chatGPT generated

# imports
import networkx as nx
from typing import Callable, List
import random

# constants


# methods
# TODO - not tested
def remove_isolated(input_graph: nx.Graph) -> nx.Graph:
    H = input_graph.copy()
    H.remove_nodes_from(list(nx.isolates(H)))
    return H


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


def filter_graph_by_weight(
    input_graph: nx.Graph, 
    weight_cutoff: float, 
    keep_above: bool = True
) -> nx.Graph:
    """
    Returns a new graph filtered by edge weight.
    
    Parameters:
        G (nx.Graph): Input graph (must have 'weight' attribute on edges)
        weight_cutoff (float): Threshold to filter edges
        keep_above (bool): 
            - True  → keep edges with weight >= cutoff
            - False → keep edges with weight <= cutoff
    
    Returns:
        nx.Graph: Filtered graph
    """
    # Create a new graph of the same type
    H = input_graph.__class__()
    H.add_nodes_from(input_graph.nodes(data=True))  # preserve nodes and attributes

    for u, v, data in input_graph.edges(data=True):
        weight = data.get("weight", 0)
        if keep_above and weight >= weight_cutoff:
            H.add_edge(u, v, **data)
        elif not keep_above and weight <= weight_cutoff:
            H.add_edge(u, v, **data)

    # remove unconnected nodes
    H = remove_isolated(input_graph=H)

    # return
    return H


def apply_to_graph(input_graph: nx.Graph, func: Callable[[nx.Graph], nx.Graph]) -> nx.Graph:
    """
    Applies a user-provided function to a copy of the input graph and returns the result.
    
    Parameters:
        G (nx.Graph): Input graph
        func (Callable): A function that takes a nx.Graph and returns a nx.Graph
    
    Returns:
        nx.Graph: The modified graph
    """
    G_copy = input_graph.copy()
    return func(G_copy)


def apply_list_to_graph(input_graph: nx.Graph, funcs: List[Callable[[nx.Graph], nx.Graph]]) -> nx.Graph:
    """
    Applies a list of functions serially to a copy of the input graph.
    
    Parameters:
        G (nx.Graph): Input graph
        funcs (List[Callable]): List of functions, each taking and returning a nx.Graph
    
    Returns:
        nx.Graph: The modified graph after all functions are applied
    """
    H = G.copy()
    for func in funcs:
        H = func(H)
    return H



# main (mostly for very basic testing)
# TODO - to test, do `python3 graph_filterer.py`
# TODO -> unit testing
if __name__ == "__main__":
    # TEST - filter by edge weight method
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
    H = filter_graph_by_weight(input_graph=G, weight_cutoff=cutoff)

    print(f"\nFiltered graph edges (weight >= {cutoff}):")
    print("filtered original graph from node count: {} to : {}".format(len(list(G.nodes())), len(list(H.nodes()))))
    for u, v, d in H.edges(data=True):
        print(f"{u}-{v}: {d['weight']}")

    # TEST - test the general appy method
    cutoff = 0.2
    H1 = apply_to_graph(input_graph=G, func=lambda g: filter_graph_by_weight(input_graph=g, weight_cutoff=cutoff, keep_above=False))

    print(f"\nFiltered graph edges (weight <= {cutoff}):")
    print("filtered original graph from node count: {} to : {}".format(len(list(G.nodes())), len(list(H1.nodes()))))
    for u, v, d in H1.edges(data=True):
        print(f"{u}-{v}: {d['weight']}")


    # TEST - test the general appy method with list of functions
    funcs = [
        lambda g: filter_graph_by_weight(input_graph=g, weight_cutoff=0.5, keep_above=True),
        lambda g: filter_graph_by_weight(input_graph=g, weight_cutoff=0.7, keep_above=False),
    ]
    H2 = apply_list_to_graph(input_graph=G, funcs=funcs)

    print(f"\nFiltered graph edges (weight between 0.5 and 0.7):")
    print("filtered original graph from node count: {} to : {}".format(len(list(G.nodes())), len(list(H2.nodes()))))
    for u, v, d in H2.edges(data=True):
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
