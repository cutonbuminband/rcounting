import bisect

import networkx as nx
import numpy as np

# See e.g. https://iopscience.iop.org/article/10.1088/1367-2630/14/8/083030


def prepare_graph(graph):
    G = nx.DiGraph(graph)
    G.remove_edges_from(nx.selfloop_edges(G))

    G = symmetrize(G)
    scale_weights(G)
    return G


def weighted_product(x, y, p):
    """
    Calculate the weighted product of x and y with scaling factor p:

    x^p * y^(1 - p)

    If p = 0, only the first factor counts; if p = 1 only the second. Values in between are a mix.
    """
    assert 0 <= p <= 1
    q = 1 - p
    return x ** q * y ** p


def calculate_weighted_degrees(graph, p=0.5):
    degrees = graph.degree()
    weighted_degrees = graph.degree(weight="weight")
    return {node: weighted_product(degrees[node], weighted_degrees[node], p) for node in graph}


def weighted_degree(edgelist, p=0.5):
    return weighted_product(len(edgelist), sum(edgelist.values()), p)


def weighted_core_number(graph, p=0.5):
    degrees = calculate_weighted_degrees(graph, p)
    nodes = sorted(degrees, key=degrees.get)
    weights = [degrees[node] for node in nodes]
    neighbors = {v: dict((x[1:] for x in graph.edges(v, data="weight"))) for v in graph}
    core = degrees
    max_weight = 0
    for _ in range(len(nodes)):
        w = weights.pop(0)
        if w > max_weight:
            max_weight = w
        w = max_weight
        v = nodes.pop(0)
        core[v] = w
        for u in neighbors[v].keys():
            if core[u] > core[v]:
                del neighbors[u][v]
                old_position = bisect.bisect(weights, core[u]) - 1
                new_core = weighted_degree(neighbors[u], p)
                core[u] = new_core
                new_position = bisect.bisect(weights, core[u])
                nodes.remove(u)
                nodes.insert(new_position, u)
                del weights[old_position]
                weights.insert(new_position, core[u])
    return core


def symmetrize(graph):
    UG = graph.to_undirected()
    for node in graph:
        for successor in graph.successors(node):
            if successor in graph.predecessors(node):
                UG.edges[node, successor]["weight"] = (
                    graph.edges[node, successor]["weight"] + graph.edges[successor, node]["weight"]
                )
    return UG


def scale_weights(graph, scaling=lambda x: 1 + np.log10(x)):
    for node in graph:
        for neighbor in graph.neighbors(node):
            graph.edges[node, neighbor]["weight"] = scaling(graph.edges[node, neighbor]["weight"])
    return graph


# Next steps:
# * The onion decomposition
# https://www.nature.com/articles/srep31708
#
# Idea: make a k-shell decomposition (recursively remove nodes of degree <= k
# from the graph until there are no more, then do the same for k+1), but keep
# track of which/how nodes are removed for each pass *within* a shell, and not
# just what the k value is.
