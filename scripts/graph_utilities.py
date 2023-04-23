import pandas as pd
import networkx as nx


def get_graph_for_bike(data: pd.DataFrame, bike_number: int) -> nx.DiGraph:
    """Given data with rides durations, extracts
    rides made with bike with `bike_number` and builds
    graph, where nodes are bike stations
    and edges weights are sums of ride duration
    between given stations
    """
    data_for_bike = data[data["bike_number"] == bike_number]
    edges_series = (
        data_for_bike.drop(columns=["uid"])
        .groupby(["rental_place", "return_place"])["duration"]
        .sum()
    )
    edges_dict = edges_series.to_dict()
    nodes = list(
        set.union(
            set(data_for_bike["rental_place"]), set(data_for_bike["return_place"])
        )
    )
    bike_graph = nx.DiGraph()
    bike_graph.add_nodes_from(nodes)
    for edge, weight in edges_dict.items():
        source, target = edge
        bike_graph.add_edge(source, target, weight=weight)
    return bike_graph
