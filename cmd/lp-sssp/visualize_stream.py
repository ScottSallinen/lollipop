# import networkx as nx
# import matplotlib.pyplot as plt
# import time
#
# def read_edge_stream(filename):
#     """Reads edge operations from a file."""
#     with open(filename, 'r') as file:
#         for line in file:
#             yield line.strip()
#
# def visualize_graph_evolution(filename, delay=1):
#     """Visualizes graph evolution from an edge stream."""
#     G = nx.Graph()
#     plt.ion()  # Turn on interactive mode
#     fig, ax = plt.subplots()
#     timestamp = 0
#
#     for line in read_edge_stream(filename):
#         parts = line.split()
#         if len(parts) == 2:  # Initial edge
#             u, v = map(int, parts)
#             G.add_edge(u, v)
#         elif len(parts) == 3 and parts[0] == 'D':  # Delete edge
#             u, v = map(int, parts[1:])
#             if G.has_edge(u, v):
#                 G.remove_edge(u, v)
#         elif len(parts) == 2:  # Add edge
#             u, v = map(int, parts)
#             G.add_edge(u, v)
#
#         ax.clear()
#         nx.draw(G, with_labels=True, node_color='lightblue', edge_color='gray', ax=ax)
#         ax.set_title(f'Timestamp: {timestamp}')
#         plt.pause(delay)
#         timestamp += 1
#
#     plt.ioff()
#     plt.show()
#
# if __name__ == "__main__":
#     filename = "/Users/pjavanrood/Documents/NetSys/lollipop/data/test2.txt"  # Change to your actual filename
#     visualize_graph_evolution(filename)


import networkx as nx
import matplotlib.pyplot as plt
import time
from matplotlib.widgets import Button

def read_edge_stream(filename):
    """Reads edge operations from a file."""
    with open(filename, 'r') as file:
        return [line.strip() for line in file]

def update_graph(ax, G, edges, index):
    """Updates the graph based on the given index."""
    G.clear()
    for i in range(index + 1):
        parts = edges[i].split()
        if len(parts) == 2:
            u, v = map(int, parts)
            G.add_edge(u, v)
        elif len(parts) == 3 and parts[0] == 'D':
            u, v = map(int, parts[1:])
            if G.has_edge(u, v):
                G.remove_edge(u, v)

    ax.clear()
    nx.draw(G, with_labels=True, node_color='lightblue', edge_color='gray', ax=ax)
    ax.set_title(f'Timestamp: {index}')
    plt.draw()

def visualize_graph_evolution(filename):
    """Visualizes graph evolution with navigation buttons."""
    edges = read_edge_stream(filename)
    G = nx.DiGraph()
    fig, ax = plt.subplots()
    plt.subplots_adjust(bottom=0.2)

    index = [0]
    update_graph(ax, G, edges, index[0])

    def next_event(event):
        if index[0] < len(edges) - 1:
            index[0] += 1
            update_graph(ax, G, edges, index[0])

    def prev_event(event):
        if index[0] > 0:
            index[0] -= 1
            update_graph(ax, G, edges, index[0])

    axprev = plt.axes([0.7, 0.05, 0.1, 0.075])
    axnext = plt.axes([0.81, 0.05, 0.1, 0.075])
    bnext = Button(axnext, 'Next')
    bprev = Button(axprev, 'Prev')

    bnext.on_clicked(next_event)
    bprev.on_clicked(prev_event)

    plt.show()

if __name__ == "__main__":
    filename = "/Users/pjavanrood/Documents/NetSys/lollipop/random_20_400.txt"  # Change to your actual filename
    visualize_graph_evolution(filename)
