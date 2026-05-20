"""Analyze ELK FMS dependency graph: parse CSV, rank by children count, generate visual graph."""

import csv
import re
import sys

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx

CSV_PATH = "Migrations Plan - ELK FMS.csv"
GRAPH_PNG = "fms_dependency_graph.png"
MERMAID_MD = "fms_dependency_graph.md"


def parse_depend_on(raw):
    """Parse '[4,5,6,7]' -> [4, 5, 6]."""
    if not raw or not raw.strip():
        return []
    nums = re.findall(r'\d+', raw)
    return [int(n) for n in nums]


def build_graph(csv_path):
    """Read CSV and build a directed dependency graph."""
    G = nx.DiGraph()
    nodes = {}

    with open(csv_path, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            num = int(row['Number'])
            name = row['Name'].strip()
            node_type = row['Type'].strip()
            deps = parse_depend_on(row.get('Depend On', ''))

            G.add_node(num, name=name, type=node_type)
            nodes[num] = {'name': name, 'type': node_type, 'deps': deps}

            # Edge: dependency -> dependent (provider feeds consumer)
            for dep in deps:
                G.add_edge(dep, num)

    return G, nodes


def print_priority_table(G):
    """Print priority ranking sorted by children count (out-degree)."""
    ranked = sorted(G.nodes(data=True), key=lambda x: G.out_degree(x[0]), reverse=True)

    print()
    print("=" * 95)
    print(" PRIORITY RANKING - Migration Order (by children/dependents count)")
    print("=" * 95)
    print(f" {'Rank':<5} {'ID':<5} {'Name':<50} {'Type':<10} {'Children':<8}")
    print("-" * 95)

    for rank, (node_id, data) in enumerate(ranked, 1):
        children = G.out_degree(node_id)
        name = data['name']
        if len(name) > 48:
            name = name[:45] + "..."
        print(f" {rank:<5} {node_id:<5} {name:<50} {data['type']:<10} {children:<8}")

    print("=" * 95)

    # Summary
    total_nodes = G.number_of_nodes()
    total_edges = G.number_of_edges()
    dashboards = sum(1 for _, d in G.nodes(data=True) if d['type'] == 'Dashboard')
    indices = sum(1 for _, d in G.nodes(data=True) if d['type'] == 'Index')

    print(f"\n Summary: {total_nodes} nodes ({dashboards} Dashboards, {indices} Indices/Transforms), {total_edges} edges")

    zero_dep = [(nid, d['name']) for nid, d in G.nodes(data=True) if G.out_degree(nid) == 0]
    if zero_dep:
        print(f" Leaf nodes (0 dependents): {len(zero_dep)}")
        for nid, name in sorted(zero_dep):
            print(f"   [{nid}] {name}")
    print()


def generate_mermaid(G, output_path):
    """Generate a Mermaid diagram file."""
    lines = ["graph TD"]
    edges = []
    for src, dst in G.edges():
        src_name = G.nodes[src]['name']
        dst_name = G.nodes[dst]['name']
        # Sanitize names for Mermaid
        src_label = src_name.replace('"', "'")
        dst_label = dst_name.replace('"', "'")
        if len(src_label) > 30:
            src_label = src_label[:27] + "..."
        if len(dst_label) > 30:
            dst_label = dst_label[:27] + "..."
        lines.append(f'    {src}["{src_label}"] --> {dst}["{dst_label}"]')

    # Style top nodes
    ranked = sorted(G.nodes(), key=lambda n: G.out_degree(n), reverse=True)
    top5 = ranked[:5]
    for nid in top5:
        children = G.out_degree(nid)
        lines.append(f"    style {nid} fill:#ff6b6b,stroke:#c0392b,color:#fff")

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines) + "\n")
    print(f"Mermaid diagram saved to: {output_path}")


def generate_graph_png(G, output_path):
    """Generate a visual PNG graph with node size proportional to children count."""
    pos = nx.spring_layout(G, k=2.5, iterations=80, seed=42)

    # Colors by type
    node_colors = []
    for n in G.nodes():
        if G.nodes[n]['type'] == 'Index':
            node_colors.append('#4A90D9')
        else:
            node_colors.append('#E8A838')

    # Sizes by children count
    max_children = max(dict(G.out_degree()).values()) if G.nodes() else 1
    node_sizes = []
    for n in G.nodes():
        degree = G.out_degree(n)
        node_sizes.append(300 + (degree / max(max_children, 1)) * 2000)

    # Labels
    labels = {}
    for n in G.nodes():
        name = G.nodes[n]['name']
        if len(name) > 25:
            name = name[:22] + "..."
        labels[n] = name

    fig, ax = plt.subplots(1, 1, figsize=(28, 20))
    nx.draw_networkx_edges(G, pos, ax=ax, edge_color='#CCCCCC', arrows=True,
                           arrowsize=10, alpha=0.5, connectionstyle='arc3,rad=0.1')
    nx.draw_networkx_nodes(G, pos, ax=ax, node_color=node_colors,
                           node_size=node_sizes, edgecolors='white', linewidths=0.5)
    nx.draw_networkx_labels(G, pos, labels, ax=ax, font_size=5, font_weight='bold')

    legend_elements = [
        mpatches.Patch(color='#4A90D9', label='Index / Transform (data source)'),
        mpatches.Patch(color='#E8A838', label='Dashboard (consumer)'),
    ]
    ax.legend(handles=legend_elements, loc='upper left', fontsize=12)
    ax.set_title('FMS ELK → Data Lake Dependency Graph\n(Node size = migration priority / dependents count)',
                 fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Graph image saved to: {output_path}")


def main():
    csv_path = CSV_PATH
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]

    print(f"Parsing: {csv_path}")
    G, nodes = build_graph(csv_path)
    print(f"Built graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    print_priority_table(G)
    generate_mermaid(G, MERMAID_MD)
    generate_graph_png(G, GRAPH_PNG)
    print("Done!")


if __name__ == "__main__":
    main()
