import networkx as nx
from typing import Dict, List, Any, Optional


class DependencyGraph:
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.pipelines: Dict[str, Dict] = {}
    
    def add_pipeline(self, pipeline_data: Dict[str, Any], parent: Optional[str] = None):
        name = pipeline_data.get("name", "Unknown")
        self.pipelines[name] = pipeline_data
        self.graph.add_node(name, data=pipeline_data)
        
        if parent:
            self.graph.add_edge(parent, name)
        
        from app.engine.parser import SLPParser
        parser = SLPParser()
        child_refs = parser.find_child_references(pipeline_data)
        
        for child in child_refs:
            if child not in self.graph:
                self.graph.add_node(child, data=None, is_placeholder=True)
            self.graph.add_edge(name, child)
    
    def get_execution_order(self) -> List[str]:
        try:
            return list(reversed(list(nx.topological_sort(self.graph))))
        except nx.NetworkXUnfeasible:
            return list(self.graph.nodes())
    
    def get_missing_pipelines(self) -> List[str]:
        missing = []
        for node in self.graph.nodes():
            node_data = self.graph.nodes[node]
            if node_data.get("is_placeholder", False) or node_data.get("data") is None:
                missing.append(node)
        return missing
    
    def get_children(self, pipeline_name: str) -> List[str]:
        if pipeline_name in self.graph:
            return list(self.graph.successors(pipeline_name))
        return []
    
    def get_parents(self, pipeline_name: str) -> List[str]:
        if pipeline_name in self.graph:
            return list(self.graph.predecessors(pipeline_name))
        return []
    
    def get_all_descendants(self, pipeline_name: str) -> List[str]:
        if pipeline_name in self.graph:
            return list(nx.descendants(self.graph, pipeline_name))
        return []
    
    def get_pipeline_data(self, pipeline_name: str) -> Optional[Dict]:
        return self.pipelines.get(pipeline_name)
    
    def get_graph_visualization(self) -> Dict[str, Any]:
        nodes = []
        edges = []
        
        for node in self.graph.nodes():
            node_data = self.graph.nodes[node]
            nodes.append({
                "id": node,
                "label": node,
                "is_loaded": node_data.get("data") is not None,
                "snap_count": len(node_data.get("data", {}).get("snaps", [])) if node_data.get("data") else 0
            })
        
        for source, target in self.graph.edges():
            edges.append({
                "source": source,
                "target": target
            })
        
        return {"nodes": nodes, "edges": edges}
    
    def clear(self):
        self.graph.clear()
        self.pipelines.clear()
