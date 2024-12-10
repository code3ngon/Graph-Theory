import pandas as pd
from neo4j import GraphDatabase

# Kết nối với Neo4j
class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.uri = uri
        self.user = user
        self.pwd = pwd
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.pwd))
        self.session = self.driver.session()

    def close(self):
        self.session.close()

    def query(self, query, parameters=None):
        result = self.session.run(query, parameters or {})
        return result

class Vertex:
    def __init__(self, data):
        self.data = data
        self.adjacent = {}  
        self.distance = float('inf')  
        self.visited = False
        self.previous = None

    def add_neighbor(self, neighbor, weight):
        self.adjacent[neighbor] = weight

class Graph:
    def __init__(self, neo4j_conn=None):
        self.vertices = {}
        self.neo4j_conn = neo4j_conn  # Kết nối Neo4j

    def add_vertex(self, data):
        vertex = Vertex(data)
        self.vertices[data] = vertex
        
        # Lưu đỉnh vào Neo4j
        if self.neo4j_conn:
            self.create_vertex(self.neo4j_conn, data)
        
        return vertex

    def add_edge(self, from_data, to_data, weight):
        if from_data in self.vertices and to_data in self.vertices:
            self.vertices[from_data].add_neighbor(self.vertices[to_data], weight)
            self.vertices[to_data].add_neighbor(self.vertices[from_data], weight)

            # Lưu cạnh vào Neo4j
            if self.neo4j_conn:
                self.create_edge(self.neo4j_conn, from_data, to_data, weight)

    def dijkstra(self, start_data):
        if start_data not in self.vertices:
            return "Start vertex not found"

        start_vertex = self.vertices[start_data]
        start_vertex.distance = 0

        unvisited = list(self.vertices.values())

        log_steps = []

        while unvisited:
            current = min(unvisited, key=lambda vertex: vertex.distance)
            if current.distance == float('inf'):
                break

            current.visited = True
            unvisited.remove(current)

            for neighbor, weight in current.adjacent.items():
                if not neighbor.visited:
                    alt_distance = current.distance + weight
                    if alt_distance < neighbor.distance:
                        neighbor.distance = alt_distance
                        neighbor.previous = current

            log_steps.append({
                'Current Node': current.data,
                'Visited Nodes': [v.data for v in self.vertices.values() if v.visited],
                'Distances': {v.data: v.distance for v in self.vertices.values()}
            })

        df_log = pd.DataFrame(log_steps)
        return df_log

    def create_vertex(self, neo4j_conn, vertex_data):
        query = "CREATE (v:Vertex {data: $data})"
        neo4j_conn.query(query, parameters={"data": vertex_data})

    def create_edge(self, neo4j_conn, from_data, to_data, weight):
        query = """
        MATCH (a:Vertex {data: $from_data}), (b:Vertex {data: $to_data})
        CREATE (a)-[:CONNECTED {weight: $weight}]->(b)
        """
        neo4j_conn.query(query, parameters={"from_data": from_data, "to_data": to_data, "weight": weight})

    def save_graph_to_neo4j(self):
        # Xóa tất cả các node và relationship cũ trước khi lưu
        self.neo4j_conn.query("MATCH (n) DETACH DELETE n")

        # Lưu các đỉnh
        for vertex_data in self.vertices:
            self.create_vertex(self.neo4j_conn, vertex_data)

        # Lưu các cạnh
        for vertex_data in self.vertices:
            vertex = self.vertices[vertex_data]
            for neighbor, weight in vertex.adjacent.items():
                self.create_edge(self.neo4j_conn, vertex_data, neighbor.data, weight)

    def save_shortest_path_to_neo4j(self, path):
        for i in range(len(path) - 1):
            from_vertex = path[i]
            to_vertex = path[i + 1]
            self.neo4j_conn.query("""
            MATCH (a:Vertex {data: $from_data}), (b:Vertex {data: $to_data})
            MERGE (a)-[:SHORTEST_PATH]->(b)
            """, parameters={"from_data": from_vertex, "to_data": to_vertex})
