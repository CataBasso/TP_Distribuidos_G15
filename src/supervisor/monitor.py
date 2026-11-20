class Monitor:
    def __init__(self, nodes, miss_limit):
        self.nodes = nodes
        self.miss_limit = miss_limit
        self.node_miss_count = {}     
        self.dead_nodes = set()    # nodos ya declarados caídos (para no devolverlos dos veces)

    def update(self, results):
        miss_limit_nodes = []
        for node in self.nodes:
            if not results.get(node): #no responde, se suma un miss
                self.node_miss_count[node] = self.node_miss_count.get(node,0) + 1

                if self.node_miss_count[node] >= self.miss_limit and node not in self.dead_nodes:
                    miss_limit_nodes.append(node)
                    self.dead_nodes.add(node)     

            elif results.get(node): #responde, se resetan los miss
                self.node_miss_count[node] = 0
                self.dead_nodes.discard(node) # si estaba muerto lo saca del set de muertos

        return miss_limit_nodes

    def mark_restarted(self, node):
        if node in self.dead_nodes:
            self.node_miss_count[node] = 0
            self.dead_nodes.discard(node)

    def reset(self):
        self.node_miss_count.clear()
        self.dead_nodes.clear()
