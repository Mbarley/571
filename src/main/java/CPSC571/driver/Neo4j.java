package CPSC571.driver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph;

public class Neo4j implements LoadableDatabase
{
	static Neo4j2Graph neo4jDB;

	static String path = "/tmp/neo4j";
	
	TreeMap<String, Vertex> vertexMap;

	private static HashMap<String, Object> vMap;
		
	public void start()
	{
		neo4jDB = new Neo4j2Graph(path);
		vertexMap = new TreeMap<String, Vertex>();
		vMap = new HashMap<String, Object>();	
	}
	
	public void stop() {
		if (neo4jDB != null) {
			neo4jDB.shutdown();
		}
	}
	
	public long loadNodes(Collection<DataNode> nodeCollection)
	{
		long startTime = System.currentTimeMillis();
		
		for(DataNode node : nodeCollection)
		{		
			Vertex vertex = neo4jDB.addVertex("page");
			vertex.setProperty("name", node.getNodeName());
			vertexMap.put(node.getNodeName(), vertex);
				
			vMap.put(node.getNodeName(), vertex.getId());
		}
		
		long loadTime = System.currentTimeMillis() - startTime;
		return loadTime;
	}
	
	public long loadEdges(Collection<DataEdge> edgeCollection)
	{
		long startTime = System.currentTimeMillis();
		
		for(DataEdge edge : edgeCollection)
		{
			Vertex from = vertexMap.get(edge.getFromNode());
			Vertex to = vertexMap.get(edge.getToNode());
			
			neo4jDB.addEdge(null, from, to, "link");
		}
		
		long loadTime = System.currentTimeMillis() - startTime;
		return loadTime;
	}
	
	public long testReachability(String startVertex) {
		
		long reachabilityStartTime = System.currentTimeMillis();

		Vertex v = neo4jDB.getVertex(vMap.get(startVertex));
		
		//Find all reachable vertices
		HashSet<Vertex> vSet = new HashSet<Vertex>();
		findReachable(v, vSet);	
		
		long reachabilityTime = System.currentTimeMillis() - reachabilityStartTime;
		return reachabilityTime;
	}
	
	private void findReachable(Vertex v, HashSet<Vertex> vSet) {
		Iterable<Vertex> vertices = v.getVertices(Direction.OUT, "link");
		Iterator<Vertex> i = vertices.iterator();

		while (i.hasNext()) {
			Vertex adj = (Vertex)i.next();
			if (!vSet.contains(adj)) {
				vSet.add(adj);
				findReachable(adj, vSet);				
			}
		}
	}
	
	public String toString() {
		return "Neo4j";
	}
}
