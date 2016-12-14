package CPSC571.driver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class OrientDB implements LoadableDatabase
{
	private static String DBPath = "plocal:C:/temp/graph/db";
	private OrientGraph orientDB;
	private HashMap<String, Vertex> vertexMap;

	public void start()
	{
		orientDB = new OrientGraph(DBPath, "admin", "admin");
		orientDB.drop();
		orientDB = new OrientGraph(DBPath, "admin", "admin");
		orientDB.createVertexType("page");
		orientDB.createEdgeType("link");
		vertexMap = new HashMap<String, Vertex>();
	}

	public void stop() {
		if (orientDB != null) {
			orientDB.drop();
		}
	}
	
	public long loadNodes(Collection<DataNode> nodeCollection)
	{
		long startTime = System.currentTimeMillis();
		
		for(DataNode node : nodeCollection)
		{
			Vertex vertex = orientDB.addVertex("class:page");
			vertex.setProperty("name", node.getNodeName());
			vertexMap.put(node.getNodeName(), vertex);
			//orientDB.commit();
			//System.out.println("Committed Vertex: " + vertex.getProperty("name"));
		}
		
		long endTime = System.currentTimeMillis();
		long loadTime = endTime - startTime;
		
		//System.out.println("Vertex Time: " + (vertexLoadEndTime - vertexLoadStartTime));
		return loadTime;
	}

	public long loadEdges(Collection<DataEdge> edgeCollection)
	{
		long startTime = System.currentTimeMillis();
		
		for(DataEdge dataEdge : edgeCollection)
		{
			Vertex fromVertex = vertexMap.get(dataEdge.getFromNode());
			Vertex toVertex = vertexMap.get(dataEdge.getToNode());
			
			if(fromVertex == null || toVertex == null)
			{
				continue;
			}
			
			Edge edge = orientDB.addEdge("class:link", fromVertex, toVertex, "link");
			//orientDB.commit();
			//System.out.println("Committed " + fromVertex.getId() + "->" + toVertex.getId());
		}
		
		long endTime = System.currentTimeMillis();
		long loadTime = endTime - startTime;
		
		return loadTime;
	}
	
	public long testReachability(String startVertex) {
		
		long reachabilityStartTime = System.currentTimeMillis();

		Vertex v = orientDB.getVertex(vertexMap.get(startVertex));
		
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
	
	public long testPatternMatching() {
		
		long startTime = System.currentTimeMillis();

		GremlinPipeline g = new GremlinPipeline(orientDB.getVertices());
		g.V().as("x").out("link").as("z").out("link").as("y").out("link").as("x").in("link").as("z").in("link").as("y").in("link").as("x");	
        
		long patternMatchingTime = System.currentTimeMillis() - startTime;
		return patternMatchingTime;
	}
	
	public long updateNodes()
	{
		long startTime = System.currentTimeMillis();
		
		for(Vertex vertex : orientDB.getVertices())
		{
			vertex.setProperty("Update", "Update");
		}
		
		long endTime = System.currentTimeMillis();
		
		return endTime - startTime;
	}
	
	public long deleteEdges()
	{
		long startTime = System.currentTimeMillis();
		
		for(Edge edge : orientDB.getEdges())
		{
			orientDB.removeEdge(edge);
		}
		
		long endTime = System.currentTimeMillis();
		
		return endTime - startTime;
	}
	
	public long deleteNodes()
	{
		long startTime = System.currentTimeMillis();
		
		for(Vertex vertex : orientDB.getVertices())
		{
			orientDB.removeVertex(vertex);
		}
		
		long endTime = System.currentTimeMillis();
		
		return endTime - startTime;
	}
	
	public String toString() {
		return "OrientDB";
	}
}
