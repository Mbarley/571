package CPSC571.driver;

import java.util.Collection;
import java.util.HashMap;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class OrientDB implements LoadableDatabase
{
	private static String DBPath = "plocal:C:/temp/graph/db";
	private OrientGraph orientDB;
	private HashMap<String, Vertex> vertexMap;

	public void start()
	{
		orientDB = new OrientGraph(DBPath, "admin", "admin");
		orientDB.createVertexType("page");
		orientDB.createEdgeType("link");
		vertexMap = new HashMap<String, Vertex>();
	}

	public void stop() {
		if (orientDB != null) {
			orientDB.shutdown();
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
			orientDB.commit();
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
			orientDB.commit();
			System.out.println("Committed " + fromVertex.getId() + "->" + toVertex.getId());
		}
		
		long endTime = System.currentTimeMillis();
		long loadTime = endTime - startTime;
		
		return loadTime;
	}
	
	public long testReachability(String startVertex) {
		
		//TODO
		return 0;
	}
	
	public String toString() {
		return "OrientDB";
	}
}
