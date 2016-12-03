package CPSC571.driver;

import java.util.Collection;
import java.util.HashMap;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class DBOrientDB implements LoadableDatabase
{
	private static String DBPath = "plocal:C:/temp/graph/db";
	private OrientGraph orientDB;
	private HashMap<String, Vertex> vertexMap;
	
	//timings
	private long vertexLoadStartTime;
	private long vertexLoadEndTime;
	private long edgeLoadStartTime;
	private long edgeLoadEndTime;
	private long vertexLoadTime;
	private long edgeLoadTime;

	public void reset()
	{
		orientDB = new OrientGraph(DBPath, "admin", "admin");
		orientDB.drop();
	}

	public void start()
	{
		orientDB = new OrientGraph(DBPath, "admin", "admin");
		orientDB.createVertexType("page");
		orientDB.createEdgeType("link");
		vertexMap = new HashMap<String, Vertex>();
	}

	public void loadNodes(Collection<DataNode> nodeCollection)
	{
		vertexLoadStartTime = System.currentTimeMillis();
		
		for(DataNode node : nodeCollection)
		{
			Vertex vertex = orientDB.addVertex("class:page");
			vertex.setProperty("name", node.getNodeName());
			vertexMap.put(node.getNodeName(), vertex);
			orientDB.commit();
			//System.out.println("Committed Vertex: " + vertex.getProperty("name"));
		}
		
		vertexLoadEndTime = System.currentTimeMillis();
		vertexLoadTime = vertexLoadEndTime - vertexLoadStartTime;
		
		//System.out.println("Vertex Time: " + (vertexLoadEndTime - vertexLoadStartTime));
	}

	public void loadEdges(Collection<DataEdge> edgeCollection)
	{
		edgeLoadStartTime = System.currentTimeMillis();
		
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
		
		edgeLoadEndTime = System.currentTimeMillis();
		edgeLoadTime = edgeLoadEndTime - edgeLoadStartTime;
	}

	public long getVertexLoadTime()
	{
		return vertexLoadTime;
	}

	public long getEdgeLoadTime()
	{
		return edgeLoadTime;
	}

}
