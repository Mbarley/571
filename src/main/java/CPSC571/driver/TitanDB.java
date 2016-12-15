package CPSC571.driver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Graph;

public class TitanDB implements LoadableDatabase
{
	private static String DBPath = "plocal:C:/temp/graph/db";
	private TitanGraph titanDB;
	private HashMap<String, Vertex> vertexMap;
	
	public static void main(String args[])
	{
		TitanDB db = new TitanDB();
		db.start();
	}
	
	public void start()
	{
		titanDB = new TitanGraph("titan-cassandra.properties");
		vertexMap = new HashMap<String, Vertex>();
	}

	public void stop() 
	{
		try
		{
			titanDB.close();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public long loadNodes(Collection<DataNode> nodeCollection)
	{
		long startTime = System.currentTimeMillis();
		
		for(DataNode node : nodeCollection)
		{
			Vertex vertex = titanDB.addVertex(node.getNodeName());
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
			
			Edge edge = fromVertex.addEdge("name", toVertex, "");
			//orientDB.commit();
			//System.out.println("Committed " + fromVertex.getId() + "->" + toVertex.getId());
		}
		
		long endTime = System.currentTimeMillis();
		long loadTime = endTime - startTime;
		
		return loadTime;
	}
	
	public long testReachability(String startVertex) {
		
		long reachabilityStartTime = System.currentTimeMillis();

		Vertex v = titanDB.vertices(null).next();
		
		
		//Find all reachable vertices
		HashSet<Vertex> vSet = new HashSet<Vertex>();
		findReachable(v, vSet);	
		
		long reachabilityTime = System.currentTimeMillis() - reachabilityStartTime;
		return reachabilityTime;
	}
	
	private void findReachable(Vertex v, HashSet<Vertex> vSet) {
		Iterator<Edge> vertices = v.edges(Direction.OUT, "link");
		
		while (vertices.hasNext()) {
			Vertex adj = (Vertex)vertices.next();
			if (!vSet.contains(adj)) {
				vSet.add(adj);
				findReachable(adj, vSet);				
			}
		}
	}
	
	public String toString() {
		return "OrientDB";
	}

	public long testPatternMatching()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	public long updateNodes()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	public long deleteEdges()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	public long deleteNodes()
	{
		// TODO Auto-generated method stub
		return 0;
	}
}
