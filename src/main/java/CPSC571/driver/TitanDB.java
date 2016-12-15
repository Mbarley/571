package CPSC571.driver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Graph;


public class TitanDB implements LoadableDatabase
{
	private static String DBPath = "plocal:C:/temp/graph/db";
	private Graph titanDB;
	private HashMap<String, Vertex> vertexMap;
	

	public void start()
	{
		BaseConfiguration baseConfiguration = new BaseConfiguration();
		titanDB = TitanFactory.open(baseConfiguration);
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

		Vertex v = titanDB.(vertexMap.get(startVertex));
		
		
		//Find all reachable vertices
		HashSet<TitanVertex> vSet = new HashSet<TitanVertex>();
		findReachable(v, vSet);	
		
		long reachabilityTime = System.currentTimeMillis() - reachabilityStartTime;
		return reachabilityTime;
	}
	
	private void findReachable(TitanVertex v, HashSet<TitanVertex> vSet) {
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
