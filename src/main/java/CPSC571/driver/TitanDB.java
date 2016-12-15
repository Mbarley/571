package CPSC571.driver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.tinkerpop.gremlin.java.GremlinPipeline;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TitanDB implements LoadableDatabase
{
	private static String DBPath = "plocal:C:/temp/titan/db";
	private TitanGraph titanDB;
	private HashMap<String, Vertex> vertexMap;
	
	public static void main(String args[])
	{
		TitanDB db = new TitanDB();
		db.start();
		db.stop();
	}
	
	public void start()
	{
        BaseConfiguration configuration = new BaseConfiguration();

        configuration.setProperty("storage.backend", "cassandrathrift");
        configuration.setProperty("storage.hostname", "localhost");
		titanDB = TitanFactory.open(configuration); 
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
			Vertex vertex = titanDB.addVertex("titan" + node.getNodeName());
			//System.out.println(vertex.label());
			vertexMap.put(node.getNodeName(), vertex);
			//orientDB.commit();
			//System.out.println("Committed Vertex: " + vertex.label());
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
			
			Edge edge = fromVertex.addEdge("name", toVertex);
			//orientDB.commit();
			//System.out.println("Committed " + fromVertex.label() + "->" + toVertex.label());
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
		return "TitanDB";
	}

	public long testPatternMatching()
	{
		long startTime = System.currentTimeMillis();

		GremlinPipeline g = new GremlinPipeline(titanDB.vertices(null));
		g.V().as("x").out("link").as("z").out("link").as("y").out("link").as("x").in("link").as("z").in("link").as("y").in("link").as("x");	
        
		long patternMatchingTime = System.currentTimeMillis() - startTime;
		return patternMatchingTime;
	}

	public long updateNodes()
	{
		long startTime = System.currentTimeMillis();
		
		Iterator<Vertex> vertices = titanDB.vertices(null);
		
		while(vertices.hasNext())
		{
			Vertex vertex = vertices.next();
			vertex.property("Update", "Update");
		}
		
		long endTime = System.currentTimeMillis();
		long result = endTime - startTime;
		
		return result;
	}

	public long deleteEdges()
	{
		long startTime = System.currentTimeMillis();

		Iterator<Edge> edges = titanDB.edges(null);
		
		while(edges.hasNext())
		{
			Edge edge = edges.next();
			edge.remove();
		}
		
		long endTime = System.currentTimeMillis();
		long result = endTime - startTime;
		
		return result;
	}

	public long deleteNodes()
	{
		long startTime = System.currentTimeMillis();

		Iterator<Vertex> vertices = titanDB.vertices(null);
		
		while(vertices.hasNext())
		{
			Vertex vertex = vertices.next();
			vertex.remove();
		}
		
		long endTime = System.currentTimeMillis();
		long result = endTime - startTime;
		
		return result;
	}
}
