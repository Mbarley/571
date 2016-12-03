package CPSC571.driver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph;

public class Neo4jLoader
{
	static Neo4j2Graph neo4jDB;

	static String path = "/tmp/neo4j";
	
	public static void main(String[] args)
	{
		neo4jDB = new Neo4j2Graph(path);

		DataLoader loader = new DataLoader();
		try
		{
			loader.setInputFile("links.tsv");
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		catch (UnsupportedEncodingException e)
		{
			e.printStackTrace();
		}
		
		try
		{
			loader.readFile();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
		TreeMap<String, Vertex> vertexMap = new TreeMap<String, Vertex>();
		
		long vertexStartTime = System.currentTimeMillis();
		
		for(Map.Entry<String,DataNode> entry : loader.getNodeMap().entrySet()) 
		{
			String name = entry.getKey();
			DataNode node = entry.getValue();
			
			Vertex vertex = neo4jDB.addVertex("page");
			vertex.setProperty("name", node.getNodeName());
			
			vertexMap.put(node.getNodeName(), vertex);
		}
		
		long vertexTime = System.currentTimeMillis() - vertexStartTime;
		
		System.out.println("Vertex Load Time: " + vertexTime + " ms");
		
		long edgeStartTime = System.currentTimeMillis();
		
		for(DataEdge edge : loader.getEdgeMap()) 
		{
			Vertex from = vertexMap.get(edge.getFromNode());
			Vertex to = vertexMap.get(edge.getToNode());
			
			neo4jDB.addEdge("id", from, to, "link");
		}
		
		long edgeTime = System.currentTimeMillis() - edgeStartTime;
		
		System.out.println("Edge Load Time: " + edgeTime + " ms");
		
		neo4jDB.shutdown();
	}
}
