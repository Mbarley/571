package CPSC571.driver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

public class DBLoader
{
	static OrientGraph orientDB;
	static OrientGraphFactory factory;
	static String path = "plocal:C:/temp/graph/db";
	
	public static void main(String[] args)
	{
		resetOrientDB(path);
		orientDB = new OrientGraph(path, "admin", "admin"); 
		orientDB.createVertexType("page");
		DataLoader loader = new DataLoader();
		try
		{
			loader.setInputFile("links.tsv");
		}
		catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (UnsupportedEncodingException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try
		{
			loader.readFile();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
		}
		
		TreeMap<String, Vertex> vertexMap = new TreeMap<String, Vertex>();
		
		long vertexStartTime = System.currentTimeMillis();
		
		for(Map.Entry<String,DataNode> entry : loader.getNodeMap().entrySet()) 
		{
			String name = entry.getKey();
			DataNode node = entry.getValue();
			
			Vertex vertex = orientDB.addVertex("page");
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
			
			OrientEdge orientEdge = orientDB.addEdge("", from, to, "");
		}
		
		long edgeTime = System.currentTimeMillis() - edgeStartTime;
		
		System.out.println("Edge Load Time: " + edgeTime + " ms");
	}

	private static void resetOrientDB(String dbPath)
	{
		orientDB = new OrientGraph(dbPath, "admin", "admin");
		orientDB.drop();
	}
}
