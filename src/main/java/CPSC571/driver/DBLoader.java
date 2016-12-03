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
	
	public static void main(String[] args)
	{
		LoadableDatabase orientDB = new DBOrientDB();
		orientDB.reset();
		orientDB.start();
		
		DataLoader loader = new DataLoader();
		try
		{
			loader.setInputFile("youtube.txt");
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
		
		orientDB.loadNodes(loader.getNodeMap().values());
		System.out.println(orientDB.getVertexLoadTime());
		orientDB.loadEdges(loader.getEdgeMap());
		System.out.println(orientDB.getEdgeLoadTime());
	}
}
