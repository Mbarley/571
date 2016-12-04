package CPSC571.driver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class DBLoader
{
	public static void main(String[] args)
	{
		LoadableDatabase[] databases = {new Neo4j()};
		
		for (LoadableDatabase db : databases) {
			db.start();
			DataLoader loader = loadDB();
			
			System.out.println("\n*** " + db + " ***");
			
			long vertexLoadTime = db.loadNodes(loader.getNodeMap().values());
			System.out.println("Vertex Load Time: " + vertexLoadTime);
			
			long edgeLoadTime = db.loadEdges(loader.getEdgeMap());
			System.out.println("Edge Load Time: " + edgeLoadTime);
			
			System.out.println("Reachability Time: " + db.testReachability("Yeast"));
			
			db.stop();			
		}
	}
	
	private static DataLoader loadDB() {
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
		catch (IOException e) {
			e.printStackTrace();
		}
		
		return loader;
	}
}
