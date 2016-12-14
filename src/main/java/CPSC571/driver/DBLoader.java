package CPSC571.driver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class DBLoader
{
	public static void main(String[] args)
	{
		LoadableDatabase[] databases = {new Neo4j(), new OrientDB()};
		DataLoader loader = loadDB();
		int numTests;
		
		if(args.length > 1)
		{
			numTests = Integer.parseInt(args[1]);
		}
		else
		{
			numTests = 1;
		}
		
		for (LoadableDatabase db : databases) {
			db.start();
			
			System.out.println("\n*** " + db + " ***");
			
			long vertexLoadTime = db.loadNodes(loader.getNodeMap().values());
			System.out.println("Vertex Load Time: " + vertexLoadTime);
			
			long edgeLoadTime = db.loadEdges(loader.getEdgeMap());
			System.out.println("Edge Load Time: " + edgeLoadTime);
			
			System.out.println("Reachability Time: " + db.testReachability("Yeast"));
			
			System.out.println("Pattern Matching Time: " + db.testPatternMatching());
			
			System.out.println("Node Update Time: " + db.updateNodes());
			
			System.out.println("Edge Delete Time: " + db.deleteEdges());
			
			System.out.println("Node Delete Time: " + db.deleteNodes());
			
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
