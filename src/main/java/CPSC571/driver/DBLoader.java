package CPSC571.driver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

public class DBLoader
{
	public static void main(String[] args)
	{
		LoadableDatabase[] databases = {new Neo4j(), new OrientDB()};
		DataLoader loader = loadDB(args[0]);
		int numTests;
		
		ArrayList<Results> results = new ArrayList<Results>();
		
		if(args.length > 1)
		{
			numTests = Integer.parseInt(args[1]);
		}
		else
		{
			numTests = 1;
		}
		
		for (LoadableDatabase db : databases) 
		{
			Results dbResults = new Results(db.toString());
			
			for(int i = 0; i < numTests; i++)
			{
				db.start();
				
				System.out.println("\n*** " + db + " ***");
				System.out.println("Run " + (i + 1));
				
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
	}
	
	private static DataLoader loadDB(String inputFile) {
		DataLoader loader = new DataLoader();
		try
		{
			loader.setInputFile(inputFile);
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

class Results
{		
	String DBName;
	
	public Results(String DBName)
	{
		this.DBName = DBName;
		vertexLoadTimes = new ArrayList<Long>();
		edgeLoadTimes = new ArrayList<Long>();
		reachabilityTimes = new ArrayList<Long>();
		patternMatchingTimes = new ArrayList<Long>();
		nodeUpdateTimes = new ArrayList<Long>();
		edgeDeleteTimes = new ArrayList<Long>();
		nodeDeleteTimes = new ArrayList<Long>();
	}
	
	ArrayList<Long> vertexLoadTimes;
	ArrayList<Long> edgeLoadTimes;
	ArrayList<Long> reachabilityTimes;
	ArrayList<Long> patternMatchingTimes;
	ArrayList<Long> nodeUpdateTimes;
	ArrayList<Long> edgeDeleteTimes;
	ArrayList<Long> nodeDeleteTimes;
	
	long totalVertexLoadTime;
	long totalEdgeLoadTime;
	long totalReachabilityTime;
	long totalPatternMatchingTime;
	long totalNodeUpdateTime;
	long totalEdgeDeleteTime;
	long totalNodeDeleteTime;
	long averageVertexLoadTime;
	long averageEdgeLoadTime;
	long averageReachabilityTime;
	long averagePatternMatchingTime;
	long averageNodeUpdateTime;
	long averageEdgeDeleteTime;
	long averageNodeDeleteTime;
	
	void generateAverages(int numTests)
	{
		averageVertexLoadTime = totalVertexLoadTime / numTests;
		averageEdgeLoadTime = totalEdgeLoadTime / numTests;
		averageReachabilityTime = totalReachabilityTime / numTests;
		averagePatternMatchingTime = totalPatternMatchingTime / numTests;
		averageNodeUpdateTime = totalNodeUpdateTime / numTests;
		averageEdgeDeleteTime = totalEdgeDeleteTime / numTests;
		averageNodeDeleteTime = totalNodeDeleteTime / numTests;
	}
}
