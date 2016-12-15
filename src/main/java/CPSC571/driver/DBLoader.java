package CPSC571.driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map;

public class DBLoader
{
	private static ArrayList<Results> results;
	
	public static void main(String[] args)
	{
		LoadableDatabase[] databases = {new Neo4j(), new OrientDB()};
		DataLoader loader = loadDB(args[0]);
		int numTests;
		
		results = new ArrayList<Results>();
		
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
			Results dbResults = new Results(db.toString(), numTests);
			
			for(int i = 0; i < numTests; i++)
			{
				db.start();
				
				System.out.println("\n*** " + db + " ***");
				System.out.println("Run " + (i + 1) + "\t");
				
				long vertexLoadTime = db.loadNodes(loader.getNodeMap().values());
				dbResults.totalVertexLoadTime += vertexLoadTime;
				dbResults.vertexLoadTimes.add(vertexLoadTime);
				System.out.println("Vertex Load Time: " + vertexLoadTime);
				
				long edgeLoadTime = db.loadEdges(loader.getEdgeMap());
				dbResults.totalEdgeLoadTime += edgeLoadTime;
				dbResults.edgeLoadTimes.add(edgeLoadTime);
				System.out.println("Edge Load Time: " + edgeLoadTime);
				
				long reachabilityTime = db.testReachability("Yeast");
				dbResults.totalReachabilityTime += reachabilityTime;
				dbResults.reachabilityTimes.add(reachabilityTime);
				System.out.println("Reachability Time: " + reachabilityTime);
				
				long patternMatchingTime = db.testPatternMatching();
				dbResults.totalPatternMatchingTime += patternMatchingTime;
				dbResults.patternMatchingTimes.add(patternMatchingTime);
				System.out.println("Pattern Matching Time: " + patternMatchingTime);
				
				long nodeUpdateTime = db.updateNodes();
				dbResults.totalNodeUpdateTime += nodeUpdateTime;
				dbResults.nodeUpdateTimes.add(nodeUpdateTime);
				System.out.println("Node Update Time: " + nodeUpdateTime);
				
				long edgeDeleteTime = db.deleteEdges();
				dbResults.totalEdgeDeleteTime += edgeDeleteTime;
				dbResults.edgeDeleteTimes.add(edgeDeleteTime);
				System.out.println("Edge Delete Time: " + edgeDeleteTime);
				
				long nodeDeleteTime = db.deleteNodes();
				dbResults.totalNodeDeleteTime += nodeDeleteTime;
				dbResults.nodeDeleteTimes.add(nodeDeleteTime);
				System.out.println("Node Delete Time: " + nodeDeleteTime);
				
				db.stop();		
			}
			
			dbResults.generateAverages();
			dbResults.printAverages();
			results.add(dbResults);		
		}
		
		try
		{
			outputResults();
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
	}
	
	private static void outputResults() throws FileNotFoundException, UnsupportedEncodingException
	{
		PrintStream original = System.out;
		
		for(Results result : results)
		{
			System.setOut(new PrintStream(new File(result.DBName + ".csv"), "UTF-8"));
			System.out.print("Run Number, ");
			System.out.print("Vertex Load Time, ");
			System.out.print("Edge Load Time, ");
			System.out.print("Reachability Time, ");
			System.out.print("Pattern Matching Time, ");
			System.out.print("Node Update Time, ");
			System.out.print("Edge Delete Time, ");
			System.out.print("Node Delete Time");
			System.out.println();
			
			for(int i = 0; i < result.numTests; i++)
			{
				System.out.print("Run " + (i + 1) + "\t");
				System.out.print(result.vertexLoadTimes.get(i) + ", ");
				System.out.print(result.edgeLoadTimes.get(i) + ", ");
				System.out.print(result.reachabilityTimes.get(i) + ", ");
				System.out.print(result.patternMatchingTimes.get(i) + ", ");
				System.out.print(result.nodeUpdateTimes.get(i) + ", ");
				System.out.print(result.edgeDeleteTimes.get(i) + ", ");
				System.out.print(result.nodeDeleteTimes.get(i) + "\n");
			}
			System.out.print("Average\t");
			System.out.print(result.averageVertexLoadTime + ", ");
			System.out.print(result.averageEdgeLoadTime + ", ");
			System.out.print(result.averageReachabilityTime + ", ");
			System.out.print(result.averagePatternMatchingTime + ", ");
			System.out.print(result.averageNodeUpdateTime + ", ");
			System.out.print(result.averageEdgeDeleteTime + ", ");
			System.out.print(result.averageNodeDeleteTime + "\n");
		}
		
		System.setOut(original);
	}
	
	private static DataLoader loadDB(String inputFile) 
	{
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
	int numTests;
	
	public Results(String DBName, int numTests)
	{
		this.DBName = DBName;
		this.numTests = numTests;
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
	
	void generateAverages()
	{
		averageVertexLoadTime = totalVertexLoadTime / numTests;
		averageEdgeLoadTime = totalEdgeLoadTime / numTests;
		averageReachabilityTime = totalReachabilityTime / numTests;
		averagePatternMatchingTime = totalPatternMatchingTime / numTests;
		averageNodeUpdateTime = totalNodeUpdateTime / numTests;
		averageEdgeDeleteTime = totalEdgeDeleteTime / numTests;
		averageNodeDeleteTime = totalNodeDeleteTime / numTests;
	}
	
	void printAverages()
	{
		System.out.println("***" + DBName + "***");
		System.out.println("Average Vertex Load Time: " + averageVertexLoadTime);
		System.out.println("Average Edge Load Time: " + averageEdgeLoadTime);
		System.out.println("Average Reachability Time: " + averageReachabilityTime);
		System.out.println("Average Pattern Matching Time: " + averagePatternMatchingTime);
		System.out.println("Average Node Update Time: " + averageNodeUpdateTime);
		System.out.println("Average Edge Delete Time: " + averageEdgeDeleteTime);
		System.out.println("Average Node Delete Time: " + averageNodeDeleteTime);
		System.out.println("");
	}
}
