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
	private final static int CONCURRENT_MAX = 4;
	
	public static void main(String[] args)
	{
		LoadableDatabase[] databases = {new TitanDB(), new Neo4j(), new OrientDB()};
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
				
				//regular tests 
				
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
				
				
				//test concurrency stuff
				System.out.println("Concurrency tests\n");
				long concurrentVertexLoadTime = db.concurrentLoadNodes(loader.getNodeMap().values(),CONCURRENT_MAX);
				dbResults.totalConcurrentVertexLoadTime += concurrentVertexLoadTime;
				dbResults.vertexLoadTimes.add(concurrentVertexLoadTime);
				System.out.println("Vertex Load Time: " + concurrentVertexLoadTime);
				
				long concurrentEdgeLoadTime = db.concurrentLoadEdges(loader.getEdgeMap(),CONCURRENT_MAX);
				dbResults.totalConcurrentEdgeLoadTime += concurrentEdgeLoadTime;
				dbResults.edgeLoadTimes.add(concurrentEdgeLoadTime);
				System.out.println("Edge Load Time: " + concurrentEdgeLoadTime);	
				
				long concurrentNodeUpdateTime = db.concurrentUpdateNodes(CONCURRENT_MAX);
				dbResults.totalConcurrentNodeUpdateTime += concurrentNodeUpdateTime;
				dbResults.concurrentNodeUpdateTimes.add(concurrentNodeUpdateTime);
				System.out.println("ConcurrentUpdate Time: " + concurrentNodeUpdateTime);
				
				long concurrentEdgeDeleteTime = db.concurrentDeleteEdges(CONCURRENT_MAX);
				dbResults.totalConcurrentEdgeDeleteTime += concurrentEdgeDeleteTime;
				dbResults.concurrentEdgeDeleteTimes.add(concurrentEdgeDeleteTime);
				System.out.println("Concurrent Edge Delete Time: " + concurrentEdgeDeleteTime);
				
				long concurrentNodeDeleteTime = db.concurrentDeleteNodes(CONCURRENT_MAX);
				dbResults.totalConcurrentNodeDeleteTime += concurrentNodeDeleteTime;
				dbResults.concurrentNodeDeleteTimes.add(concurrentNodeDeleteTime);
				System.out.println("Concurrent Node Delete Time: " + concurrentNodeDeleteTime);
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
			System.out.print("Concurrent Vertex Load Time, ");
			System.out.print("Edge Load Time, ");
			System.out.print("Concurrent Edge Load Time, ");
			System.out.print("Reachability Time, ");
			System.out.print("Pattern Matching Time, ");
			System.out.print("Node Update Time, ");
			System.out.print("Concurrent Node Update Time, ");
			System.out.print("Edge Delete Time, ");
			System.out.print("Concurrent Edge Delete Time, ");
			System.out.print("Node Delete Time");
			System.out.print("Concurrent Node Delete Time, ");
			System.out.println();
			
			for(int i = 0; i < result.numTests; i++)
			{
				System.out.print("Run " + (i + 1) + ",");
				System.out.print(result.vertexLoadTimes.get(i) + ", ");
				System.out.print(result.concurrentVertexLoadTimes.get(i) + ", ");
				System.out.print(result.edgeLoadTimes.get(i) + ", ");
				System.out.print(result.concurrentEdgeLoadTimes.get(i) + ", ");
				System.out.print(result.reachabilityTimes.get(i) + ", ");
				System.out.print(result.patternMatchingTimes.get(i) + ", ");
				System.out.print(result.nodeUpdateTimes.get(i) + ", ");
				System.out.print(result.concurrentNodeUpdateTimes.get(i)+ ", ");
				System.out.print(result.edgeDeleteTimes.get(i) + ", ");
				System.out.print(result.concurrentEdgeDeleteTimes.get(i) + ", ");
				System.out.print(result.nodeDeleteTimes.get(i) + ", ");
				System.out.print(result.concurrentNodeDeleteTimes.get(i) + "\n");;
			}
			System.out.print("Average\t");
			System.out.print(result.averageVertexLoadTime + ", ");
			System.out.print(result.averageConcurrentVertexLoadTime + ", ");
			System.out.print(result.averageEdgeLoadTime + ", ");
			System.out.print(result.averageConcurrentEdgeLoadTime + ", ");
			System.out.print(result.averageReachabilityTime + ", ");
			System.out.print(result.averagePatternMatchingTime + ", ");
			System.out.print(result.averageNodeUpdateTime + ", ");
			System.out.print(result.averageConcurrentNodeUpdateTime + ", ");
			System.out.print(result.averageEdgeDeleteTime + ", ");
			System.out.print(result.averageConcurrentEdgeDeleteTime + ", ");
			System.out.print(result.averageNodeDeleteTime + ", ");
			System.out.print(result.averageConcurrentNodeDeleteTime + "\n");
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
		concurrentVertexLoadTimes = new ArrayList<Long>();
		concurrentEdgeLoadTimes = new ArrayList<Long>();
		concurrentNodeUpdateTimes = new ArrayList<Long>();
		concurrentNodeDeleteTimes = new ArrayList<Long>();
		concurrentEdgeDeleteTimes = new ArrayList<Long>();
	}
	
	ArrayList<Long> vertexLoadTimes;
	ArrayList<Long> concurrentVertexLoadTimes;
	ArrayList<Long> edgeLoadTimes;
	ArrayList<Long> concurrentEdgeLoadTimes;
	ArrayList<Long> reachabilityTimes;
	ArrayList<Long> patternMatchingTimes;
	ArrayList<Long> nodeUpdateTimes;
	ArrayList<Long> concurrentNodeUpdateTimes;
	ArrayList<Long> edgeDeleteTimes;
	ArrayList<Long> nodeDeleteTimes;
	ArrayList<Long> concurrentEdgeDeleteTimes;
	ArrayList<Long> concurrentNodeDeleteTimes;
	//running totals
	long totalVertexLoadTime;
	long totalConcurrentVertexLoadTime;
	long totalEdgeLoadTime;
	long totalConcurrentEdgeLoadTime;
	long totalReachabilityTime;
	long totalPatternMatchingTime;
	long totalNodeUpdateTime;
	long totalConcurrentNodeUpdateTime;
	long totalEdgeDeleteTime;
	long totalConcurrentEdgeDeleteTime;
	long totalConcurrentNodeDeleteTime;
	long totalNodeDeleteTime;
	//running averages
	long averageVertexLoadTime;
	long averageConcurrentVertexLoadTime;
	long averageEdgeLoadTime;
	long averageConcurrentEdgeLoadTime;
	long averageReachabilityTime;
	long averagePatternMatchingTime;
	long averageNodeUpdateTime;
	long averageConcurrentNodeUpdateTime;
	long averageEdgeDeleteTime;
	long averageConcurrentEdgeDeleteTime;
	long averageConcurrentNodeDeleteTime;
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
		averageConcurrentNodeUpdateTime = totalConcurrentNodeUpdateTime / numTests;
		averageConcurrentEdgeLoadTime =totalConcurrentEdgeLoadTime / numTests;
		averageConcurrentVertexLoadTime = totalConcurrentVertexLoadTime / numTests;
		averageConcurrentNodeDeleteTime = totalConcurrentNodeDeleteTime / numTests;
		averageConcurrentEdgeDeleteTime = totalConcurrentEdgeDeleteTime / numTests;
	}
	
	void printAverages()
	{
		System.out.println("***" + DBName + "***");
		System.out.println("Average Vertex Load Time: " + averageVertexLoadTime);
		System.out.println("Average Concurrent Vertex Load Time: " + averageConcurrentVertexLoadTime);
		System.out.println("Average Edge Load Time: " + averageEdgeLoadTime);
		System.out.println("Average Concurrent Edge Load Time: " + averageConcurrentEdgeLoadTime);
		System.out.println("Average Reachability Time: " + averageReachabilityTime);
		System.out.println("Average Pattern Matching Time: " + averagePatternMatchingTime);
		System.out.println("Average Node Update Time: " + averageNodeUpdateTime);
		System.out.println("Average Concurrent Node Update Time: " + averageConcurrentNodeUpdateTime);
		System.out.println("Average Edge Delete Time: " + averageEdgeDeleteTime);
		System.out.println("Average Concurrent Edge Delete Time: " + averageConcurrentEdgeDeleteTime);
		System.out.println("Average Node Delete Time: " + averageNodeDeleteTime);
		System.out.println("Average Concurrent Node Delete Time: " + averageConcurrentNodeDeleteTime);
		System.out.println("");
	}
}
