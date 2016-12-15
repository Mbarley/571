package CPSC571.driver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j2.Neo4j2Graph;
import com.tinkerpop.gremlin.java.GremlinPipeline;

public class Neo4j implements LoadableDatabase
{
	static Neo4j2Graph neo4jDB;

	static String path = "/tmp/neo4j";
	
	TreeMap<String, Vertex> vertexMap;

	private static HashMap<String, Object> vMap;
		
	static Graph g;
	
	public void start()
	{
		neo4jDB = new Neo4j2Graph(path);
		vertexMap = new TreeMap<String, Vertex>();
		vMap = new HashMap<String, Object>();	
	}
	
	public void stop()
	{
		if (neo4jDB != null)
		{
			neo4jDB.shutdown();
		}
	}
	//load node function
	public void loadNode(DataNode node){
		Vertex vertex = neo4jDB.addVertex("page");
		vertex.setProperty("name", node.getNodeName());
		vertexMap.put(node.getNodeName(), vertex);
			
		vMap.put(node.getNodeName(), vertex.getId());
	}
	//load all nodes function
	public long loadNodes(Collection<DataNode> nodeCollection)
	{
		long startTime = System.currentTimeMillis();
		
		for(DataNode node : nodeCollection)
		{		
			loadNode(node);
		}
		
		long loadTime = System.currentTimeMillis() - startTime;
		return loadTime;
	}
	//load all nodes concurrently function
	public long concurrentLoadNodes(Collection<DataNode> nodeCollection, int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		//start ze timer
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the transactions on threads
		for(DataNode node : nodeCollection){		
			pool.submit(new ConcurrentHandler(node,false, true));
		}
		
		try {
			if(!pool.awaitTermination(180, TimeUnit.SECONDS)){
				//pool did not finish in three minutes
			    pool.shutdownNow(); // Cancel currently executing tasks
			    // Wait a while for tasks to respond to being cancelled
			    if(!pool.awaitTermination(60, TimeUnit.SECONDS)){
			        System.err.println("Pool did not terminate");
			    }
			}else{
				endTime = System.currentTimeMillis();
			}
		} catch (InterruptedException e) {
			//Recieved an exception awaiting thread termination 
			e.printStackTrace();
		}
		
		return endTime - startTime;
	}
	//load edge function
	public void loadEdge(DataEdge edge){
		Vertex from = vertexMap.get(edge.getFromNode());
		Vertex to = vertexMap.get(edge.getToNode());
		
		neo4jDB.addEdge(null, from, to, "link");
	}
	//load all edges function
	public long loadEdges(Collection<DataEdge> edgeCollection)
	{
		long startTime = System.currentTimeMillis();
		
		for(DataEdge edge : edgeCollection)
		{
			loadEdge(edge);
		}
		
		long loadTime = System.currentTimeMillis() - startTime;
		return loadTime;
	}
	//load all edges concurrently function
	public long concurrentLoadEdges(Collection<DataEdge> edgeCollection, int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		//start ze timer
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the transactions on threads
		for(DataEdge edge : edgeCollection){		
			pool.submit(new ConcurrentHandler(edge,true, true));
		}
		
		try {
			if(!pool.awaitTermination(180, TimeUnit.SECONDS)){
				//pool did not finish in three minutes
			    pool.shutdownNow(); // Cancel currently executing tasks
			    // Wait a while for tasks to respond to being cancelled
			    if(!pool.awaitTermination(60, TimeUnit.SECONDS)){
			        System.err.println("Pool did not terminate");
			    }
			}else{
				endTime = System.currentTimeMillis();
			}
		} catch (InterruptedException e) {
			//Recieved an exception awaiting thread termination 
			e.printStackTrace();
		}
		
		return endTime - startTime;
	}
	public long testReachability(String startVertex)
	{
		long startTime = System.currentTimeMillis();

		Vertex v = neo4jDB.getVertex(vMap.get(startVertex));
		
		//Find all reachable vertices
		HashSet<Vertex> vSet = new HashSet<Vertex>();
		findReachable(v, vSet);	
		
		long reachabilityTime = System.currentTimeMillis() - startTime;
		return reachabilityTime;
	}

	private void findReachable(Vertex v, HashSet<Vertex> vSet)
	{
		for (Vertex adj : v.getVertices(Direction.OUT, "link"))
		{
			if (!vSet.contains(adj))
			{
				vSet.add(adj);
				findReachable(adj, vSet);				
			}
		}
	}
	
	public long testPatternMatching()
	{
		long startTime = System.currentTimeMillis();

		GremlinPipeline g = new GremlinPipeline(neo4jDB.getVertices());
		g.V().as("x").out("link").as("z").out("link").as("y").out("link").as("x").in("link").as("z").in("link").as("y").in("link").as("x");	
        
		long patternMatchingTime = System.currentTimeMillis() - startTime;
		return patternMatchingTime;
	}
	//update node function
	public void updateNode(Vertex vertex){
		vertex.setProperty("Update", "Update");
	}
	//update all nodes function
	public long updateNodes()
	{
		long startTime = System.currentTimeMillis();
		
		for(Vertex vertex : neo4jDB.getVertices())
		{
			updateNode(vertex);
		}
		
		long endTime = System.currentTimeMillis();
		
		return endTime - startTime;
	}
	//update all nodes concurrently function
	public long concurrentUpdateNodes(int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		
		//start ze timer
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the updates on threads
		for(Vertex vertex : neo4jDB.getVertices())
		{
			pool.submit(new ConcurrentHandler(vertex));
		}
			
		try {
			if(!pool.awaitTermination(180, TimeUnit.SECONDS)){
				//pool did not finish in three minutes
			    pool.shutdownNow(); // Cancel currently executing tasks
			    // Wait a while for tasks to respond to being cancelled
			    if(!pool.awaitTermination(60, TimeUnit.SECONDS)){
			        System.err.println("Pool did not terminate");
			    }
			}else{
				endTime = System.currentTimeMillis();
			}
		} catch (InterruptedException e) {
			//Recieved an exception awaiting thread termination 
			e.printStackTrace();
		}
		
		return endTime - startTime;
	}

	 
	 
	public long deleteEdges()
	{
		long startTime = System.currentTimeMillis();
		
		for(Edge edge : neo4jDB.getEdges())
		{
			deleteEdge(edge);
		}
		
		long endTime = System.currentTimeMillis();
		
		return endTime - startTime;
	}
	public void deleteEdge(Edge edge){
		neo4jDB.removeEdge(edge);
	}
	public long concurrentDeleteEdges(int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		//start ze timer
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the transactions on threads
		for(Edge edge : neo4jDB.getEdges()){		
			pool.submit(new ConcurrentHandler(edge,true, false));
		}
		
		try {
			if(!pool.awaitTermination(180, TimeUnit.SECONDS)){
				//pool did not finish in three minutes
			    pool.shutdownNow(); // Cancel currently executing tasks
			    // Wait a while for tasks to respond to being cancelled
			    if(!pool.awaitTermination(60, TimeUnit.SECONDS)){
			        System.err.println("Pool did not terminate");
			    }
			}else{
				endTime = System.currentTimeMillis();
			}
		} catch (InterruptedException e) {
			//Recieved an exception awaiting thread termination 
			e.printStackTrace();
		}
		
		return endTime - startTime;
	}
	public long deleteNodes()
	{
		long startTime = System.currentTimeMillis();
		
		for(Vertex vertex : neo4jDB.getVertices())
		{
			deleteNode(vertex);
		}
		
		long endTime = System.currentTimeMillis();
		
		return endTime - startTime;
	}	
	public void deleteNode(Vertex vertex){
		neo4jDB.removeVertex(vertex);
	}
	public long concurrentDeleteNodes(int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		//start ze timer
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the transactions on threads
		for(Vertex vertex : neo4jDB.getVertices()){		
			pool.submit(new ConcurrentHandler(vertex,false, false));
		}
		
		try {
			if(!pool.awaitTermination(180, TimeUnit.SECONDS)){
				//pool did not finish in three minutes
			    pool.shutdownNow(); // Cancel currently executing tasks
			    // Wait a while for tasks to respond to being cancelled
			    if(!pool.awaitTermination(60, TimeUnit.SECONDS)){
			        System.err.println("Pool did not terminate");
			    }
			}else{
				endTime = System.currentTimeMillis();
			}
		} catch (InterruptedException e) {
			//Recieved an exception awaiting thread termination 
			e.printStackTrace();
		}
		
		return endTime - startTime;
	}
	public String toString()
	{
		return "Neo4j";
	}
	//concurrency handler class; instantiated with a method;
	class ConcurrentHandler implements Runnable {
		//constructor should take method being run by the thread
		
		//data we are ading deleteing
		private Object data;
		//data we are updating
		
		private int type;
		private boolean adding;
		
		private static final int NODE = -1;
		private static final int EDGE = 1;
		private static final int UPDATE = 0;
		
		//CONSTRUCTOR FOR ADDING AND REMOVING DATA
		public ConcurrentHandler(Object data, boolean isEdge, boolean adding){
			this.data = data;
			if(isEdge){
				type = EDGE;
			}else{
				type = NODE;
			}
			this.adding = adding;
		}
		
		//CONSTRUCTOR FOR UPDATING NODES
		public ConcurrentHandler(Object data){
			type = UPDATE;
			this.data = data;
		}
		
		//run method
		//this vs the guy she tells you about
		public void run() {
			if(type == UPDATE){
				updateNode((Vertex) data);
			}else if(type == EDGE){
				if(adding){
					loadEdge((DataEdge) data);
				}else{
					deleteEdge((Edge) data);
				}
			}else if(type == NODE){
				if(adding){
					loadNode((DataNode) data);
				}else{
					deleteNode((Vertex) data);
				}
			}
		}
	}
}
