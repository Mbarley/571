package CPSC571.driver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.gremlin.java.GremlinPipeline;

import CPSC571.driver.Neo4j.ConcurrentHandler;

public class OrientDB implements LoadableDatabase
{
	private static String DBPath = "plocal:C:/temp/graph/db";
	private OrientGraph orientDB;
	private HashMap<String, Vertex> vertexMap;

	public void start(){
		orientDB = new OrientGraph(DBPath, "admin", "admin");
		orientDB.drop();
		orientDB = new OrientGraph(DBPath, "admin", "admin");
		orientDB.createVertexType("page");
		orientDB.createEdgeType("link");
		vertexMap = new HashMap<String, Vertex>();
	}
	public void stop() {
		if (orientDB != null) {
			orientDB.shutdown();
		}
	}
	
	/**
	 * Base untimed functions
	 * used by the testing functions to do atomic tasks
	 */
	//load node function
	private void loadNode(DataNode node){
		Vertex vertex = orientDB.addVertex("class:page");
		vertex.setProperty("name", node.getNodeName());
		vertexMap.put(node.getNodeName(), vertex);
		//orientDB.commit();
		//System.out.println("Committed Vertex: " + vertex.getProperty("name"));
	}
	//load edge function
	private void loadEdge(DataEdge dataEdge){
		Vertex fromVertex = vertexMap.get(dataEdge.getFromNode());
		Vertex toVertex = vertexMap.get(dataEdge.getToNode());
		
		if(fromVertex != null && toVertex != null)
			orientDB.addEdge("class:link", fromVertex, toVertex, "link");
		//orientDB.commit();
		//System.out.println("Committed " + fromVertex.getId() + "->" + toVertex.getId());
	}
	//Find reachable function
	private void findReachable(Vertex v, HashSet<Vertex> vSet) {
		Iterable<Vertex> vertices = v.getVertices(Direction.OUT, "link");
		Iterator<Vertex> i = vertices.iterator();

		while (i.hasNext()) {
			Vertex adj = (Vertex)i.next();
			if (!vSet.contains(adj)) {
				vSet.add(adj);
				findReachable(adj, vSet);				
			}
		}
	}
	//update node function
	private void updateNode(Vertex vertex){
		vertex.setProperty("Update", "Update");
	}
	//delete edge function
	private void deleteEdge(Edge edge){
		orientDB.removeEdge(edge);
	}
	//delete node function
	private void deleteNode(Vertex vertex){
		orientDB.removeVertex(vertex);
	}
	
	
	
	/**
	 * Here start the regular time testing functions
	 */
	//load all nodes
	public long loadNodes(Collection<DataNode> nodeCollection)
	{
		long startTime = System.currentTimeMillis();
		
		for(DataNode node : nodeCollection)
		{
			loadNode(node);
		}
		
		long endTime = System.currentTimeMillis();
		long loadTime = endTime - startTime;
		
		//System.out.println("Vertex Time: " + (vertexLoadEndTime - vertexLoadStartTime));
		return loadTime;
	}
	//load all edges
	public long loadEdges(Collection<DataEdge> edgeCollection)
	{
		long startTime = System.currentTimeMillis();
		
		for(DataEdge dataEdge : edgeCollection)
		{
			loadEdge(dataEdge);
		}
		
		long endTime = System.currentTimeMillis();
		long loadTime = endTime - startTime;
		
		return loadTime;
	}
	//time reachability testing
	public long testReachability(String startVertex) {
		
		long reachabilityStartTime = System.currentTimeMillis();

		Vertex v = orientDB.getVertex(vertexMap.get(startVertex));
		
		//Find all reachable vertices
		HashSet<Vertex> vSet = new HashSet<Vertex>();
		findReachable(v, vSet);	
		
		long reachabilityTime = System.currentTimeMillis() - reachabilityStartTime;
		return reachabilityTime;
	}
	//time pattern matching tests
	public long testPatternMatching() {
		
		long startTime = System.currentTimeMillis();

		GremlinPipeline g = new GremlinPipeline(orientDB.getVertices());
		g.V().as("x").out("link").as("z").out("link").as("y").out("link").as("x").in("link").as("z").in("link").as("y").in("link").as("x");	
        
		long patternMatchingTime = System.currentTimeMillis() - startTime;
		return patternMatchingTime;
	}
	//time updating nodes function
	public long updateNodes()
	{
		long startTime = System.currentTimeMillis();
		
		for(Vertex vertex : orientDB.getVertices())
		{
			updateNode(vertex);
		}
		
		long endTime = System.currentTimeMillis();
		
		return endTime - startTime;
	}
	//delete nodes test
	public long deleteNodes()
	{
		long startTime = System.currentTimeMillis();
		
		for(Vertex vertex : orientDB.getVertices())
		{
			deleteNode(vertex);
		}
		
		long endTime = System.currentTimeMillis();
		
		return endTime - startTime;
	}
	//delete edges test
	public long deleteEdges()
	{
		long startTime = System.currentTimeMillis();
		
		for(Edge edge : orientDB.getEdges())
		{
			deleteEdge(edge);
		}
		
		long endTime = System.currentTimeMillis();
		
		return endTime - startTime;
	}
	/**
	 * Concurrency tests
	 */
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
	//update all nodes concurrently function
	public long concurrentUpdateNodes(int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		
		//start ze timer
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the updates on threads
		for(Vertex vertex : orientDB.getVertices())
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
	public long concurrentDeleteEdges(int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		//start ze timer
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the transactions on threads
		for(Edge edge : orientDB.getEdges()){		
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
	public long concurrentDeleteNodes(int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		//start ze timer
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the transactions on threads
		for(Vertex vertex : orientDB.getVertices()){		
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
	public String toString() {
		return "OrientDB";
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
