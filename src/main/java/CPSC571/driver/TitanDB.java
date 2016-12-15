package CPSC571.driver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.tinkerpop.gremlin.java.GremlinPipeline;

import CPSC571.driver.OrientDB.ConcurrentHandler;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TitanDB implements LoadableDatabase
{
	private static String DBPath = "plocal:C:/temp/titan/db";
	private TitanGraph titanDB;
	private HashMap<String, Vertex> vertexMap;
	
	public static void main(String args[])
	{
		TitanDB db = new TitanDB();
		db.start();
		db.stop();
	}
	
	public void start()
	{
        BaseConfiguration configuration = new BaseConfiguration();

        configuration.setProperty("storage.backend", "cassandrathrift");
        configuration.setProperty("storage.hostname", "localhost");
		titanDB = TitanFactory.open(configuration); 
		vertexMap = new HashMap<String, Vertex>();
	}

	public void stop() 
	{
		try
		{
			titanDB.close();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
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
	public void loadNode(DataNode node){
		Vertex vertex = titanDB.addVertex("titan" + node.getNodeName());
		//System.out.println(vertex.label());
		vertexMap.put(node.getNodeName(), vertex);
		//orientDB.commit();
		//System.out.println("Committed Vertex: " + vertex.label());
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
	public void loadEdge(DataEdge dataEdge){
		Vertex fromVertex = vertexMap.get(dataEdge.getFromNode());
		Vertex toVertex = vertexMap.get(dataEdge.getToNode());
		
		if(fromVertex != null && toVertex != null)
			fromVertex.addEdge("name", toVertex);
		//orientDB.commit();
		//System.out.println("Committed " + fromVertex.label() + "->" + toVertex.label());
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
	public long testReachability(String startVertex) {
		
		long reachabilityStartTime = System.currentTimeMillis();

		Vertex v = titanDB.vertices(null).next();
			
		//Find all reachable vertices
		HashSet<Vertex> vSet = new HashSet<Vertex>();
		findReachable(v, vSet);	
		
		long reachabilityTime = System.currentTimeMillis() - reachabilityStartTime;
		return reachabilityTime;
	}
	
	private void findReachable(Vertex v, HashSet<Vertex> vSet) {
		Iterator<Edge> vertices = v.edges(Direction.OUT, "link");
		
		while (vertices.hasNext()) {
			Vertex adj = (Vertex)vertices.next();
			if (!vSet.contains(adj)) {
				vSet.add(adj);
				findReachable(adj, vSet);				
			}
		}
	}
	
	public String toString() {
		return "TitanDB";
	}

	public long testPatternMatching()
	{
		long startTime = System.currentTimeMillis();

		GremlinPipeline g = new GremlinPipeline(titanDB.vertices(null));
		g.V().as("x").out("link").as("z").out("link").as("y").out("link").as("x").in("link").as("z").in("link").as("y").in("link").as("x");	
        
		long patternMatchingTime = System.currentTimeMillis() - startTime;
		return patternMatchingTime;
	}

	public long updateNodes()
	{
		long startTime = System.currentTimeMillis();
		
		Iterator<Vertex> vertices = titanDB.vertices(null);
		
		while(vertices.hasNext())
		{
			Vertex vertex = vertices.next();
			updateNode(vertex);
		}
		
		long endTime = System.currentTimeMillis();
		long result = endTime - startTime;
		
		return result;
	}
	public void updateNode(Vertex vertex){
		vertex.property("Update", "Update");
	}
	public long concurrentUpdateNodes(int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		Iterator<Vertex> vertices = titanDB.vertices(null);
		//start ze timer
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the updates on threads
		while(vertices.hasNext()){
			pool.submit(new ConcurrentHandler(vertices.next()));
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

		Iterator<Edge> edges = titanDB.edges(null);
		
		while(edges.hasNext())
		{
			Edge edge = edges.next();
			deleteEdge(edge);
		}
		
		long endTime = System.currentTimeMillis();
		long result = endTime - startTime;
		
		return result;
	}
	public void deleteEdge(Edge edge){
		edge.remove();
	}
	public long concurrentDeleteEdges(int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		Iterator<Edge> edges = titanDB.edges(null);
		//start ze timer
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the transactions on threads
		while(edges.hasNext()){	
			pool.submit(new ConcurrentHandler(edges.next(),true, false));
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

		Iterator<Vertex> vertices = titanDB.vertices(null);
		
		while(vertices.hasNext())
		{
			Vertex vertex = vertices.next();
			deleteNode(vertex);
		}
		
		long endTime = System.currentTimeMillis();
		long result = endTime - startTime;
		
		return result;
	}
	public void deleteNode(Vertex vertex){
		vertex.remove();
	}
	public long concurrentDeleteNodes(int max_concurrent){
		//set up executor service pool 
		ExecutorService pool = Executors.newFixedThreadPool(max_concurrent);
		//start ze timer
		Iterator<Vertex> vertices = titanDB.vertices(null);
		long startTime = System.currentTimeMillis();
		long endTime = -1;
		//start running the transactions on threads
		while(vertices.hasNext()){		
			pool.submit(new ConcurrentHandler(vertices.next(),false, false));
		}
		
		//wait to finish
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
