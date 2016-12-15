package CPSC571.driver;

import java.util.Collection;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public interface LoadableDatabase
{
	

	//start and stop/utility
	public void start();
	public void stop();
	public String toString();
	
	
	//tests
	public long loadEdges(Collection<DataEdge> edgeCollection);
	public long loadNodes(Collection<DataNode> nodeCollection);
	public long updateNodes();
	public long deleteEdges();
	public long deleteNodes();

	//other tests
	public long testReachability(String startVertex);
	public long testPatternMatching();
	
	//concurrent method tests
	public long concurrentUpdateNodes(int max_concurrent);
	public long concurrentLoadEdges(Collection<DataEdge> edgeCollection, int max_concurrent);
	public long concurrentLoadNodes(Collection<DataNode> nodeCollection, int max_concurrent);
	public long concurrentDeleteEdges(int max_concurrent);
	public long concurrentDeleteNodes(int max_concurrent);

}
