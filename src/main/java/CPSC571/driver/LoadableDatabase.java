package CPSC571.driver;

import java.util.Collection;

public interface LoadableDatabase
{
	public void start();
	public long loadNodes(Collection<DataNode> nodeCollection);
	public long loadEdges(Collection<DataEdge> edgeCollection);
	public long testReachability(String startVertex);
	public void stop();
	public String toString();
}
