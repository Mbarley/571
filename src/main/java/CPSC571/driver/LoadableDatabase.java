package CPSC571.driver;

import java.util.Collection;

public interface LoadableDatabase
{
	public void reset();
	public void start();
	public void loadNodes(Collection<DataNode> nodeCollection);
	public void loadEdges(Collection<DataEdge> edgeCollection);
	public long getVertexLoadTime();
	public long getEdgeLoadTime();
}
