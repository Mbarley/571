package CPSC571.driver;

public class DataEdge
{
	private String fromNode;
	private String toNode;
	
	public DataEdge(String from, String to)
	{
		fromNode = from;
		toNode = to;
	}
	
	public String getToNode()
	{
		return toNode;
	}
	public void setToNode(String toNode)
	{
		this.toNode = toNode;
	}
	public String getFromNode()
	{
		return fromNode;
	}
	public void setFromNode(String fromNode)
	{
		this.fromNode = fromNode;
	}
	
	

}
