package CPSC571.driver;

import java.util.ArrayList;
import java.util.HashMap;

public class DataNode
{
	private String nodeName;
	private ArrayList<String> edgeList;
	private HashMap<String, String> properties;
	
	public DataNode(String name)
	{
		this.setNodeName(name);
		edgeList = new ArrayList<String>();
		properties = new HashMap<String, String>();
	}
	
	public void addEdge(String edge)
	{
		if(!edgeList.contains(edge))
		{
			edgeList.add(edge);
		}
	}

	public String getNodeName()
	{
		return nodeName;
	}

	public void setNodeName(String nodeName)
	{
		this.nodeName = nodeName;
	}

	public HashMap<String, String> getProperties()
	{
		return properties;
	}

	public void setProperties(HashMap<String, String> properties)
	{
		this.properties = properties;
	}

	public ArrayList<String> getEdgeList()
	{
		return edgeList;
	}

	public void setEdgeList(ArrayList<String> edgeList)
	{
		this.edgeList = edgeList;
	}
}
