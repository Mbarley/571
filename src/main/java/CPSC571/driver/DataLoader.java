package CPSC571.driver;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class DataLoader
{
	
	private File inputFile;
	private BufferedReader inputReader;
	private TreeMap<String, DataNode> nodeMap;
	private HashSet<DataEdge> edgeMap;
	private boolean directed;
	
	public static void main(String args[])
	{
		if(args.length < 1)
		{
			System.exit(1);
		}
		DataLoader loader = new DataLoader();
		
		try
		{
			loader.setInputFile(args[0]);
		} 
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
			System.exit(2);
		} 
		catch (UnsupportedEncodingException e)
		{
			e.printStackTrace();
			System.exit(3);
		}
		
		if(args.length >= 2)
		{
			if(args[1].equals("directed".toLowerCase()) || args[1].equals("true".toLowerCase()))
			{
				loader.directed = true;
			}
			else
			{
				loader.directed = false;
			}
		}
		else
		{
			loader.directed = false;
		}
		
		try
		{
			loader.readFile();
		} 
		catch (IOException e)
		{
			e.printStackTrace();
			System.exit(4);
		}
		System.out.println("Node Map Size: " + loader.nodeMap.size());
		
	}
	

	public DataLoader()
	{
		nodeMap = new TreeMap<String, DataNode>();
		edgeMap = new HashSet<DataEdge>();
	}

	public void setInputFile(String fileName) throws FileNotFoundException, UnsupportedEncodingException
	{
		this.inputFile = new File(fileName);
		inputReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"));
	}
	
	public void readFile() throws IOException
	{
		String line;
		String[] parts;
		DataNode fromNode = null;
		DataNode toNode = null;
		
		while((line = inputReader.readLine()) != null)
		{
			line = line.trim();
			parts = line.split("\t");
			
			//ignore lines that do not conform to a two-part structure
			if(parts.length != 2)
			{	
				continue;
			}
			
			fromNode = getOrCreateNode(parts[0]);
			toNode = getOrCreateNode(parts[1]);
			
			fromNode.addEdge(toNode.getNodeName());
			//System.out.println("Edge created: " + fromNode.getNodeName() + " -> " + toNode.getNodeName());
			getEdgeMap().add(new DataEdge(fromNode.getNodeName(), toNode.getNodeName()));
			
			if(!directed)
			{
				toNode.addEdge(fromNode.getNodeName());
				getEdgeMap().add(new DataEdge(toNode.getNodeName(), fromNode.getNodeName()));
				//System.out.println("Edge created: " + toNode.getNodeName() + " -> " + fromNode.getNodeName());
			}
			
			nodeMap.put(fromNode.getNodeName(), fromNode);
			nodeMap.put(toNode.getNodeName(), toNode);
			
		}
	}

	private DataNode getOrCreateNode(String nodeName)
	{
		DataNode node;
		if(nodeMap.containsKey(nodeName))
		{
			node = nodeMap.get(nodeName);	
		}
		else
		{
			node = new DataNode(nodeName);
			//System.out.println("Node created: " + node.getNodeName()); //test
		}
		
		return node;
	}


	public TreeMap<String, DataNode> getNodeMap()
	{
		return nodeMap;
	}


	public void setNodeMap(TreeMap<String, DataNode> nodeMap)
	{
		this.nodeMap = nodeMap;
	}


	public HashSet<DataEdge> getEdgeMap()
	{
		return edgeMap;
	}


	public void setEdgeMap(HashSet<DataEdge> edgeMap)
	{
		this.edgeMap = edgeMap;
	}
}
