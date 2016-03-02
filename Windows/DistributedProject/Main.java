package DistributedProject;
	import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;
//import java.util.concurrent.TimeUnit;
	
public class Main {
	
	private int socketNum;
	private int leaderSocket;
	private int repSize=3;
	private String strategy="linear"; //"linear" or "lazy"
	private ArrayList<Triple<Process, Integer,Integer>> proc; //list with the PID,ID,Socket for each process
	
	public Main(int startsocket)
	{
		socketNum=startsocket;
		leaderSocket=startsocket+1;
		proc= new ArrayList<Triple<Process, Integer,Integer>>();
	}
		
	public static void main(String args[]) throws IOException, NoSuchAlgorithmException, InterruptedException 
	{   
		Main network = new Main(40000);
		ProcessBuilder pb = new ProcessBuilder("java.exe","-cp","bin","DistributedProject.Node",""+network.leaderSocket,"1",""+network.socketNum,""+network.leaderSocket,""+network.repSize,network.strategy);
		pb.inheritIO(); 		// inherit IO to see the output of other programs 
	    Process p = pb.start();
	    network.proc.add(new Triple<Process,Integer,Integer>(p,1,network.leaderSocket));

	    //create the network with 10 nodes
	    for(int i=2; i<=10; i++)
	    {
	    	network.join(i);	
	    }
	    //waits for user input via keyboard
	    network.listen();			
	}

	private void listen() throws IOException
	{
        	while(true)
        	{
        		BufferedReader stdIn =new BufferedReader(new InputStreamReader(System.in));
        		String fromUser = stdIn.readLine();
        		if (fromUser != null) 
        		{
        			String[] parts =fromUser.split(","); 
        			new UserClientThread(parts, this).start();
        		}
        	}   
	}
	
	private void waitResponse() throws IOException
	{
		ServerSocket serverSocket = new ServerSocket(this.socketNum);
	    Socket clientSocket = serverSocket.accept();
	    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
	    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		String confirmation;
		while ((confirmation = in.readLine()) != null) 
		{
			String[] parts =confirmation.split(","); 
            if (parts[0].equals("donequery"))
            {
	           	 parts[1]=parts[1].replaceAll("\\\\", "\n");
	           	 System.out.println(parts[1].substring(1));
	           	 break;
            }
            else if (parts[0].equals("donejoin"))
            {
            	//System.out.println(parts[1]);
            	break;
            }
            else if (parts[0].equals("doneinsert"))
            {
            	System.out.println(parts[1]);
            	break;
            }
            else if (parts[0].equals("donedelete"))
            {
            	System.out.println(parts[1]);
            	break;
            }
            else if (parts[0].equals("donedepart"))
            {
            	//System.out.println(parts[1]);
            	break;
            }
            else if (parts[0].equals("doneprint"))
            {
            	System.out.println(parts[1]);
            	break;
            }
            else 
            	break;
		}
		out.println("OK");
		serverSocket.close();
	}
	
	public void print() throws UnknownHostException, IOException 
	{
		Socket kkSocket = new Socket("localhost", this.leaderSocket);
        PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
		BufferedReader in = new BufferedReader(new InputStreamReader(kkSocket.getInputStream()));
		
		String confirm;
		out.println("print" + "," + this.leaderSocket + "," + "List:");
		while ((confirm = in.readLine()) != null) 
	    {
			//request received from node
	    	if (confirm.equals("OK"))
            {
                break;
            }
        }
		
		kkSocket.close();
		
		this.waitResponse();	
	}
	
	public void join(int id) throws NoSuchAlgorithmException, IOException
	{
		int nodeSocket = id+this.socketNum;
		ProcessBuilder pb = new ProcessBuilder("java.exe","-cp","bin","DistributedProject.Node", ""+nodeSocket ,""+id,""+this.socketNum,""+this.leaderSocket,""+this.repSize,this.strategy);
		pb.inheritIO();
        Process p = pb.start();
        this.proc.add(new Triple<Process, Integer,Integer>(p,id,nodeSocket));
        Socket kkSocket = new Socket("localhost", this.leaderSocket);
        PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(kkSocket.getInputStream()));
        String confirm;
        out.println("join,"+ id + "," + nodeSocket);        
		while ((confirm = in.readLine()) != null) 
	    {
			//request received from node
	    	if (confirm.equals("OK"))
            {
                break;
            }
        }
		kkSocket.close();
		
		this.waitResponse();
	}
	
	public void depart(int id) throws NoSuchAlgorithmException, IOException
	{
		//check if this id exists
		Iterator<Triple<Process,Integer,Integer>> myIt= this.proc.iterator();
		Triple<Process,Integer,Integer> obj=null,temp=null;
		while (myIt.hasNext()) 
		{
			temp = myIt.next();
			if (temp.getId() == id)
			{
				obj=temp;
				break;
			}
	    }		
		if (obj!=null){ 
			//connect and remove the node from the network (update keyranges and prev/next fields)
			Socket kkSocket = new Socket("localhost", this.leaderSocket);
	        PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);

			out.println("depart,"+ id);
			BufferedReader in = new BufferedReader(new InputStreamReader(kkSocket.getInputStream()));
			String confirm;
			while ((confirm = in.readLine()) != null) 
		    {
				//request received from node
		    	if (confirm.equals("OK"))
	            {
	                break;
	            }
	        }
			kkSocket.close();
			
			this.waitResponse();
			//kill it and remove it from array proc
			obj.getPid().destroy();
			this.proc.remove(obj);
		}
	}
	
	public void insert(String key, int value) throws UnknownHostException, IOException, NoSuchAlgorithmException
	{
		Random rand = new Random();
		int index= rand.nextInt(this.proc.size());
		int socket= this.proc.get(index).getSocket();
		Socket kkSocket = new Socket("localhost", socket);
        PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
		out.println("insert,"+ key + ","+ value +","+socket +"," +"Main");
		
		BufferedReader in = new BufferedReader(new InputStreamReader(kkSocket.getInputStream()));
		String confirm;
		while ((confirm = in.readLine()) != null) 
	    {
	    	//request received from node
	    	if (confirm.equals("OK"))
            {
                break;
            }
        }
		kkSocket.close();
		this.waitResponse();
	}
	
	public void delete(String key) throws UnknownHostException, IOException
	{
		Random rand = new Random();
		int index= rand.nextInt(this.proc.size());
		int socket= this.proc.get(index).getSocket();
		Socket kkSocket = new Socket("localhost", socket);
        PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
		BufferedReader in = new BufferedReader(new InputStreamReader(kkSocket.getInputStream()));
		String confirm;
		
		out.println("delete,"+ key + "," + socket);	

		while ((confirm = in.readLine()) != null) 
	    {
			//request received from node
	    	if (confirm.equals("OK"))
            {
                break;
            }
        }
		kkSocket.close();
		this.waitResponse();

	}
	
	public void query(String key) throws UnknownHostException, IOException
	{
		Random rand = new Random();
		int index= rand.nextInt(this.proc.size());
		int socket= this.proc.get(index).getSocket();
		Socket kkSocket = new Socket("localhost", socket);
        PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
		BufferedReader in = new BufferedReader(new InputStreamReader(kkSocket.getInputStream()));
		String confirm;
		
		out.println("query,"+ key +","+ socket + "," + " ");		

		while ((confirm = in.readLine()) != null) 
	    {
			//request received from node
	    	if (confirm.equals("OK"))
            {
                break;
            }
        }
		kkSocket.close();
		this.waitResponse();
	}
	
	public void insertFile(String filename) throws IOException, NoSuchAlgorithmException
	{
		//open File
		FileInputStream fstream = new FileInputStream(filename);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		String strLine;
		//long startTime = System.nanoTime();
		//Read File Line By Line
		while ((strLine = br.readLine()) != null)   
		{
		  //1)split the line on "," 2)remove spaces from the value string to make it string
		  String[] parts= strLine.split(",");
		  parts[1]=parts[1].replaceAll("\\s+", "");
		  int value= Integer.parseInt(parts[1]);
		  String key = parts[0];
		  //insert (key,value)
		  this.insert(key, value);
		}
		//long endTime = System.nanoTime();;
		//System.out.println("Insert took " + ((endTime - startTime)/1000000) + " msec");
		//Close the input stream
		br.close();
	}
	
	public void queryFile(String filename) throws IOException, NoSuchAlgorithmException
	{
		//open File
		FileInputStream fstream = new FileInputStream(filename);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		String strLine;
		
		//Read File Line By Line
		while ((strLine = br.readLine()) != null)   
		{
		  //insert (key,value)
		  this.query(strLine);
		}
		//Close the input stream
		br.close();
	}
	
	public void parseFile(String filename) throws IOException, NoSuchAlgorithmException
	{
		//open File
		FileInputStream fstream = new FileInputStream(filename);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		String strLine;

		//Read File Line By Line
		while ((strLine = br.readLine()) != null)   
		{
		  //1)split the line on ", "
		  String[] parts= strLine.split(", ");
		  String request = parts[0];
		  String key = parts[1];
		  if (request.equals("query"))
		  {
			  this.query(key);
		  }
		  else if (request.equals("delete"))
		  {
			  this.delete(key);
		  }
		  else if (request.equals("insert"))
		  {
			  int value= Integer.parseInt(parts[2]);
			  this.insert(key, value);
		  }
		}
		//Close the input stream
		br.close();
	}
	
	/** Kills all nodes and main*/
	public void KILL()
	{
		Iterator<Triple<Process,Integer,Integer>> myIt= this.proc.iterator();
		while (myIt.hasNext()) 
		{
			myIt.next().getPid().destroy();
	    }
		this.proc.clear();
		System.exit(0); // exit main
	}	
		
	//for testing
	public void printkeyRange() throws IOException
	{
		System.out.println("Key Range:");
	    for(int i=0; i<this.proc.size(); i++)
	    {
			Socket kkSocket = new Socket("localhost", this.proc.get(i).getSocket());
	        PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(kkSocket.getInputStream()));		
			
	        System.out.println(this.proc.get(i).getId() + " : ");
	        out.println("PrintKR");
			String confirm;
			
			while ((confirm = in.readLine()) != null) 
			{
	             if (confirm.equals("OK"))
	             {
	            	 break;
	             }
	         }
			kkSocket.close();		
	    }
	}
}

/*TODO out.close();
in.close();
clientSocket.close();
serverSocket.close();*/