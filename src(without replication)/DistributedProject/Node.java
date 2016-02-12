package DistributedProject;
import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;

import Testing.Pair;

public class Node
{
	public int previous;	//socket of previous node
	public int next;   		//socket of previous node
	public int socket; 		//my socket
	public int id; 		//id is hashed
	public int mainSocket;
	public int leaderSocket;// needed only to print the network list..maybe we can change that so it's not needed
	public String[] keyRange;	//{HIGH ID (my id), LOW ID (previous id)}
	public ArrayList<Pair<String, Integer>> fileList;
	//boroume na kanoume private ta pedia, na ta peirazoume me getters/setters
	
	public Node(int startsocket,String startid, int startmain, int startleader )
    {
    	this.socket= startsocket;
    	this.id= Integer.parseInt(startid); //id is hashed
    	this.previous=startsocket; next= startsocket;
    	this.mainSocket=startmain;
    	this.leaderSocket=startleader;
    	this.fileList= new ArrayList<Pair<String, Integer>>();
    	this.keyRange = new String[] {"",""};
    }
  
    public static void main(String args[]) throws IOException
    {     	
    	Node ThisNode = new Node(Integer.parseInt(args[0]),args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]));
        ServerSocket serverSocket = new ServerSocket(ThisNode.socket);
        while(true)
        {
	        Socket clientSocket = serverSocket.accept();
	        new ClientThread(clientSocket, ThisNode).start();
        }
        
    }	
    
	public String sendRequest (int socket,String request) throws UnknownHostException, IOException
	{
		String returnMSG=null;
		//connect to server
	    Socket kkSocket = new Socket("localhost", socket);
	    PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
	    BufferedReader in = new BufferedReader(new InputStreamReader(kkSocket.getInputStream()));
	    String fromNode;
	   	  
	    out.println(request); 
	    while ((fromNode = in.readLine()) != null) 
	    {
	    	//needs fixing , without space
            if (fromNode.equals("DoneUpdate"))
            {
            	returnMSG=null;
                break;
            }
            String[] parts2 =fromNode.split(",");
            if (parts2[0].equals("DoneQuery"))
            {
            	returnMSG=parts2[1];
                break;
            }
            String[] parts =fromNode.split(" ");
            if (parts[0].equals("DonePrint"))
            {
            	returnMSG=parts[1]; //Has the whole network
                break;
            }
            else if (parts[0].equals("ID"))
            {
            	returnMSG=parts[1]; //Has the previous' node ID (hashed)
                break;
            }
            else if (parts[0].equals("DoneInsert"))
            {
            	returnMSG=null;
                break;
            }
            else if (parts[0].equals("DoneFindkeyrage"))
            {
            	returnMSG=null; 
                break;
            }
            else if (parts[0].equals("DoneDelete"))
            {
            	returnMSG=null; 
                break;
            }
            
        }
	    kkSocket.close();
	    return returnMSG;
	}
	
	public void join(String idjoin, int socket) throws UnknownHostException, IOException, NoSuchAlgorithmException 
	{
		//place 2
		Iterator<Pair<String, Integer>> myIt= this.fileList.iterator();
		String hashedid=sha1(idjoin);
		boolean isFirst = checkFirst();
		if (isFirst)//if this node is first, the one with the smallest ID
		{
			//if id is smaller(equal) than my HIGH OR bigger than my LOW (last node), the node goes behind me
			if (hashedid.compareTo(this.keyRange[0])<=0 || hashedid.compareTo(this.keyRange[1])>0)
			{
				//message from sendrequest is ignored (null)
				this.sendRequest(socket,"Update" + " " + this.socket+ " " + this.previous); //send message to the joining node
				this.sendRequest(this.previous, "Update" + " " + socket + " " + "NULL"); // update this node's previous
				this.sendRequest(this.socket,"Update" + " " + "NULL" + " " + socket); //update this node's next
				
				this.findkeyRange(); //this Node finds key range
				this.sendRequest(socket,"Findkeyrange"); //joined node finds range (previous)
				
				//distribute the files between the 2 nodes (the newly inserted and its next)			
				while (myIt.hasNext()) 
				{
			        Pair<String, Integer> pair = myIt.next();
			        // If the Key of the file is smaller than the ID of the newly inserted node then the file should go to this node
			        if (sha1(pair.getKey()).compareTo(hashedid)<=0)
			        {
			        	this.fileList.remove(pair); //remove the file from the old node
			        	this.sendRequest(socket, "Insert " + pair.getKey() + " " + pair.getValue()); //sent request to insert the file to the new node 
			        }
			    }
				
			}
			else //node doesn't join here, ask next node
			{
				this.sendRequest(this.next, "Join" + " " + idjoin + " " + socket);
			}
		}
		else //this node is not first
		{
			//if id is smaller(equal) than my HIGH AND bigger than my LOW , the node goes behind me
			if (hashedid.compareTo(this.keyRange[0])<=0 && hashedid.compareTo(this.keyRange[1])>0)
			{
				this.sendRequest(socket,"Update" + " " + this.socket + " " + this.previous); //send message to the joining node
				this.sendRequest(this.previous, "Update" + " " + socket + " " + "NULL"); // update only the "next" of the previous node
				this.sendRequest(this.socket,"Update" + " " + "NULL" + " " + socket); //update this node's previous
				
				this.findkeyRange(); //this Node finds key range
				this.sendRequest(socket,"Findkeyrange"); //joined node finds range (previous)
				
				//distribute the files between the 2 nodes (the newly inserted and its next)			
				while (myIt.hasNext()) 
				{
			        Pair<String, Integer> pair = myIt.next();
			        // If the Key of the file is smaller than the ID of the newly inserted node then the file should go to this node
			        if (sha1(pair.getKey()).compareTo(hashedid)<=0)
			        {
			        	this.fileList.remove(pair); //remove the file from the old node
			        	this.sendRequest(socket, "Insert " + pair.getKey() + " " + pair.getValue()); //sent request to insert the file to the new node 
			        }
			    }
			}
			else //forward the request
			{
				this.sendRequest(this.next, "Join" + " " + idjoin + " " + socket);	
			}
		}
	}
		
	 	
	public void depart(String id) throws UnknownHostException, IOException, NoSuchAlgorithmException // update the socket list in main
	{
		//if my id is the same as depart, then i have to leave
		String hashedid=sha1(id);
		if (hashedid.compareTo(sha1(""+this.id))== 0)
		{
			//next and previous nodes update their fields
			this.sendRequest(this.next,"Update " + "NULL " + this.previous);
			this.sendRequest(this.next,"Findkeyrange");
			this.sendRequest(this.previous,"Update " + this.next + " NULL");
			
			//send all the files to next node
			Iterator<Pair<String, Integer>> pair= this.fileList.iterator();
			while (pair.hasNext()) 
			{
		        	Pair<String, Integer> tmp1 = pair.next();
			        this.sendRequest(this.next,"Insert " + tmp1.getKey() + " " + tmp1.getValue());
		    	}
		}
		else //forward the request
		{
			this.sendRequest(this.next,"Depart " + id);
		}
	}

	
	public void insert(String key, int value) throws UnknownHostException, IOException, NoSuchAlgorithmException
	{
		//create the new Pair
		Pair<String, Integer> temp= new Pair<String, Integer>(key,value);
		String hashedkey= sha1(key);
		boolean isFirst = checkFirst();
    	
		if (isFirst)//if this node is first
		{
			//if key is smaller(equal) than my HIGH OR bigger than my LOW (last node), i must take the key
			if (hashedkey.compareTo(this.keyRange[0])<=0 || hashedkey.compareTo(this.keyRange[1])>0)
			{
				//if an entry <key', value'> where key=key' already exists we have to remove it first to add the new one
				Pair<String, Integer> removePair;
				if ((removePair=this.contains(fileList,hashedkey))!= null)
				{
					this.fileList.remove(removePair);
				}
				this.fileList.add(temp);
				//System.out.println(this.socket + " " + this.fileList.get(0).getKey());
			}
			else //forward the request
			{
				this.sendRequest(this.next,"Insert " + key + " " + value);
			}
		}
		else //this node is not first
		{
			//if key is smaller(equal) than my HIGH AND bigger than my LOW , i must take the key
			if (hashedkey.compareTo(this.keyRange[0])<=0 && hashedkey.compareTo(this.keyRange[1])>0)
			{
				//if an entry <key', value'> where key=key' already exists we have to remove it first to add the new one
				Pair<String, Integer> removePair;
				if ((removePair=this.contains(fileList,hashedkey))!= null)
				{
					this.fileList.remove(removePair);
				}
				this.fileList.add(temp);
				//System.out.println(this.socket + " " + this.fileList.get(0).getKey());
			}
			else //forward the request
			{
				this.sendRequest(this.next,"Insert " + key + " " + value);
			}
		}
	}
	public void delete(String key) throws NoSuchAlgorithmException, UnknownHostException, IOException
	{
		String hashedkey= sha1(key);
		boolean isFirst = checkFirst();
		if (isFirst)//if this node is first
		{
			//if key is smaller(equal) than my HIGH OR bigger than my LOW (last node), the file is in this node
			if (hashedkey.compareTo(this.keyRange[0])<=0 || hashedkey.compareTo(this.keyRange[1])>0)
			{
				//if an entry <key', value'> where key=key' already exists we have to remove it
				Pair<String, Integer> removePair;
				removePair=this.contains(fileList,hashedkey); // removePair might be null if the file does not exist
				this.fileList.remove(removePair);
			}
			else //forward the request
			{
				this.sendRequest(this.next,"Delete " + key);
			}
		}
		else //this node is not first
		{
			//if key is smaller(equal) than my HIGH AND bigger than my LOW , the file is in this node
			if (hashedkey.compareTo(this.keyRange[0])<=0 && hashedkey.compareTo(this.keyRange[1])>0)
			{
				//if an entry <key', value'> where key=key' already exists we have to remove it
				Pair<String, Integer> removePair;
				removePair=this.contains(fileList,hashedkey); // removePair might be null if the file does not exist
				this.fileList.remove(removePair);
			}
			else //forward the request
			{
				this.sendRequest(this.next,"Delete " + key);
			}
		}
	}
	
	public String query(String key) throws NoSuchAlgorithmException, UnknownHostException, IOException
	{
		String hashedkey= sha1(key);
		boolean isFirst = checkFirst();
		String msg;
		if (isFirst)//if this node is first
		{
			//if key is smaller(equal) than my HIGH OR bigger than my LOW (last node), the file is in this node
			if (hashedkey.compareTo(this.keyRange[0])<=0 || hashedkey.compareTo(this.keyRange[1])>0)
			{
				//if an entry <key', value'> where key=key' already exists we have to return it
				Pair<String, Integer> queryPair;
				queryPair=this.contains(fileList,hashedkey); // removePair might be null if the file does not exist
				if (queryPair!=null)
				{
					return "FileName: " + queryPair.getKey()+ " " +"Value: " + queryPair.getValue()+ " " + "Node: " +this.id;
				}
				else
				{
					return "The file does not exist";
				}
				
			}
			else //forward the request
			{
				msg=this.sendRequest(this.next,"Query " + key);
				return msg;
			}
		}
		else //this node is not first
		{
			//if key is smaller(equal) than my HIGH AND bigger than my LOW , the file is in this node
			if (hashedkey.compareTo(this.keyRange[0])<=0 && hashedkey.compareTo(this.keyRange[1])>0)
			{
				//if an entry <key', value'> where key=key' already exists we have to return it
				Pair<String, Integer> queryPair;
				queryPair=this.contains(fileList,hashedkey); // removePair might be null if the file does not exist
				if (queryPair!=null)
				{
					return "FileName: " + queryPair.getKey()+ " " +"Value: " + queryPair.getValue()+ " " + "Node: " +this.id;
				}
				else
				{
					return "The file does not exist";
				}
			}
			else //forward the request
			{
				msg=this.sendRequest(this.next,"Query " + key);
				return msg;
			}
		}
		
	}
	
	/** Finds and returns an object (key,value) from a list of such objects
	 *  that has the same key as the one we were looking. If an object like 
	 *  this does not exist it returns null. Also the list does not contain
	 *  objects with the same values (key,value)
	 * @throws NoSuchAlgorithmException */
	public Pair<String, Integer> contains (ArrayList<Pair<String, Integer>> myList, String key) throws NoSuchAlgorithmException
	{
		Iterator<Pair<String, Integer>> myIt= myList.iterator();
		Pair<String, Integer> obj=null;
		while (myIt.hasNext()) {
	        Pair<String, Integer> tmp1 = myIt.next();
	        if (sha1(tmp1.getKey()).equals(key)) //key is the hashed value
	        {
	            obj= tmp1;
	        }
	    }
		return obj;
	}
	
	/**Finds the hash Key Ranges for the Node, based on his ID and the previous' node ID 
	 * @throws NoSuchAlgorithmException */
	public void findkeyRange() throws UnknownHostException, IOException, NoSuchAlgorithmException
	{
		String prevID;
		prevID = this.sendRequest(this.previous,"ID");
		this.keyRange[0] = sha1(""+this.id);
		this.keyRange[1] = sha1(prevID);
	}
	
	/**Returns true if the Node's ID is the smallest */
	public boolean checkFirst()
	{
    	//check if i'm the first (if HIGH ID < LOW ID) or the first node ever created (HIGH ID=LOW ID="")
    	if (this.keyRange[0].compareTo(this.keyRange[1])<0 || this.keyRange[0].equals(this.keyRange[1]) )
    		return true;
    	else
    		return false;
	}
	
	/**Traverses all items in the ItemList of the Node and prints them to stdout */
	public void printItems ()
	{
		Iterator<Pair<String, Integer>> myIt= this.fileList.iterator();
		while (myIt.hasNext()) 
		{
	        Pair<String, Integer> tmp1 = myIt.next();
	        System.out.println(tmp1.getKey() + " " + tmp1.getValue());     
	    }
		/*for(int i=0; i<this.fileList.size(); i++)
		{
			System.out.println(this.fileList.get(i).getKey() + " " + this.fileList.get(i).getValue());
	    }*/
	}
	

	
	private static String sha1(String input) throws NoSuchAlgorithmException {
        MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        byte[] result = mDigest.digest(input.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }
         
        return sb.toString();
    }
}

/*out.close();
in.close();
clientSocket.close();
serverSocket.close();*/
/* This code was at "place 1" (look up)
/*System.out.println("connection Established");
PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

String inputLine;
System.out.println("Gonna do something..");
	while ((inputLine = in.readLine()) != null) 
	{
		String[] parts =inputLine.split(" "); 
		if (parts[0].equals("Join"))
		{
			//System.out.println(ThisNode.id + " will help " + parts[1]+ " to join with socket,leader " + parts[2]+ " "+ parts[3]);
		    ThisNode.join(parts[1],Integer.parseInt(parts[2]), Integer.parseInt(parts[3]));
		    break;
		}
		if (parts[0].equals("Update"))
		{	
			
			if (!parts[1].equals("NULL"))
			{
				ThisNode.next=Integer.parseInt(parts[1]);
			}
			if (!parts[2].equals("NULL"))
			{
				ThisNode.previous=Integer.parseInt(parts[2]);
			}
			System.out.println(ThisNode.id + "has updated, next= "+ ThisNode.next + ", previous= "+ ThisNode.previous);
			out.println("Done");
			System.out.println("Did it");
		    break;
		}
		//if (parts[0].equals("Done"))
			
	}*/

//this code was at place 2
/*if (this.previous==leaderSocket && this.next==leaderSocket && this.socket == leaderSocket ) // we add the second node , there was only the first leader
{
	System.out.println(idjoin+" Enters 2nd!");
	this.sendRequest(socket,"Update " + this.next + " " + this.previous); // 1)next 2)previous
	this.sendRequest(this.socket,"Update "+ socket + " " + socket);
	//this.next= socket;
	//this.previous= socket;
}*/
//provlimA
//an thelo na valo ena ID megalytero apo ola, tote tha ginetai synexeia kyklos
//prepei na stamatao an ftaso pali ston leader komvo
