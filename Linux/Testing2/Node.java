package Testing2;
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
	public int next;   		//socket of next node
	public int socket; 		//my socket
	public int id; 			//id is not hashed
	public int mainSocket;	//main socket
	public int leaderSocket;//leader's socket
	public String[] keyRange;	//{HIGH ID (my id), LOW ID (previous id)}
	public ArrayList<Pair<String, Integer>> fileList;
	
	public Node(int startsocket,String startid, int startmain, int startleader )
    {
    	this.socket= startsocket;
    	this.id= Integer.parseInt(startid);
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
	        //client socket socket is closed at the thread
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
	    	String[] parts =fromNode.split(",");
	    	//when ACK received, end sendRequest
	    	if (fromNode.equals("OK"))
            {
            	returnMSG=null;
                break;
            }            
            if (parts[0].equals("ID"))
            {
            	returnMSG=parts[1]; //Has the previous' node ID (hashed)
                break;
            }
            else 
            	break;
        }
	    kkSocket.close();
	    return returnMSG;
	}
	
	public void join(String idjoin, int socket) throws UnknownHostException, IOException, NoSuchAlgorithmException 
	{
		Iterator<Pair<String, Integer>> myIt= this.fileList.iterator();
		String hashedid=sha1(idjoin);
		boolean isFirst = checkFirst();
		if (isFirst)//if this node is first, the one with the smallest ID
		{
			//if id is smaller(equal) than my HIGH || bigger than my LOW (last node), the node goes behind me
			if (hashedid.compareTo(this.keyRange[0])<=0 || hashedid.compareTo(this.keyRange[1])>0)
			{
				//message from sendrequest is ignored (null)
				this.sendRequest(socket,"update" + "," + this.socket+ "," + this.previous); //send message to the joining node
				this.sendRequest(this.previous, "update" + "," + socket + "," + "NULL"); //update the previous node's "next"
				this.sendRequest(this.socket,"update" + "," + "NULL" + "," + socket); //update this node's "prev"
				//this.previous = socket; 
				
				String oldrange=this.keyRange[1]; //store old HIGH range, used to split files correctly
				this.findkeyRange(); //this Node finds key range
				this.sendRequest(socket,"findkeyrange"); //joined node (previous) finds key range 
				
				//distribute the files between the 2 nodes (the newly inserted and its next)	
				isFirst = checkFirst();//check again if this node is still first 
				while (myIt.hasNext()) 
				{
			        Pair<String, Integer> pair = myIt.next();			        		        
			        if (isFirst)// new node is LAST
			        {
				        // fileKey < newID && fileKey > oldHighRange , then send it to joined node
				        if (sha1(pair.getKey()).compareTo(hashedid)<=0 && sha1(pair.getKey()).compareTo(oldrange)>0 )
				        {
				        	this.sendRequest(socket, "insert," + pair.getKey() + "," + pair.getValue() + "," + this.socket + "," + "Node"); //sent request to insert the file to the new node    	
				        	myIt.remove();//remove the file from the old node
				        }
			        }
			        else // new node is FIRST
			        {
			        	// fileKey < newID || fileKey > oldHighRange , then send it to joined node
				        if (sha1(pair.getKey()).compareTo(hashedid)<=0 || sha1(pair.getKey()).compareTo(oldrange)>0 )
				        {
				        	this.sendRequest(socket, "insert," + pair.getKey() + "," + pair.getValue() + "," + this.socket + "," + "Node"); //sent request to insert the file to the new node    	
				        	myIt.remove();//remove the file from the old node
				        }
			        }

			    }
				//Join completed
				this.sendRequest(this.leaderSocket, "donejoin");				
			}
			else //node doesn't join here, ask next node
			{
				this.sendRequest(this.next, "join" + "," + idjoin + "," + socket);
			}
		}
		else //this node is not first
		{
			//if id is smaller(equal) than my HIGH && bigger than my LOW , the node goes behind me
			if (hashedid.compareTo(this.keyRange[0])<=0 && hashedid.compareTo(this.keyRange[1])>0)
			{
				this.sendRequest(socket,"update" + "," + this.socket + "," + this.previous); //send message to the joining node
				this.sendRequest(this.previous, "update" + "," + socket + "," + "NULL"); //update the previous node's "next"
				this.sendRequest(this.socket,"update" + "," + "NULL" + "," + socket); //update this node's "previous"
				//this.previous = socket; 
				
				this.findkeyRange(); //this Node finds key range
				this.sendRequest(socket,"findkeyrange"); //joined node finds range (previous)
				
				//distribute the files between the 2 nodes (the newly inserted and its next)			
				while (myIt.hasNext()) 
				{
			        Pair<String, Integer> pair = myIt.next();
			        // fileKey < newID , then send it to joined node
			        if (sha1(pair.getKey()).compareTo(hashedid)<=0)
			        {
			        	this.sendRequest(socket, "insert," + pair.getKey() + "," + pair.getValue() + "," + this.socket + "," + "Node"); //sent request to insert the file to the new node
			        	myIt.remove();//remove the file from the old node
			        }
			    }
				//Join completed
				this.sendRequest(this.leaderSocket, "donejoin");
			}
			else //node doesn't join here, ask next node
			{
				this.sendRequest(this.next, "join" + "," + idjoin + "," + socket);	
			}
		}
	}		
	 	
	public void depart(String id) throws UnknownHostException, IOException, NoSuchAlgorithmException // update the socket list in main
	{
		String hashedid=sha1(id);
		//if my id is the same as depart, then i have to leave
		if (hashedid.compareTo(sha1(""+this.id))== 0)
		{
			//next and previous nodes update their fields
			this.sendRequest(this.next,"update" + "," + "NULL" + "," + this.previous);
			this.sendRequest(this.next,"findkeyrange");
			this.sendRequest(this.previous,"update" + "," + this.next + "," + "NULL");
			
			//send all the files to next node
			Iterator<Pair<String, Integer>> pair= this.fileList.iterator();
			while (pair.hasNext()) 
			{
		        	Pair<String, Integer> tmp1 = pair.next();
			        this.sendRequest(this.next,"insert," + tmp1.getKey() + "," + tmp1.getValue() + "," + this.socket + "," +"Node");
		    }
			//Depart completed
			this.sendRequest(this.leaderSocket, "donedepart");
			
		}
		else //forward the request
		{
			this.sendRequest(this.next,"depart" + "," + id);
		}
	}

	public void insert(String key, int value,int startsocket,String sender) throws UnknownHostException, IOException, NoSuchAlgorithmException
	{
		//create the new Pair
		Pair<String, Integer> temp= new Pair<String, Integer>(key,value);
		String hashedkey= sha1(key);
		boolean isFirst = checkFirst();
    	
		if (isFirst)//if this node is first
		{
			//if key is smaller(equal) than my HIGH || bigger than my LOW (last node), i must take the key
			if (hashedkey.compareTo(this.keyRange[0])<=0 || hashedkey.compareTo(this.keyRange[1])>0)
			{
				//if an entry <key', value'> where key=key' already exists we have to remove it first to add the new one
				Pair<String, Integer> removePair;
				if ((removePair=this.contains(fileList,hashedkey))!= null)
				{
					this.fileList.remove(removePair);
				}
				this.fileList.add(temp);
				
				//Insert completed
				//if Main requested the insert, reply
				//else if a Node requested the insert, just send "OK" (done in ClientThread)
				if (sender.compareTo("Main")==0)
				{
					this.sendRequest(startsocket, "doneinsert");
				}
			}
			else //forward the request
			{
				this.sendRequest(this.next,"insert," + key + "," + value + "," + startsocket +","+ sender);
			}
		}
		else //this node is not first
		{
			//if key is smaller(equal) than my HIGH && bigger than my LOW , i must take the key
			if (hashedkey.compareTo(this.keyRange[0])<=0 && hashedkey.compareTo(this.keyRange[1])>0)
			{
				//if an entry <key', value'> where key=key' already exists we have to remove it first to add the new one
				Pair<String, Integer> removePair;
				if ((removePair=this.contains(fileList,hashedkey))!= null)
				{
					this.fileList.remove(removePair);
				}
				this.fileList.add(temp);
				
				//Insert completed
				//if Main requested the insert, reply
				//else if a Node requested the insert, just send "OK" (done in ClientThread)
				if (sender.compareTo("Main")==0)
				{
					this.sendRequest(startsocket, "doneinsert");
				}
			}
			else //forward the request
			{
				this.sendRequest(this.next,"insert," + key + "," + value + "," + startsocket + "," + sender);
			}
		}
	}
	
	public void delete(String key, int startsocket) throws NoSuchAlgorithmException, UnknownHostException, IOException
	{
		String hashedkey= sha1(key);
		boolean isFirst = checkFirst();
		if (isFirst)//if this node is first
		{
			//if key is smaller(equal) than my HIGH || bigger than my LOW (last node), the file is in this node
			if (hashedkey.compareTo(this.keyRange[0])<=0 || hashedkey.compareTo(this.keyRange[1])>0)
			{
				//if an entry <key', value'> where key=key' already exists we have to remove it
				Pair<String, Integer> removePair;
				removePair=this.contains(fileList,hashedkey); // removePair might be null if the file does not exist
				this.fileList.remove(removePair);
				this.sendRequest(startsocket, "donedelete"); // inform the starting node for the delete
			}
			else //forward the request
			{
				this.sendRequest(this.next,"delete," + key + "," + startsocket);
			}
		}
		else //this node is not first
		{
			//if key is smaller(equal) than my HIGH && bigger than my LOW , the file is in this node
			if (hashedkey.compareTo(this.keyRange[0])<=0 && hashedkey.compareTo(this.keyRange[1])>0)
			{
				//if an entry <key', value'> where key=key' already exists we have to remove it
				Pair<String, Integer> removePair;
				removePair=this.contains(fileList,hashedkey); // removePair might be null if the file does not exist
				this.fileList.remove(removePair);
				this.sendRequest(startsocket, "donedelete"); // inform the starting node for the delete
			}
			else //forward the request
			{
				this.sendRequest(this.next,"delete," + key + "," + startsocket);
			}
		}
	}
	
	public void query(String key, int startsocket, String query_answer) throws NoSuchAlgorithmException, UnknownHostException, IOException
	{
		if (key.equals("*"))
		{
			String msg1;
			//we reached the last node 
			if (this.next==startsocket)
			{
				msg1= this.printItems();
				this.sendRequest(startsocket, "donequery,"+query_answer+msg1);
			}
			else
			{
				msg1= this.printItems();
				this.sendRequest(this.next, "query," + "*" + "," + startsocket + "," + query_answer+msg1);
			}
		}
		else
		{
			String hashedkey= sha1(key);
			boolean isFirst = checkFirst();
			if (isFirst)//if this node is first
			{
				//if key is smaller(equal) than my HIGH OR bigger than my LOW (last node), the file is in this node
				if (hashedkey.compareTo(this.keyRange[0])<=0 || hashedkey.compareTo(this.keyRange[1])>0)
				{
					//if an entry <key', value'> where key=key' already exists we have to return it
					Pair<String, Integer> queryPair;
					queryPair=this.contains(fileList,hashedkey); // removePair might be null if the file does not exist
					if (queryPair != null)
					{
						String msg1=" FileName: " + queryPair.getKey()+ " " +"Value: " + queryPair.getValue()+ " " + "Node: " +this.id+","+startsocket;
						//System.out.println(msg1);
						this.sendRequest(startsocket, "donequery," +msg1 );
					}
					else
					{
						this.sendRequest(startsocket, "donequery, File does not exist" );
					}		
				}
				else //forward the request
				{
					this.sendRequest(this.next,"query," + key+","+startsocket+ "," + query_answer);
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
					if (queryPair != null)
					{
						String msg1=" FileName: " + queryPair.getKey()+ " " +"Value: " + queryPair.getValue()+ " " + "Node: " +this.id+","+startsocket;
						//System.out.println(msg1);
						this.sendRequest(startsocket, "donequery," +msg1 );
					}
					else
					{
						this.sendRequest(startsocket, "donequery, File does not exist" );
					}
				}
				else //forward the request
				{
					this.sendRequest(this.next,"query," + key+","+startsocket+ "," + query_answer);
				}
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
    	//check if i'm the first (if HIGH ID < LOW ID) or the only node
    	if (this.keyRange[0].compareTo(this.keyRange[1])<0 || this.keyRange[0].equals(this.keyRange[1]) )
    		return true;
    	else
    		return false;
	}
	
	/**Traverses all items in the ItemList of the Node and puts them into a string*/
	public String printItems ()
	{
		String result;
		result = "" + this.id +":"+ "\\";				
		Iterator<Pair<String, Integer>> myIt= this.fileList.iterator();
		while (myIt.hasNext()) 
		{
	        Pair<String, Integer> tmp1 = myIt.next();
	        result= result+ " " + tmp1.getKey() +" "+ tmp1.getValue() + "\\";	
	    }
		return result;
	}

	/**Encode String using sha1 algorithm*/
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