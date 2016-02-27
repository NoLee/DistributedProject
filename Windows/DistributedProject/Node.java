package DistributedProject;
import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
	public String[] keyRangeTail; // the key range of my head
	public int repSize ;	 // replication factor
	public String strategy ; // lazy or linear evaluation
	public ArrayList<Pair<String, Integer>> fileList;
	public ArrayList<Pair<String, Integer>> replicaList;
	//lock are necessary for the "lazy evaluation"
	//because when files are written lazily, main may request to read the same files (concurrent modification exception)
	public final ReadWriteLock lock = new ReentrantReadWriteLock();
	
	public Node(int startsocket,String startid, int startmain, int startleader,int rep, String strat )
    {
    	this.socket= startsocket;
    	this.id= Integer.parseInt(startid);
    	this.previous=startsocket; next= startsocket;
    	this.mainSocket=startmain;
    	this.leaderSocket=startleader;
    	this.repSize= rep;
    	this.strategy=strat;
    	this.fileList= new ArrayList<Pair<String, Integer>>();
    	this.replicaList= new ArrayList<Pair<String, Integer>>();
    	this.keyRange = new String[] {"",""};
    	this.keyRangeTail = new String[] {"",""};
    }	
  
    public static void main(String args[]) throws IOException
    {    
    	Node ThisNode = new Node(Integer.parseInt(args[0]),args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]),Integer.parseInt(args[4]),args[5]);
        ServerSocket serverSocket = new ServerSocket(ThisNode.socket);
        while(true)
        {
	        Socket clientSocket = serverSocket.accept();
	        new ClientThread(clientSocket, ThisNode,ThisNode.lock).start();
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
            if (parts[0].equals("HeadKR"))
            {
            	returnMSG=parts[1]+","+ parts[2]; //Has the previous' node ID (hashed)
                break;
            }
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
				int oldprevious= this.previous; // the old previous node, before we change it
				//message from sendrequest is ignored (null)
				this.sendRequest(socket,"update" + "," + this.socket+ "," + this.previous); //send message to the joining node
				this.sendRequest(this.previous, "update" + "," + socket + "," + "NULL"); //update the previous node's "next"
				this.sendRequest(this.socket,"update" + "," + "NULL" + "," + socket); //update this node's "prev"
				//this.previous = socket; 
				
				String oldrange=this.keyRange[1]; //store old HIGH range, used to split files correctly
				this.findkeyRange(1,1); //this Node finds key range
				
				this.sendRequest(socket,"findkeyrange,1,1"); //joined node (previous) finds key range 
				this.sendRequest(socket,"findkeyrange" + "," + this.repSize + "," + this.repSize);
				
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
				        	this.sendRequest(this.next, "deleterepnode" + "," + pair.getKey() + "," + (this.repSize-1));
				        	Lock w = lock.writeLock();
				        	w.lock();
				        	myIt.remove();//remove the file from the old node
				        	w.unlock();
				        }
			        }
			        else // new node is FIRST
			        {
			        	// fileKey < newID || fileKey > oldHighRange , then send it to joined node
				        if (sha1(pair.getKey()).compareTo(hashedid)<=0 || sha1(pair.getKey()).compareTo(oldrange)>0 )
				        {
				        	this.sendRequest(socket, "insert," + pair.getKey() + "," + pair.getValue() + "," + this.socket + "," + "Node"); //sent request to insert the file to the new node 
				        	this.sendRequest(this.next, "deleterepnode" + "," + pair.getKey() + "," + (this.repSize-1));
				        	Lock w = lock.writeLock();
				        	w.lock();
				        	myIt.remove();//remove the file from the old node
				        	w.unlock();
				        }	        
			        }

			    }
				this.sendRequest(oldprevious , "fixreplicas" + "," + (this.repSize-1));
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
				int oldprevious= this.previous; // the old previous node, before we change it
				this.sendRequest(socket,"update" + "," + this.socket + "," + this.previous); //send message to the joining node
				this.sendRequest(this.previous, "update" + "," + socket + "," + "NULL"); //update the previous node's "next"
				this.sendRequest(this.socket,"update" + "," + "NULL" + "," + socket); //update this node's "previous"
				//this.previous = socket; 
				
				this.findkeyRange(1,1); //this Node finds key range
				this.sendRequest(socket,"findkeyrange,1,1"); //joined node finds range (previous)
				
				this.sendRequest(socket,"findkeyrange" + "," + this.repSize + "," + this.repSize);
				
				//distribute the files between the 2 nodes (the newly inserted and its next)			
				while (myIt.hasNext()) 
				{
			        Pair<String, Integer> pair = myIt.next();
			        // fileKey < newID , then send it to joined node
			        if (sha1(pair.getKey()).compareTo(hashedid)<=0)
			        {
			        	this.sendRequest(socket, "insert," + pair.getKey() + "," + pair.getValue() + "," + this.socket + "," + "Node"); //sent request to insert the file to the new node
			        	this.sendRequest(this.next, "deleterepnode" + "," + pair.getKey() + "," + (this.repSize-1));
			        	Lock w = lock.writeLock();
			        	w.lock();
			        	myIt.remove();//remove the file from the old node
			        	w.unlock();
			        }
			    }
				this.sendRequest(oldprevious , "fixreplicas" + "," + (this.repSize-1));
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
			this.sendRequest(this.next,"findkeyrange,1,1");
			this.sendRequest(this.previous,"update" + "," + this.next + "," + "NULL");
			
			this.sendRequest(this.next,"findkeyrange" + "," + this.repSize + "," + (this.repSize-1));
			//send all the files to next node
			Iterator<Pair<String, Integer>> pair= this.fileList.iterator();
			while (pair.hasNext()) 
			{
		        	Pair<String, Integer> tmp1 = pair.next();
			        this.sendRequest(this.next,"insert," + tmp1.getKey() + "," + tmp1.getValue() + "," + this.socket + "," +"Node");
			        this.sendRequest(this.next, "deleterepnode" + "," + tmp1.getKey() + "," + 1);
		    }
			this.sendRequest(this.previous , "fixreplicas" + "," + (this.repSize-1));
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
		
		//(for first node) :: if key is smaller(equal) than my HIGH || bigger than my LOW (last node), i must take the key
		boolean checkFirstKR = isFirst && (hashedkey.compareTo(this.keyRange[0])<=0 || hashedkey.compareTo(this.keyRange[1])>0);
		//(for other nodes) :: if key is smaller(equal) than my HIGH && bigger than my LOW , i must take the key 
		boolean checkOtherKR = !isFirst && (hashedkey.compareTo(this.keyRange[0])<=0 && hashedkey.compareTo(this.keyRange[1])>0);
    	
		if (checkFirstKR || checkOtherKR)
		{
			//if an entry <key', value'> where key=key' already exists we have to remove it first to add the new one
			Pair<String, Integer> removePair;
		    Lock w = lock.writeLock();
			if ((removePair=this.contains(fileList,hashedkey))!= null)
			{
			    w.lock();
			   	this.fileList.remove(removePair);
			    w.unlock();
			}
			w.lock();
			this.fileList.add(temp);
			w.unlock();
			
			//Insert completed
			//if Main requested the insert (and we have enetual consistency), reply
			//else if a Node requested the insert, just send "OK" (done in ClientThread)
			if (sender.compareTo("Main")==0 && (this.strategy.equals("lazy")||(this.repSize==1)))
			{
				System.out.println("Answer from :"+ this.id);
				this.sendRequest(startsocket, "doneinsert");
			}
			
			// if we have replicas
			if (this.repSize >1) 
			{
				this.sendRequest(this.next, "insertreplica," + (this.repSize-1) + "," + key + "," + value + "," + startsocket + "," + sender);
			}
		}
		else //forward the request
		{
			this.sendRequest(this.next,"insert," + key + "," + value + "," + startsocket +","+ sender);
		}
	}
	
	public void delete(String key, int startsocket) throws NoSuchAlgorithmException, UnknownHostException, IOException
	{
		String hashedkey= sha1(key);
		boolean isFirst = checkFirst();
		
		//like insert
		boolean checkFirstKR = isFirst && (hashedkey.compareTo(this.keyRange[0])<=0 || hashedkey.compareTo(this.keyRange[1])>0); 
		boolean checkOtherKR = !isFirst && (hashedkey.compareTo(this.keyRange[0])<=0 && hashedkey.compareTo(this.keyRange[1])>0);
		
		if (checkFirstKR || checkOtherKR)
		{
				//if an entry <key', value'> where key=key' already exists we have to remove it
				Pair<String, Integer> removePair;
				Lock w = lock.writeLock();
				removePair=this.contains(fileList,hashedkey); // removePair might be null if the file does not exist
				w.lock();
				this.fileList.remove(removePair);
				w.unlock();
				
				if (this.strategy.equals("lazy") || this.repSize==1)
				{
					//System.out.println("Answer from :"+ this.id);
					this.sendRequest(startsocket, "donedelete"); // inform the starting node for the delete
				}
				// if we have replicas
				if (this.repSize >1) 
				{
					this.sendRequest(this.next, "deletereplica," + (this.repSize-1) + "," + key  + ","+startsocket);
				}
		}
		else //forward the request
		{
			this.sendRequest(this.next,"delete," + key + "," + startsocket);
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
			//for the node with the lowest hash id we have that HIGH<LOW (for the range), so we need to check if a node 
			//is the tail (the last one) of a replica chain with head of the chain being this node.
			boolean tailOfFirst;
			if (this.keyRangeTail[0].compareTo(this.keyRangeTail[1])<0)
				tailOfFirst=true;
			else 
				tailOfFirst=false;
			//linear evaluation, only the last node of the chain answers.
			boolean checkReplicaFirstLinear = tailOfFirst && (hashedkey.compareTo(this.keyRangeTail[0])<=0 || hashedkey.compareTo(this.keyRangeTail[1])>0) && this.strategy.equals("linear"); 
			boolean checkReplicaOtherLinear = !tailOfFirst && (hashedkey.compareTo(this.keyRangeTail[0])<=0 && hashedkey.compareTo(this.keyRangeTail[1])>0) && this.strategy.equals("linear");
		
			//lazy evaluation, the answering node is the one that either has it or it has a replica of it.
			boolean isFirst = checkFirst();
			boolean checkFirstKRlazy = (isFirst && (hashedkey.compareTo(this.keyRange[0])<=0 || hashedkey.compareTo(this.keyRange[1])>0)) && this.strategy.equals("lazy"); 
			boolean checkOtherKRlazy = (!isFirst && (hashedkey.compareTo(this.keyRange[0])<=0 && hashedkey.compareTo(this.keyRange[1])>0)) && this.strategy.equals("lazy");

			boolean hasReplicaOfFirst;
			if (this.keyRange[1].compareTo(this.keyRangeTail[1])<0)
				hasReplicaOfFirst=true;
			else 
				hasReplicaOfFirst=false;
			boolean checkReplicaFirstLazy = (hasReplicaOfFirst && (hashedkey.compareTo(this.keyRange[1])<=0 || hashedkey.compareTo(this.keyRangeTail[1])>0)) && this.strategy.equals("lazy"); 	
			boolean checkReplicaOtherLazy = (!hasReplicaOfFirst && (hashedkey.compareTo(this.keyRange[1])<=0 && hashedkey.compareTo(this.keyRangeTail[1])>0)) && this.strategy.equals("lazy");
			
			if (checkFirstKRlazy||checkOtherKRlazy)
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
					this.sendRequest(startsocket, "donequery, File does not exist Node: " +this.id );
				}			
			}
			else 
			{
				if (checkReplicaFirstLinear || checkReplicaOtherLinear|| checkReplicaFirstLazy || checkReplicaOtherLazy)
				{
					//if an entry <key', value'> where key=key' already exists we have to return it
					Pair<String, Integer> queryPair;
					queryPair=this.contains(replicaList,hashedkey); // removePair might be null if the file does not exist
					if (queryPair != null)
					{
						String msg1=" FileName: " + queryPair.getKey()+ " " +"Value: " + queryPair.getValue()+ " " + "Node: " +this.id+","+startsocket;
						//System.out.println(msg1);
						this.sendRequest(startsocket, "donequery," +msg1 );
					}
					else
					{
						this.sendRequest(startsocket, "donequery, File does not exist Node: " +this.id );
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
		Lock r = lock.readLock();
	    r.lock();
    	Iterator<Pair<String, Integer>> myIt= myList.iterator();
		Pair<String, Integer> obj=null;
		while (myIt.hasNext()) {
	        Pair<String, Integer> tmp1 = myIt.next();
	        if (sha1(tmp1.getKey()).equals(key)) //key is the hashed value
	        {
	            obj= tmp1;
	        }	        
	    }
        r.unlock();	       
        return obj;		
	}
	
	/**Finds the hash Key Ranges for the Node, based on his ID and the previous' node ID 
	 * @throws NoSuchAlgorithmException */
	public void findkeyRange(int K, int nextK) throws UnknownHostException, IOException, NoSuchAlgorithmException
	{
		//find your own keyRange if replSize=1 , else find keyRangeTail
		if (K==1)
		{
			String prevID;
			prevID = this.sendRequest(this.previous,"ID");
			this.keyRange[0] = sha1(""+this.id);
			this.keyRange[1] = sha1(prevID);
		}
		else if (K>1)
		{
			String KRT = this.sendRequest(this.previous, "TellKR,"+(K-1));
			//this.keyRangeTail = KRT.split(",");
			String Kappa[] = KRT.split(",");
			this.keyRangeTail[0] = Kappa[0];
			this.keyRangeTail[1] = Kappa[1];
			if (nextK>1)
			{
				this.sendRequest(this.next, "findkeyrange" + "," + K + "," + (nextK-1));
			}
		}
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
		
		//FOR TESTING PURPOSES, TO PRINT REPLICAS
		result=result+"Replicas: \\";
		Iterator<Pair<String, Integer>> myIt2= this.replicaList.iterator();
		while (myIt2.hasNext()) 
		{
	        Pair<String, Integer> tmp1 = myIt2.next();
	        result= result+ " " + tmp1.getKey() +" "+ tmp1.getValue() + "\\";	
	    }
		return result;
	}

	/**Encode String using sha1 algorithm*/
	public String sha1(String input) throws NoSuchAlgorithmException {
        MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        byte[] result = mDigest.digest(input.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }         
        return sb.toString();
    }
}