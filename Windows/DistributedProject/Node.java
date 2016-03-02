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


public class Node
{
	private int previous;		//socket of previous node
	private int next;   		//socket of next node
	private int socket; 		//my socket
	private int id; 			//id is not hashed
	private int mainSocket;		//main socket
	private int leaderSocket;	//leader's socket
	private String[] keyRange;	//{HIGH ID (my id), LOW ID (previous id)}
	public String[] keyRangeTail; //the key range of my head
	private int repSize ;		//replication factor
	private String strategy ; 	//lazy or linear evaluation
	private ArrayList<Pair<String, Integer>> fileList;
	private ArrayList<Pair<String, Integer>> replicaList;
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	//locks are necessary for the "lazy evaluation"
	//because when files are written lazily, main may request to read the same files (concurrent modification exception)		
	
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
	
	// Some setters/getters for the node Attributes
	public int getID()
	{
		return this.id;
	}
	public int getmainSocket()
	{
		return this.mainSocket;
	}
	public int getNext ()
	{
		return this.next;
	}
	public void setNext (int value)
	{
		this.next=value;
	}
	public int getPrevious ()
	{
		return this.previous;
	}
	public void setPrevious (int value)
	{
		this.previous=value;
	}
	public String[] getkeyRange()
	{
		return this.keyRange;
	}
	
    public static void main(String args[]) throws IOException
    {    
    	Node ThisNode = new Node(Integer.parseInt(args[0]),args[1],Integer.parseInt(args[2]),Integer.parseInt(args[3]),Integer.parseInt(args[4]),args[5]);
        ServerSocket serverSocket = new ServerSocket(ThisNode.socket);
        //server socket is never closed
        while(true)
        {
	        Socket clientSocket = serverSocket.accept();
	        new ClientThread(clientSocket, ThisNode ).start();
	        //client socket socket is closed at the thread
        }

    }	
    
    /**
     * Send a request (in the type of String) to a Node that listens to a specific socket
     * @throws UnknownHostException
     * @throws IOException
     */
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
            	returnMSG=parts[1] + "," + parts[2]; //Has the keyRange of the head of this chain
                break;
            }
            	break;
        }
	    kkSocket.close();
	    return returnMSG;
	}
	
	//Main methods for a node : join, depart, insert, delete, query 
	public void join(String idjoin, int socket) throws UnknownHostException, IOException, NoSuchAlgorithmException 
	{
		Iterator<Pair<String, Integer>> myIt= this.fileList.iterator();
		String hashedid=sha1(idjoin);
		boolean isFirst = checkFirst();
		
		//if im first && id is smaller(equal) than my HIGH || bigger than my LOW (last node), the node goes behind me
		boolean joinBeforeFirst = isFirst &&(hashedid.compareTo(this.keyRange[0])<=0 || hashedid.compareTo(this.keyRange[1])>0);
		//if im not first && id is smaller(equal) than my HIGH && bigger than my LOW , the node goes behind me
		boolean joinBeforeOther = !isFirst && (hashedid.compareTo(this.keyRange[0])<=0 && hashedid.compareTo(this.keyRange[1])>0);
		
		if (joinBeforeFirst || joinBeforeOther)
		{		
			int oldprevious= this.previous; // the old previous node, before we change it
			
			this.sendRequest(socket,"update" + "," + this.socket+ "," + this.previous); //send message to the joining node
			this.sendRequest(this.previous, "update" + "," + socket + "," + "NULL"); //update the previous node's "next"
			this.sendRequest(this.socket,"update" + "," + "NULL" + "," + socket); //update this node's "prev"			
			
			this.findkeyRange(1,1); //this Node finds keyRange			
			this.sendRequest(socket,"findkeyrange,1,1"); //joined node (previous) finds keyRange 
			this.sendRequest(socket,"findkeyrange" + "," + this.repSize + "," + (this.repSize+1)); //joined node and K next find keyRangeTail

			isFirst = checkFirst(); //check again if this node is first (after the join) 
			//distribute the files between the 2 nodes (the newly inserted and its next)
			while (myIt.hasNext()) 
			{
				Pair<String, Integer> pair = myIt.next();
				boolean checkOtherKR = !isFirst && !(sha1(pair.getKey()).compareTo(this.keyRange[0])<=0 && sha1(pair.getKey()).compareTo(this.keyRange[1])>0 );
				boolean checkFirstKR = isFirst && !(sha1(pair.getKey()).compareTo(this.keyRange[0])<=0 || sha1(pair.getKey()).compareTo(this.keyRange[1])>0 );
		        
				//for each file, if the file does not belong to me (check with keyRange)
				//then send it to joined node and delete it from me
				if (checkOtherKR || checkFirstKR)
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
			//this.sendRequest(this.leaderSocket, "donejoin" + "," + "JOIN Answer from: "+ this.id);				
		}
		else //node doesn't join here, ask next node
		{
			this.sendRequest(this.next, "join" + "," + idjoin + "," + socket);
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
			
			this.sendRequest(this.next,"findkeyrange" + "," + this.repSize + "," + this.repSize);
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
			//this.sendRequest(this.leaderSocket, "donedepart" + "," + "DEPART Answer from: "+ this.id);
			
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
			//if Main requested the insert (and we have eventual consistency), reply
			//else if a Node requested the insert, just send "OK" (done in ClientThread)
			if (sender.compareTo("Main")==0 && (this.strategy.equals("lazy")||(this.repSize==1)))
			{
				this.sendRequest(startsocket, "doneinsert" + ","+ "INSERT Answer from: "+ this.id);
			}
			// if we have replicas
			if (this.repSize >1) 
			{
				this.sendRequest(this.next, "insertreplica" + "," + (this.repSize-1) + "," + key + "," + value + "," + startsocket + "," + sender );
			}
		}
		else //forward the request
		{
			this.sendRequest(this.next,"insert" + "," + key + "," + value + "," + startsocket + "," + sender);
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

					this.sendRequest(startsocket, "donedelete" + ","+ "DELETE Answer from: "+ this.id); // inform the starting node for the delete
				}
				// if we have replicas
				if (this.repSize >1) 
				{
					this.sendRequest(this.next, "deletereplica" + "," + (this.repSize-1) + "," + key  + "," +startsocket);
				}
		}
		else //forward the request
		{
			this.sendRequest(this.next,"delete" + "," + key + "," + startsocket);
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
				this.sendRequest(startsocket, "donequery" + "," + query_answer+msg1);
			}
			else
			{
				msg1= this.printItems();
				this.sendRequest(this.next, "query" + "," + "*" + "," + startsocket + "," + query_answer+msg1);
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
			//Also need to check if the node is the tail of a chain where the head is the first node (lowest hashed ID)
			boolean checkReplicaFirstLinear = tailOfFirst && (hashedkey.compareTo(this.keyRangeTail[0])<=0 || hashedkey.compareTo(this.keyRangeTail[1])>0) && this.strategy.equals("linear"); 
			boolean checkReplicaOtherLinear = !tailOfFirst && (hashedkey.compareTo(this.keyRangeTail[0])<=0 && hashedkey.compareTo(this.keyRangeTail[1])>0) && this.strategy.equals("linear");
		
			//lazy evaluation, the answering node is the one that either has it or it has a replica of it.
			//Also need to check if the node is the first node (lowest hashed ID)
			boolean isFirst = checkFirst();
			boolean checkFirstKRlazy = (isFirst && (hashedkey.compareTo(this.keyRange[0])<=0 || hashedkey.compareTo(this.keyRange[1])>0)) && this.strategy.equals("lazy"); 
			boolean checkOtherKRlazy = (!isFirst && (hashedkey.compareTo(this.keyRange[0])<=0 && hashedkey.compareTo(this.keyRange[1])>0)) && this.strategy.equals("lazy");
			
			//Also need to check if the node is in a chain where the head is the first node (lowest hashed ID)
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
				System.out.println("Files check");
				Pair<String, Integer> queryPair;
				queryPair=this.contains(fileList,hashedkey); // removePair might be null if the file does not exist
				if (queryPair != null)
				{
					String msg1= queryPair.getKey()+ " "  + queryPair.getValue()+ " " + "|QUERY Answer from: " +this.id;
					this.sendRequest(startsocket, "donequery" + "," + " " + msg1 );
				}
				else
				{
					this.sendRequest(startsocket, "donequery" + "," + " File does not exist|QUERY Answer from: " +this.id );
				}			
			}
			else 
			{
				if (checkReplicaFirstLinear || checkReplicaOtherLinear|| checkReplicaFirstLazy || checkReplicaOtherLazy)
				{
					System.out.println("Replica check "+ hasReplicaOfFirst + (hashedkey.compareTo(this.keyRange[1])<=0 || hashedkey.compareTo(this.keyRangeTail[1])>0));
					//if an entry <key', value'> where key=key' already exists we have to return it
					Pair<String, Integer> queryPair;
					queryPair=this.contains(replicaList,hashedkey); // removePair might be null if the file does not exist
					if (queryPair != null)
					{
						String msg1= queryPair.getKey()+ " "  + queryPair.getValue()+ " " + "|QUERY Answer from: " +this.id;
						this.sendRequest(startsocket, "donequery" + ","  + " " +msg1 );
					}
					else
					{
						this.sendRequest(startsocket, "donequery" + "," + " File does not exist|QUERY Answer from: " +this.id );
					}		
				}
				else //forward the request
				{
					this.sendRequest(this.next,"query" + "," + key + "," +startsocket+ "," + query_answer);
				}
			}
		}
	}
	
	//Methods that are needed for replication, secondary methods
    /**
	* Node inserts a file (key,value) from its replica list, forwards the
	* request, if more replicas need to be entered and acts according
	* to its strategy (Lazy or Linear).
	* @throws NoSuchAlgorithmException
	* @throws UnknownHostException
	* @throws IOException
	* */
	public void insertreplica (int K, String key, int value,int startsocket,String sender) throws NoSuchAlgorithmException, UnknownHostException, IOException 
	{
		Pair<String, Integer> temp= new Pair<String, Integer>(key,value);
		Pair<String, Integer> removePair;
		
		Lock w = lock.writeLock();
		String hashedkey = this.sha1(key);
		//if the replica already exists remove it and add it with the new value
		if ((removePair=this.contains(this.replicaList,hashedkey))!= null)
		{
		    w.lock();
		   	this.replicaList.remove(removePair);
		    w.unlock();
		}
		w.lock();
		this.replicaList.add(temp);
		w.unlock();					

		
		if (K==1) // If this is supposed to be the last last replica
		{
			if(sender.equals("Main")&& this.strategy.equals("linear"))
			{//last replica node must reply to the StartingNode
				this.sendRequest(startsocket, "doneinsert" + ","+ "INSERTREPLICA Answer from: "+ this.id);
			}
			else if (sender.equals("Node")) //must put ELSE with "Node", allios kanei kyklo synexeia to arxeio gia depart/join
			{
				//dont send to staring node "doneinsert", he might be dead
			}						
		}
		else 
		{
			this.sendRequest(this.next, "insertreplica,"+ (K-1) + "," + key + "," + value + "," + startsocket + "," + sender);
		}
	}
	
   /**
	 * Node deletes a file (key,value) to its replica list, forwards the
	 * request, if more replicas need to be deleted and acts according
	 * to its strategy (Lazy or Linear)
	 * @throws UnknownHostException
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public void deletereplica (int K, String key, int startsocket) throws UnknownHostException, IOException, NoSuchAlgorithmException
	{ 
		String hashedkey = this.sha1(key); 	
		//remove the pair from replica list
		Pair<String, Integer> removePair;
		removePair=this.contains(this.replicaList,hashedkey); // removePair might be null if the file does not exist
		
		Lock w = lock.writeLock();					
		w.lock();
		this.replicaList.remove(removePair);
		w.unlock();
		if (K==1) // if this is the last replica 
		{
			if(this.strategy.equals("linear"))
			{//last replica node must reply to the StartingNode
				this.sendRequest(startsocket, "donedelete" + ","+ "DELETEREPLICA Answer from: "+ this.id);
			}						
		}
		else 
		{
			this.sendRequest(this.next, "deletereplica," + (K-1) + "," + key  + ","+startsocket);
		}
	}
	
   /**
	 * When a new node joins or an old one departs there will be splitting/merging
	 * of the file lists of 2 nodes so some replicas must be deleted.
	 * @throws NoSuchAlgorithmException
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void deleterepnode (String key, int K) throws NoSuchAlgorithmException, UnknownHostException, IOException
	{
		if (K==1) // if we reached the node that we have to delete the "key" from 
		{
			String hashedkey = this.sha1(key);
			Pair<String, Integer> removePair;
			removePair=this.contains(this.replicaList,hashedkey); // removePair might be null if the file does not exist
			Lock w = lock.writeLock();					
			w.lock();
			this.replicaList.remove(removePair);
			w.unlock();
		}
		else 
		{
			this.sendRequest(this.next, "deleterepnode" + "," + key + "," + (K-1));
		}
	}
	
	/**
	 * When a new node joins or an old one departs the nodes near this one
	 * must either insert some replicas that they didn't had or delete some
	 * that they are not supposed to
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void fixreplicas (int K) throws UnknownHostException, IOException
	{
		Iterator<Pair<String, Integer>> myIt= this.fileList.iterator();
		Lock r = lock.readLock();
	    r.lock();
		while (myIt.hasNext()) 
		{
	        Pair<String, Integer> tmp1 = myIt.next();
	        //insert the file to the replica list of the next K-1 nodes
	        this.sendRequest(this.next, "insertreplica" + "," + (this.repSize-1) + "," + tmp1.getKey() + "," + tmp1.getValue() + "," + this.socket + "," + "Node");
	        //delete the file from the replica list of the Kth next node
	        this.sendRequest(this.next, "deleterepnode" + "," + tmp1.getKey() + "," + this.repSize);
	    }
		r.unlock();	
		if (K>1) // if more nodes need to fix their replicas
		{
			this.sendRequest(this.previous , "fixreplicas" + "," + (K-1));
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
	
	/**@param K (=1) Finds the hash Key Ranges for the Node, based on his ID and the previous' node ID 
	 * @param K (>1) Finds the hash Key Ranges of the chain's head of this Node
	 * @param nextK  Forward to next K nodes
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
	
	/**Returns true if the Node's hashed ID is the smallest */
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
		//we use "\\" as a separator, main replaces "\\" with EOL
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
