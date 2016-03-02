package DistributedProject;

import java.io.*;
import java.net.*;
import java.security.NoSuchAlgorithmException;

public class ClientThread extends Thread
{
	protected Socket socket;
	protected Node ThisNode;
	
	public ClientThread(Socket clientSocket, Node currentNode)
	{
        this.socket = clientSocket;
        this.ThisNode= currentNode;
    }
	
	public void run() 
	{
		try {
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		    String inputLine;
			while ((inputLine = in.readLine()) != null) 
			{				
				String[] parts1 =inputLine.split(",");
				if (parts1[0].equals("insert"))
				{
					//if main started the request, reply immediately to previous node
					if (parts1[4].compareTo("Main")==0)
					{
						out.println("OK");
					}
					this.ThisNode.insert(parts1[1],Integer.parseInt(parts1[2]),Integer.parseInt(parts1[3]),parts1[4]);

					//if a node started the request, handle the request and then reply
					if (parts1[4].compareTo("Node")==0)
					{
						out.println("OK");
					}			
					break;
				}
				else if (parts1[0].equals("insertreplica"))
				{
					out.println("OK"); // maybe it will change on depart/join
					this.ThisNode.insertreplica(Integer.parseInt(parts1[1]),parts1[2],Integer.parseInt(parts1[3]),Integer.parseInt(parts1[4]),parts1[5]);
					break;				
				}
				else if (parts1[0].equals("doneinsert"))
				{
					out.println("OK");
					//inform main
					this.ThisNode.sendRequest(this.ThisNode.getmainSocket(), "doneinsert" + "," + parts1[1] );					
					break;
				}
				else if (parts1[0].equals("query"))
				{
					out.println("OK");
					this.ThisNode.query(parts1[1],Integer.parseInt(parts1[2]),parts1[3]);					
					break;
				}
				else if (parts1[0].equals("donequery"))
				{
					out.println("OK");
					//inform main
					this.ThisNode.sendRequest(this.ThisNode.getmainSocket(), "donequery" + "," + parts1[1] );					
					break;
				}
				else if (parts1[0].equals("delete"))
				{
					out.println("OK");
					this.ThisNode.delete(parts1[1],Integer.parseInt(parts1[2]));					
					break;				
				}		
				else if (parts1[0].equals("deletereplica"))
				{
					out.println("OK");
					this.ThisNode.deletereplica(Integer.parseInt(parts1[1]), parts1[2], Integer.parseInt(parts1[3]));				
					break;				
				}
				else if (parts1[0].equals("deleterepnode"))
				{
					out.println("OK");
					this.ThisNode.deleterepnode(parts1[1], Integer.parseInt(parts1[2]));
					break;
				}
				else if (parts1[0].equals("donedelete"))
				{
					out.println("OK");
					//inform main
					this.ThisNode.sendRequest(this.ThisNode.getmainSocket(), "donedelete" + "," + parts1[1] );					
					break;
				}
				else if (parts1[0].equals("join"))
				{
					out.println("OK");
					this.ThisNode.join(parts1[1],Integer.parseInt(parts1[2]));
					
					break;
				}
				else if (parts1[0].equals("donejoin"))
				{
					out.println("OK");
					this.ThisNode.sendRequest(this.ThisNode.getmainSocket(), "donejoin");
					//this.ThisNode.sendRequest(this.ThisNode.getmainSocket(), "donejoin" + "," + parts1[1]);
					break;
				}
				else if (parts1[0].equals("fixreplicas"))
				{
					out.println("OK");
					this.ThisNode.fixreplicas(Integer.parseInt(parts1[1]));
					break;
				}
				else if (parts1[0].equals("depart"))
				{
					out.println("OK");
					this.ThisNode.depart(parts1[1]);
					break;
				}
				else if (parts1[0].equals("donedepart"))
				{
					out.println("OK");
					this.ThisNode.sendRequest(this.ThisNode.getmainSocket(), "donedepart");
					//this.ThisNode.sendRequest(this.ThisNode.getmainSocket(), "donedepart" + "," + parts1[1]);
					break;
				}
				else if (parts1[0].equals("update"))
				{					
					if (!parts1[1].equals("NULL"))
					{
						this.ThisNode.setNext(Integer.parseInt(parts1[1]));
					}
					if (!parts1[2].equals("NULL"))
					{
						this.ThisNode.setPrevious(Integer.parseInt(parts1[2]));
					}
					out.println("OK");
					break;
				}
				else if (parts1[0].equals("ID"))
				{
					out.println("ID"+ "," + this.ThisNode.getID());					
					break;
				}
				else if (parts1[0].equals("findkeyrange")) 
				{					
					this.ThisNode.findkeyRange(Integer.parseInt(parts1[1]),Integer.parseInt(parts1[2]));
					out.println("OK");					
					break;
				}
				else if (parts1[0].equals("TellKR"))
				{
					int Kay = Integer.parseInt(parts1[1]);
					String[] keyRange=this.ThisNode.getkeyRange();
					
					if ( Kay == 1)
					{
						out.println("HeadKR" + "," + keyRange[0] + "," + keyRange[1]);
					}
					else
					{
						String msg=this.ThisNode.sendRequest(this.ThisNode.getPrevious(),"TellKR"+ "," + (Kay-1));
						out.println("HeadKR" + "," + msg);
					}
				}
				// THIS IS FOR TESTING ONLY, MAYBE REMOVE IT?
				else if (parts1[0].equals("PrintKR"))
				{
					String[] keyRange=this.ThisNode.getkeyRange();
					System.out.println("KR : "+ keyRange[0] +" "+ keyRange[1]);
					System.out.println("TKR: "+this.ThisNode.keyRangeTail[0] +" "+ this.ThisNode.keyRangeTail[1]); 
					out.println("OK");					
					break;
				}
				if (parts1[0].equals("print"))
				{
					int startnode = Integer.parseInt(parts1[1]);
					out.println("OK");
					if (this.ThisNode.getNext()==startnode)
					{
						//if we reached the last node (started from leader node)
						this.ThisNode.sendRequest(startnode,"doneprint" + "," +parts1[2]+"->"+this.ThisNode.getID());	//send the full list to the leader
					}
					else
					{
						//more nodes need to be written on the list
						this.ThisNode.sendRequest(this.ThisNode.getNext(),"print"+ "," + startnode + "," +parts1[2]+"->"+this.ThisNode.getID());	
					}
					break;
				}
				if (parts1[0].equals("doneprint"))
				{
					this.ThisNode.sendRequest(this.ThisNode.getmainSocket(),"doneprint"+","+parts1[1]);
				}
				else					
					break;
			}
			socket.close();			
		}

		catch (IOException e) 
		{
			e.printStackTrace();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}		
}
	

