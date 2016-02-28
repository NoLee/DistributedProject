package DistributedProject;

import java.io.*;
import java.net.*;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.locks.ReadWriteLock;

public class ClientThread extends Thread
{
	protected Socket socket;
	protected Node ThisNode;
	ReadWriteLock lock ;
	
	public ClientThread(Socket clientSocket, Node currentNode,ReadWriteLock locket) 
	{
        this.socket = clientSocket;
        this.ThisNode= currentNode;
        lock = locket;
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
					this.ThisNode.sendRequest(Integer.parseInt(parts1[1]), "doneinsert" + "," + parts1[2] );					
					break;
				}
				else if (parts1[0].equals("query"))
				{
					//send ACK
					out.println("OK");
					this.ThisNode.query(parts1[1],Integer.parseInt(parts1[2]),parts1[3]);					
					break;
				}
				else if (parts1[0].equals("donequery"))
				{
					out.println("OK");
					//inform main
					this.ThisNode.sendRequest(Integer.parseInt(parts1[1]), "donequery" + "," + parts1[2] );					
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
					this.ThisNode.sendRequest(Integer.parseInt(parts1[1]), "donedelete" + "," + parts1[2] );					
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
					this.ThisNode.sendRequest(Integer.parseInt(parts1[1]), "donejoin" + "," + parts1[2]);
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
					this.ThisNode.sendRequest(Integer.parseInt(parts1[1]), "donedepart" + "," + parts1[2]);;
					
					break;
				}
				// ------ BELOW HERE WE USE NODE ATTRIBUTES SO WE CANT MAKE THEM PRIVATE! ------
				else if (parts1[0].equals("update"))
				{					
					if (!parts1[1].equals("NULL"))
					{
						this.ThisNode.next=Integer.parseInt(parts1[1]);
					}
					if (!parts1[2].equals("NULL"))
					{
						this.ThisNode.previous=Integer.parseInt(parts1[2]);
					}
					out.println("OK");
					socket.close();
					break;
				}
				else if (parts1[0].equals("ID"))
				{
					out.println("ID," + this.ThisNode.id);					
					break;
				}
				else if (parts1[0].equals("findkeyrange")) //find MY keyRange
				{					
					this.ThisNode.findkeyRange(Integer.parseInt(parts1[1]),Integer.parseInt(parts1[2]));
					out.println("OK");					
					break;
				}
				else if (parts1[0].equals("TellKR"))
				{
					int Kay = Integer.parseInt(parts1[1]);
					
					if ( Kay == 1)
					{
						out.println("HeadKR,"+this.ThisNode.keyRange[0] + ","+this.ThisNode.keyRange[1]);
					}
					else
					{
						String msg=this.ThisNode.sendRequest(this.ThisNode.previous,"TellKR," + (Kay-1));
						out.println("HeadKR,"+msg);
					}
				}
				else if (parts1[0].equals("PrintKR"))
				{
					System.out.println("KR : "+this.ThisNode.keyRange[0] +" "+ this.ThisNode.keyRange[1]);
					System.out.println("TKR: "+this.ThisNode.keyRangeTail[0] +" "+ this.ThisNode.keyRangeTail[1]);
					out.println("OK");					
					break;
				}
				if (parts1[0].equals("print"))
				{
					out.println("OK");
					if (this.ThisNode.next==this.ThisNode.leaderSocket)
					{
						//if we reached the last node (started from leader node)
						this.ThisNode.sendRequest(this.ThisNode.next,"doneprint"+","+parts1[1]+"->"+this.ThisNode.id);	//send the full list to the leader
					}
					else
					{
						//more nodes need to be written on the list
						this.ThisNode.sendRequest(this.ThisNode.next,"print"+","+parts1[1]+"->"+this.ThisNode.id);	
					}
					break;
				}
				if (parts1[0].equals("doneprint"))
				{
					this.ThisNode.sendRequest(this.ThisNode.mainSocket,"doneprint"+","+parts1[1]);
				}
				else					
					break;
			}
			socket.close();			
		}

		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}		

}
	

