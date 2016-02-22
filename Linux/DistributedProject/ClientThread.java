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
				else if (parts1[0].equals("doneinsert"))
				{
					out.println("OK");
					//inform main
					this.ThisNode.sendRequest(this.ThisNode.mainSocket, "doneinsert" );					
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
					this.ThisNode.sendRequest(this.ThisNode.mainSocket, "donequery," +parts1[1] );					
					break;
				}
				else if (parts1[0].equals("delete"))
				{
					out.println("OK");
					this.ThisNode.delete(parts1[1],Integer.parseInt(parts1[2]));					
					break;				
				}				
				else if (parts1[0].equals("donedelete"))
				{
					out.println("OK");
					//inform main
					this.ThisNode.sendRequest(this.ThisNode.mainSocket, "donedelete" );					
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
					this.ThisNode.sendRequest(this.ThisNode.mainSocket, "donejoin");
					
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
					this.ThisNode.sendRequest(this.ThisNode.mainSocket, "donedepart");
					
					break;
				}
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
				else if (parts1[0].equals("findkeyrange"))
				{					
					this.ThisNode.findkeyRange();
					out.println("OK");					
					break;
				}
				else if (parts1[0].equals("PrintKR"))
				{
					System.out.println(this.ThisNode.keyRange[0] +" "+ this.ThisNode.keyRange[1]);
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
	

