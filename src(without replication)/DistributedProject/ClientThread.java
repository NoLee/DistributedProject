package DistributedProject;

import java.io.*;
import java.net.*;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

import Testing.Pair;

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
		    String inputLine,message;
			while ((inputLine = in.readLine()) != null) 
			{
				String[] parts =inputLine.split(" "); 
				if (parts[0].equals("Join"))
				{
					this.ThisNode.join(parts[1],Integer.parseInt(parts[2]));
					//System.out.println("Done Join "+parts[1]);
					out.println("DoneJoin");
					socket.close();
					break;
				}
				else if (parts[0].equals("Depart"))
				{
					this.ThisNode.depart(parts[1]);
					out.println("DoneDepart");
					socket.close();
					break;
				}
				else if (parts[0].equals("Update"))
				{					
					if (!parts[1].equals("NULL"))
					{
						this.ThisNode.next=Integer.parseInt(parts[1]);
					}
					if (!parts[2].equals("NULL"))
					{
						this.ThisNode.previous=Integer.parseInt(parts[2]);
					}
					//Debugging print
					//System.out.println(this.ThisNode.id + "has updated, next= "+ this.ThisNode.next + ", previous= "+ this.ThisNode.previous);
					out.println("DoneUpdate");
					socket.close();
					break;
				}
				else if (parts[0].equals("Print"))
				{
					if (this.ThisNode.next==this.ThisNode.leaderSocket)
					{
						//if we reached the first node, start communicating back till main 
						out.println("DonePrint "+parts[1]+"->"+this.ThisNode.id);
					}
					else
					{
						message = this.ThisNode.sendRequest(this.ThisNode.next,parts[0]+" "+parts[1]+"->"+this.ThisNode.id);	
						out.println("DonePrint "+message);
					}
					//socket.close();	
					break;
				}
				else if (parts[0].equals("Insert"))
				{
					String[] toConcat = new String[parts.length-2];
					String fileName;
					
					for (int i=1;i<parts.length-1;i++)
					{
						toConcat[i-1] = parts[i];
					}
					fileName = String.join(" ",toConcat);
					
					this.ThisNode.insert(fileName,Integer.parseInt(parts[parts.length-1]));
					//System.out.println("Done Join "+parts[1]);
					out.println("DoneInsert");
					//socket.close();
					break;
				}
				else if (parts[0].equals("Delete")|| parts[0].equals("Query"))
				{
					String[] toConcat = new String[parts.length-1];
					String fileName;
					
					for (int i=1;i<parts.length;i++)
					{
						toConcat[i-1] = parts[i];
					}
					fileName = String.join(" ",toConcat);
					
					if (parts[0].equals("Delete"))
					{
						this.ThisNode.delete(fileName);
						out.println("DoneDelete");
						break;
					}
					else
					{
						String query_answer=this.ThisNode.query(fileName);
						out.println("DoneQuery,"+query_answer);
						break;
					}
				}
				else if (parts[0].equals("ID"))
				{
					out.println("ID " + this.ThisNode.id);
					break;
				}
				else if (parts[0].equals("Findkeyrange"))
				{
					this.ThisNode.findkeyRange();
					out.println("DoneFindkeyrage");
					break;
				}
				else if (parts[0].equals("PrintKR"))
				{
					System.out.println(this.ThisNode.keyRange[0] +" "+ this.ThisNode.keyRange[1]);
					out.println("DonePrintKR");
					break;
				}
				else if (parts[0].equals("PrintItems"))
				{
					this.ThisNode.printItems();
					out.println("DonePrintItems");
					break;
				}
				else
					break;
			} 
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
	

