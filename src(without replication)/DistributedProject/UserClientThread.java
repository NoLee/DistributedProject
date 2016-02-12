package DistributedProject;

import java.io.*;
import java.net.*;
import java.security.NoSuchAlgorithmException;

public class UserClientThread extends Thread
{
	protected String[] request;
	protected Main network;

	
	public UserClientThread(String[] string, Main object)
	{
        this.request= string;
        this.network= object;
    }
	

	public void run() 
	{
		try 
		{
			if (request[0].equals("Join"))
			{
				this.network.join(Integer.parseInt(this.request[1]));
			}
			else if (request[0].equals("Depart"))
			{
				this.network.depart(Integer.parseInt(this.request[1]));
			}
			else if (request[0].equals("Print"))
			{
				this.network.print();
			}
			else if (request[0].equals("PrintKR"))
			{
				this.network.printkeyRange();
			}
			else if (request[0].equals("PrintItems"))
			{
				this.network.printItems();
			}
			else if (request[0].equals("Insert"))
			{
				String[] toConcat = new String[request.length-2];
				String fileName;
				
				for (int i=1;i<request.length-1;i++)
				{
					toConcat[i-1] = request[i];
				}
				fileName = String.join(" ",toConcat);
				this.network.insert(fileName,Integer.parseInt(this.request[request.length-1]));
			}
			else if (request[0].equals("Delete") || request[0].equals("Query"))
			{
				String[] toConcat = new String[request.length-1];
				String fileName;
				
				for (int i=1;i<request.length;i++)
				{
					toConcat[i-1] = request[i];
				}
				fileName = String.join(" ",toConcat);
				if (request[0].equals("Delete"))
				{
					this.network.delete(fileName);
				}
				else
				{
					this.network.query(fileName);
				}
			}
			else if (request[0].equals("InsertFile"))
			{
				this.network.insertFile(request[1]);
			}
			else if (request[0].equals("KILL"))
			{
				this.network.KILL();
			}
			


		} 
		catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
