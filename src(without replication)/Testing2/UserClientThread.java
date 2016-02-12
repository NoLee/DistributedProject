package Testing2;

import java.io.*;
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
			if (request[0].equals("join"))
			{
				this.network.join(Integer.parseInt(this.request[1]));
			}
			else if (request[0].equals("depart"))
			{
				this.network.depart(Integer.parseInt(this.request[1]));
			}
			else if (request[0].equals("print"))
			{
				this.network.print();
			}
			else if (request[0].equals("PrintKR"))
			{
				this.network.printkeyRange();
			}
			else if (request[0].equals("insert"))
			{
				this.network.insert(request[1],Integer.parseInt(request[2]));
			}
			else if (request[0].equals("delete"))
			{
				this.network.delete(request[1]);
			}
			else if (request[0].equals("query"))
			{
				this.network.query(request[1]);
			}
			else if (request[0].equals("insertfile"))
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
