package Testing;
import java.io.*;
import java.net.*;

public class Provider1 {
	/*ServerSocket providerSocket;
    //Socket connection = null;
    ObjectOutputStream out;
    ObjectInputStream in;
    String message;*/
  
    public static void main(String args[])
    {
    	
    	int portnumber = 2016;
        try ( 
        		ServerSocket serverSocket = new ServerSocket(portnumber);
        		Socket clientSocket = serverSocket.accept();
                PrintWriter out =
                        new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()));
        		
        	)
        {
        	String inputLine;
			//out.println("hi im p1");    
			// Initiate conversation with client
			while ((inputLine = in.readLine()) != null) {
			    if (inputLine.equals("Hi"))
			    {
			    	out.println("hi there , im p1");
			    	new Provider1().client();
			    	break;
			    }	
			}
        }
	    catch (IOException e) {
	        System.out.println("Exception caught when trying to listen on port "
	            + portnumber + " or listening for a connection");
	        System.out.println(e.getMessage());
	    }
    }


    /*out.close();
    in.close();
    clientSocket.close();
    serverSocket.close();*/


	private void client ()
	{
		try (
				//connect to server
		    Socket kkSocket = new Socket("localhost", 2017);
		    PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
		    BufferedReader in = new BufferedReader(
		        new InputStreamReader(kkSocket.getInputStream()));
		) {
		    //BufferedReader stdIn =
		       // new BufferedReader(new InputStreamReader(System.in));
		    String fromServer;
		    String fromUser;
		   
		    //while ((fromServer = in.readLine()) != null) {
		        //System.out.println("Server: " + fromServer);
		        
		       /* if (fromServer.equals("hi there , im provider2") )
		        {
		       	break;
		        }*/
		        fromUser = "Hi";
		        
		        if (fromUser != null) {
		            //system.out.println("Client: " + fromUser);
		            out.println(fromUser);   
		        }
	
		   // }
		} catch (UnknownHostException e) {
		    System.err.println("Don't know about host ");
		    System.exit(1);
		} catch (IOException e) {
		    System.err.println("Couldn't get I/O for the connection to ");
		        System.exit(1);
		    }	
	}

}
