package Testing;
import java.io.*;
import java.net.*;

public class Nodes {
	public static void main(String[] args) throws IOException {
        

		int portNumber1 = 2016;
		int portNumber2 = 2017;
		try (
				//connect to server
		    Socket kkSocket = new Socket("localhost", portNumber2);
		    PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
		    BufferedReader in = new BufferedReader(
		        new InputStreamReader(kkSocket.getInputStream()));
		) {
		    BufferedReader stdIn =
		        new BufferedReader(new InputStreamReader(System.in));
		    String fromServer;
		    String fromUser;
		   
		    while ((fromServer = in.readLine()) != null) {
		        System.out.println("Server: " + fromServer);
		        
		        if (fromServer.equals("hi there , im provider2") )
		        {
		        	break;
		        }
		        fromUser = stdIn.readLine();
		        
		        if (fromUser != null) {
		            System.out.println("Client: " + fromUser);
		            out.println(fromUser);   
		        }

		    }
		} catch (UnknownHostException e) {
		    System.err.println("Don't know about host ");
		    System.exit(1);
		} catch (IOException e) {
		    System.err.println("Couldn't get I/O for the connection to ");
		        System.exit(1);
		    }
		
/*		try (
				//connect to server
		    Socket kkSocket = new Socket("localhost", portNumber2);
		    PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
		    BufferedReader in = new BufferedReader(
		        new InputStreamReader(kkSocket.getInputStream()));
		) {
		    BufferedReader stdIn =
		        new BufferedReader(new InputStreamReader(System.in));
		    String fromServer;
		    String fromUser;
		    System.out.println("CHECK");
		    while ((fromServer = in.readLine()) != null) {
		        System.out.println("Server: " + fromServer);
		        System.out.println("CHECK");
		        if (fromServer== "hi there , im provider2")
		        	break;
		        fromUser = stdIn.readLine();
		        if (fromUser != null) {
		            System.out.println("Client: " + fromUser);
		            out.println(fromUser);
		        }
		    }
		} catch (UnknownHostException e) {
		    System.err.println("Don't know about host ");
		    System.exit(1);
		} catch (IOException e) {
		    System.err.println("Couldn't get I/O for the connection to ");
		        System.exit(1);
		    }*/
	}

}
