package Testing;
	import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

public class Main {

	public static void main(String args[]) throws IOException, NoSuchAlgorithmException 
	{     
				System.out.println("sup\n"+"how\n"+"are\n"+"you\n");
				ArrayList<Pair<String, Integer>> myList = new ArrayList<Pair<String, Integer>>();
				Pair<String, Integer> test1= new Pair<String, Integer>("wet",1);
				Pair<String, Integer> test2= new Pair<String, Integer>("notwet",2);
				Pair<String, Integer> test3= new Pair<String, Integer>("notwet",2);
				boolean x = myList.isEmpty();
				myList.add(test1);
				myList.add(test2);
				boolean y = myList.isEmpty();
				Iterator<Pair<String, Integer>> myIt= myList.iterator();
				int i=0;
				while (myIt.hasNext()) {
			        Pair<String, Integer> tmp1 = myIt.next();
			        if (tmp1.getValue()==5) {
			           
			        }
			        i = i+1;
			    }
				
				String lol= " 2".replaceAll("\\s+","");
				int z= Integer.parseInt(lol);
				System.out.println(""+lol);
				// Open the file
				FileInputStream fstream = new FileInputStream("insert.txt");
				BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

				String strLine;

				//Read File Line By Line
				while ((strLine = br.readLine()) != null)   {
				  String[] parts= strLine.split(",");
				  parts[1]=parts[1].replaceAll("\\s+", "");
				  System.out.println (parts[0]+"||"+parts[1]);
				}

				//Close the input stream
				br.close();
				
				/*String x="Hello I'am George";
				String[] parts= x.split(" ");
				System.out.println(parts[2]);
				ProcessBuilder pb1 = new ProcessBuilder("java.exe","-cp","bin","Testing.Provider1");
		        Process p = pb1.start();
		        System.out.println("dfdrf");
				ProcessBuilder pb2 = new ProcessBuilder("java.exe","-cp","bin","Testing.Provider2");
		        Process p2 = pb2.start();
		        new Main().lel();*/     
	}
	
	private void lel() throws IOException
	{
		int portNumber1 = 2016;
		int portNumber2 = 2017;
		try (
				//connect to server
		    Socket kkSocket = new Socket("localhost", portNumber1);
		    PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
		    BufferedReader in = new BufferedReader(
		        new InputStreamReader(kkSocket.getInputStream()));
		) {
		    BufferedReader stdIn =
		        new BufferedReader(new InputStreamReader(System.in));
		    String fromServer;
		    String fromUser;
		   
		   /* while ((fromServer = in.readLine()) != null) {
		        System.out.println("Server: " + fromServer);
		        
		        if (fromServer.equals("hi there , im provider1") )
		        {
		        	this.listen();
		        	break;
		        }*/
		        fromUser = stdIn.readLine();
		        
		        if (fromUser != null) {
		            System.out.println("Client: " + fromUser);
		            out.println(fromUser); 
		            this.listen();
		        }
	
		    //}
		} catch (UnknownHostException e) {
		    System.err.println("Don't know about host ");
		    System.exit(1);
		} catch (IOException e) {
		    System.err.println("Couldn't get I/O for the connection to ");
		        System.exit(1);
		    }
	
	}
	
	private void listen()
	{
		try ( 
        		ServerSocket serverSocket = new ServerSocket(2018);
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
			    if (inputLine.equals("Hi im P2"))
			    {
			    	System.out.println("Bingo P2 found me!");
			    	break;
			    }	
			}
        }
	    catch (IOException e) {
	        System.out.println("Exception caught when trying to listen on port "
	            + "2018" + " or listening for a connection");
	        System.out.println(e.getMessage());
	    }
	}
	
	private static String sha1(String input) throws NoSuchAlgorithmException {
        MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        byte[] result = mDigest.digest(input.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }
         
        return sb.toString();
    }
}
