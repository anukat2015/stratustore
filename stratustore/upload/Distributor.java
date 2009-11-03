/*
    Stratustore - a SimpleDB backed RDF data store based on Jena
    Copyright (C) 2009 Raffael Stein

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package stratustore.upload;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;


import com.hp.hpl.jena.util.FileUtils;

/**
 * A Distributor is used to orchestrate a collection of EC2 instances which upload data to SimpleDB
 * in parallel. Every instance is sent data in a round robin scheme.
 * 
 * @author Raffael Stein
 *
 */
public class Distributor {
	/**
	 * The amount of triples that are packed into one packet
	 */
	public  static final int PACKETSIZE = 2500;
	// TODO instantiate new EC2 instances instead
	private static final List<InetSocketAddress> serverList = new ArrayList<InetSocketAddress>() ;
	
	private static Logger log = Logger.getLogger(Distributor.class.getName());
	
	static int lastServer = 0;

	public static void main(String[] args) {
		Properties props = new Properties();
		try {
			props.load(new FileInputStream("config/aws.properties"));
		} catch (IOException e) {
			log.error("Error reading Access Key and Secret Key from aws.properties file");
		}

		// TODO include Apache Commons CLI parsing
		if(args.length != 2) {
			System.out.println("Usage: java jena.Distributor <ntriples dataset file> <SimpleDB domain>");
			System.exit(1);
		}

		String file = args[0];
		if(FileUtils.guessLang(file) != "N-TRIPLE") {
			log.error("Only N-Triples format is supported at the moment!");
			System.exit(1);
		}
			
		String domain = args[1];

		serverList.add( new InetSocketAddress("ec2-67-202-45-198.compute-1.amazonaws.com", 5000));
		serverList.add( new InetSocketAddress("ec2-75-101-189-27.compute-1.amazonaws.com", 5000));
/*		serverList.add( new InetSocketAddress("ec2-75-101-189-27.compute-1.amazonaws.com", 5002));
		serverList.add( new InetSocketAddress("localhost", 5000));
		serverList.add( new InetSocketAddress("localhost", 5001));
*/		
//		Thread t = new Thread() { public void run() {Server.main( new String[] {"5000"});}};
//		Thread t2 = new Thread() { public void run() {Server.main( new String[] {"5001"});}};
//		t.start();
//		t2.start();
		
		int offsetID = 0; // To have unique IDs for all Items in the SimpleDB
		int nodeCount = 0;
		StringBuffer cutLines = new StringBuffer();
		BufferedReader in;
		String row;
		
		String params = "domain:" + domain + "@" +
			"accessKey:" + props.getProperty("aws.accessId") + "@" +
			"secretKey:" + props.getProperty("aws.secretKey") + "||";

		sendServerParams(params);
		
		log.info("Starting upload");
		try {
			in = new BufferedReader( new FileReader(file) );

			while((row = in.readLine()) != null) {
				nodeCount++;
				cutLines.append(row).append('\n');
				if(nodeCount >= PACKETSIZE) {
					distributeToServers(cutLines.toString(), offsetID);
					offsetID += PACKETSIZE;
					cutLines.setLength(0);
					nodeCount = 0;
				}
			}
			//flush remaining packets in list
			distributeToServers(cutLines.toString(), offsetID);
			log.info("Done sending");
			// TODO: Write final item count to the DB, otherwise no further additions to DB possible
		} catch (IOException e) {
			log.error("Could not open file \"" + file + "\" for reading");
			e.printStackTrace();
			System.exit(1);
		}
		
		// close servers
		log.info("Stopping all servers");
		params = "stop:true||";
		sendServerParams(params);
	}

	/**
	 * Send <params> to all of the servers in the serverList
	 * 
	 * @param params The string to send
	 */
	private static void sendServerParams(String params) {
		for(InetSocketAddress serverAdress : serverList) {
			try {
			Socket socket = new Socket(serverAdress.getAddress(), serverAdress.getPort());
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

			out.write(params);
			out.close();
			socket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Send the contents of cutLines to the next server in the serverList
	 *  
	 * @param cutLines The string containing N-Triples, one triple a line
	 * @param offsetID The first Element ID of the new triples
	 */
	private static void distributeToServers(String cutLines, int offsetID) {
		boolean packetDelivered = false;
		// TODO think about what happens if all servers are down
		// TODO ensure correctness of submission
		while(!packetDelivered) {
			Socket socket;
			InetSocketAddress serverAdress = serverList.get(lastServer);
			try {
				socket = new Socket(serverAdress.getAddress(), serverAdress.getPort());

				BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

				// butt ugly!
				String s = "offset:" + String.valueOf(offsetID) + "||" +
					cutLines; 
				out.write(s);
				out.close();
				socket.close();
				packetDelivered = true;
			} catch (Exception e) {
				// TODO when retried too often, remove server from list
				log.warn("Error while delivering message to server " + serverAdress.getHostName() +
						" " + e.getMessage());
			} finally {
				if(lastServer == serverList.size()-1)
					lastServer = 0;
				else
					lastServer++;
			}
		}
	}

/*	private static void writeToFile(String newFilename, Reader reader) {
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter (newFilename));
			BufferedReader br = new BufferedReader(reader);
			String s;
			while ((s = br.readLine()) != null) {
				writer.write(s + "\n");
			}
			writer.close();
		} catch (IOException e) {
			log.error("Could not open file \"" + newFilename + "\" for writing");
			e.printStackTrace();
			System.exit(1);
		}
	}
*/
}
