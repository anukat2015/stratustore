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

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

import org.apache.log4j.Logger;

public class Server {
		public static boolean flag = false;
		private static Logger log = Logger.getLogger(Server.class.getName());

		public static void main(String[] args) {

			flag = true;
			ServerSocket server = null;
			int port;
			boolean stop = false;
			String domain = "", accessKey = "", secretKey = "";
			StoreInSimpleDB store = null;
			
			try {
				port = Integer.parseInt(args[0]);
			} catch( Exception e ) {
				log.info("No port given, using standard port");
				port = StoreInSimpleDB.PORT;
			}
			
			
			try {
				server = new ServerSocket(port);
			} catch (Exception e) {
				System.out.println("Error: Cannot start SocketServer");
			}
			
			log.info("Server startet at " + new Date() + " on port " + port);

			while (!stop) {
				String input = null;
				StringBuilder sb = new StringBuilder(255);
				try {
					Socket client = server.accept();

					InputStream is = client.getInputStream();
					InputStreamReader isr = new InputStreamReader(is); 
					BufferedReader in = new BufferedReader(isr); 

					try {
						while ((input = in.readLine()) != null) {
							if (input.length() > 0) {
								sb.append(input);
							}
						}
						
						// To separate the commands from the data use "||" (double pipe)
						// commands are separated using "@"
						// each command consists of <type>:<value>
						// TODO: make this less ugly :-/ put options, commands in separate class or sth
						// TODO: test this more thoroughly
						int offsetID = -1;
						int delim = sb.indexOf("||");
						if( delim < 0 ) {
							log.error("No command could be found in message, discarding!");
							in.close();
							client.close();
							continue;
						}
						String commands = sb.substring(0, delim);
						String[] splitCommands = commands.split("\\@");
						for(int i=0; i<splitCommands.length; i++) {
							String[] typeValue = new String[2];
							typeValue = splitCommands[i].split("\\:");
							
							if(typeValue.length != 2) {
								log.error("Wrong parameter format, discarding message");
								sb.setLength(0);
							}
							if(typeValue[0].equals("offset"))
								offsetID = Integer.parseInt(typeValue[1]);
							else if(typeValue[0].equals("domain"))
								domain = typeValue[1];
							else if(typeValue[0].equals("accessKey"))
								accessKey = typeValue[1];
							else if(typeValue[0].equals("secretKey"))
								secretKey = typeValue[1];
							else if(typeValue[0].equals("stop")) {
								stop = true;
								log.debug("Stop received");
							}
							else {
								log.debug("Unknown command received! Discarding message");
								sb.setLength(0);
							}
						}
						
						input = sb.substring(delim+2).toString();
						
						if (input.length() > 0) {
							if(domain.equals("") || accessKey.equals("") || secretKey.equals("")) {
								log.error("Parameter missing, discarding message");
								break;
							}
							if(offsetID == -1) {
								log.warn("No offset received, setting to 0!");
								offsetID = 0;
							}
							
							log.info("Got message with offset "+offsetID+", uploading...");
							store = StoreInSimpleDB.getInstance(domain, accessKey, secretKey, 15);
							store.store(new StringBufferInputStream(input), offsetID);
						}
						in.close();
						client.close();
					} catch (Exception e) {
						log.error("Error while getting message from client: " + e.getMessage());
					}
				} catch (Exception e) {
					log.error("Error while building socket connection with client: " + e.getMessage());
				}
			}
			if(store != null)
				store.close();
		}
	}