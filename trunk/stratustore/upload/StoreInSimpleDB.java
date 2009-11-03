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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import stratustore.graph.ModelSimpleDB;

import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.FileUtils;

/**
 * StoreInSimpleDB is used to put data from a Reader Object into SimpleDB. The submitted
 * triples have to be in N-Triple format.
 * 
 * The Server class uses this to upload the received data into the DB. But it can also 
 * be used standalone.
 *  
 * @author Raffael Stein
 *
 */
public class StoreInSimpleDB {
	// Singleton pattern to reuse the threads of the model upload
	public  static StoreInSimpleDB storage = null;
	private static Logger log = Logger.getLogger(StoreInSimpleDB.class.getName());
	public  static final int PORT = 4399;
	public  static String defaultLanguage = "N-TRIPLE";
	private ModelSimpleDB mymodel;
	
	private StoreInSimpleDB(String domain, String accessKey, String secretKey, int threadcount) {
		mymodel = new ModelSimpleDB(domain, accessKey, secretKey, threadcount);
	}
	
	public static StoreInSimpleDB getInstance(String domain, String accessKey, String secretKey, int threadcount) {
		if(storage==null)
			storage = new StoreInSimpleDB(domain, accessKey, secretKey, threadcount);
		return storage;
	}
	
	public void store(InputStream inputStream) {
		store(inputStream, 0, defaultLanguage);
	}
	public void store(InputStream inputStream, int offsetID) {
		store(inputStream, offsetID, defaultLanguage);
	}
	public void store(InputStream inputStream, String lang) {
		store(inputStream, 0, lang);
	}
	
	/**
	 * Storing the data from the reader in the DB. Starting with Item names from offsetID
	 * 
	 * @param inputStream The reader from which the data is read
	 * @param offsetID The first ID to use
	 * @param lang The language of the triples, e.g. TURTLE
	 */
	public void store(InputStream inputStream, int offsetID, String lang) {
		mymodel.setOffset(offsetID);
		log.info("Reading data...");

		try {
		//TODO returns too early if used in multithreading! cannot write next offset if data upload not finished!
			mymodel.read(inputStream, "", lang);
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			log.error("Error reading data " + e.getMessage());
			close();
			System.exit(1);
		}
	}

	public void close() {
		mymodel.close();
	}
	
	public static void main(String[] args) {		
		Properties props = new Properties();
		try {
			props.load(new FileInputStream("config/aws.properties"));
		} catch (IOException e) {
			log.error("Error reading Access Key and Secret Key from aws.properties file");
		}

		if(args.length != 3) {
			System.out.println("Usage: java jena.upload.StoreInSimpleDB <ntriples dataset file> <SimpleDB domain> <#threads>");
			System.exit(1);
		}

		String file = args[0];
		String domain = args[1];
		String threadcount = args[2];

		StoreInSimpleDB i;
		
		i= getInstance(domain, props.getProperty("aws.accessId"), props.getProperty("aws.secretKey"), Integer.parseInt(threadcount));

		long time = -System.currentTimeMillis();
		i.store(FileManager.get().open(file),FileUtils.guessLang(file));
		i.close();

		time += System.currentTimeMillis();
		log.info("It took "+time/1000.0+" secs");
	}
}
