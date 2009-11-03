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

package stratustore;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import stratustore.graph.ModelSimpleDB;

import com.hp.hpl.jena.query.*;

public class ExecQuery {
	private static Logger log = Logger.getLogger(ExecQuery.class.getName());

	public static void main(String[] args) {		
		
		Properties props = new Properties();
		try {
			props.load(new FileInputStream("config/aws.properties"));
		} catch (IOException e) {
			log.error("Error reading Access Key and Secret Key from aws.properties file");
			System.exit(1);
		}

		if(args.length != 2) {
			System.out.println("Usage: java jena.QuerySimpleDB <query file> <SimpleDB domain>");
			System.exit(1);
		}

		String query = args[0];
		String domain = args[1];
		log.debug("Executing query <"+query+"> on domain <"+domain+">");
		ModelSimpleDB mymodel = new ModelSimpleDB(domain, props.getProperty("aws.accessId"), props.getProperty("aws.secretKey"));

		Long time = -System.currentTimeMillis();
		Query qu = QueryFactory.read(query);
		QueryExecution qe = QueryExecutionFactory.create(qu, mymodel);
		ResultSet results = qe.execSelect();
		time += System.currentTimeMillis();
		try {
			ResultSetFormatter.out(new FileOutputStream("query-output.txt"), results, qu);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		ResultSetFormatter.out(System.out, results, qu);
		qe.close();
		mymodel.close();
		log.debug("Active Threadcount: "+Thread.activeCount());
		log.info("It took "+time/1000.0+" secs to query");
	}

}
