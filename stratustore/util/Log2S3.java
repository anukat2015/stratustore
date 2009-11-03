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

package stratustore.util;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.jets3t.service.*;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.*;
import org.jets3t.service.security.AWSCredentials;

class Log2S3 {
	private static Logger log = Logger.getLogger(Log2S3.class.getName());

	public static boolean objectExists(S3Service s3RestService, S3Bucket bucket, String key) throws S3ServiceException {
		try { 
			s3RestService.getObjectDetails(bucket,key);
			return(true); 
		} catch(S3ServiceException x) { return(false); } } 	
	
	public static void main(String[] args) throws Exception {
		StringBuffer inputText = new StringBuffer();
		
		/* Build filename from first argument, host name and current date-time */
        SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd HH.mm.ss Z");
        String fileName = (args.length > 0 ? args[0] + " " : "") + 
        	myFormat.format(new Date()).toString() + " " +
        	InetAddress.getLocalHost().getCanonicalHostName() + ".txt";

        /* Read input */
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String str = "";
		str = in.readLine();
		while (str != null) {
			inputText.append(str+"\n");
			str = in.readLine();
		}

        /* Load credentials from properties file */
		Properties props = new Properties();
		try {
			props.load(new FileInputStream("config/aws.properties"));
		} catch (IOException e) {
			log.error("Error reading Access Key and Secret Key from aws.properties file");
			log.error("Please specify aws.accessId and aws.secretKey in config/aws.properties");
			System.exit(1);
		}
		AWSCredentials awsCredentials = new AWSCredentials(props.getProperty("aws.accessId"), props.getProperty("aws.secretKey"));
        S3Service s3 = new RestS3Service(awsCredentials);

		/* Check if bucket "bsbm_log" is available, create if necessary */
        S3Bucket bsbmBucket = s3.getOrCreateBucket("bsbm_logs");
        
		/* Check if file exists, otherwise create file */
        String testName = fileName;
        int i = 1;
        while( objectExists(s3, bsbmBucket, testName) ) {
        	System.out.println("The filename "+testName+" already exists");
        	testName = fileName + i++;
        }
        fileName = testName;
        S3Object logfile = new S3Object(fileName, inputText.toString());
        s3.putObject(bsbmBucket, logfile);
        
	}
}
