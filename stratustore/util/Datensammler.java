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
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import javax.xml.parsers.*;
import javax.xml.xpath.*;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;


public class Datensammler {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if( args.length != 1 ) {
			System.err.println("Usage Datensammler <prefix>");
			System.exit(1);
		}

		final String prefix = args[0];
		File dir = new File("c:/rstein/bsbm_logs/");
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(prefix);
			}
		};
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        // factory.setNamespaceAware( true );
        // factory.setValidating( true );
        DocumentBuilder builder = null;
		try {
			builder = factory.newDocumentBuilder();
		} catch (ParserConfigurationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        Document doc; 

        XPath xpath = XPathFactory.newInstance().newXPath();

        int count = 0;
        Double qmixadd = 0.0;
		Double[] queries = new Double[12];
		for(int i = 0; i<12; i++) queries[i] = 0.0;
		
        for( File f : dir.listFiles( filter ) ) {
			try {
				count++;
				doc = builder.parse( f );
				qmixadd += Double.valueOf(xpath.evaluate("/bsbm/querymix/qmph", doc));
				for(int i = 0; i<=11; i++) {
					String xpathstring = "/bsbm/queries/query[@nr=\""+(i+1)+"\"]/qps";
					queries[i] += Double.valueOf(xpath.evaluate(xpathstring, doc));
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (XPathExpressionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
        System.out.println("processed files: "+count);
		System.out.println("Total querymix qmph: "+qmixadd);
		for(int i = 0; i<=11; i++) {
			System.out.println("Query "+(i+1)+": "+(1.0*queries[i]));
		}
	}

}
