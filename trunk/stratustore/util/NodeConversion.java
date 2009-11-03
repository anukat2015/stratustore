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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.test.NodeCreateUtils;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.shared.PrefixMapping;

public class NodeConversion {
	static Log log = LogFactory.getLog( NodeConversion.class );

	public static Node decodeNodeType(String value) {
		return decodeNodeType(value, null);
	}
	
	/** Retrieve a Jena Node type from a given String
	 *  The first two characters of the string are checked:
	 *  <li>:  is a Blank Node
	 *  <li>"  is a Literal (all Literals are enclosed in quotation marks)
	 *  <li>s3 is a reference to Amazon S3 service; get (literal) content from there; NOT IMPLEMENTED
	 *  <li>everything else is a URI, especially because we store shortened URIs like bsbm:Product
	 *  
	 * @param value The string to be converted
	 * @param pm The prefixMapping to be used
	 * @return A Node object if successful, null otherwise
	 * @throws Exception
	 */
	public static Node decodeNodeType(String value, PrefixMapping pm) {
        if (value == null) {
            return null;
        }
        if(value.length() < 2)
        	return NodeCreateUtils.create(value); // should never get called 
        
        String prefix = value.substring(0, 2).toLowerCase();
        if (prefix.startsWith(":")) {
            return decodeBlankNode(value);
        } else if (prefix.startsWith("\"")) {
            return decodeLiteral(value);
        } else if (prefix.startsWith("s3")) {
            //TODO: implement this
        	return decodeLiteral(value);
        } else
        	return decodeURI(value, pm);
    }

	public static Node decodeLiteral(String value) {
		if(value.charAt(0) == '"' && value.charAt(value.length()-1) == '"')
			return Node.createLiteral(value.substring(1, value.length()-1));
		
		try {
		String content = value.substring(1, value.lastIndexOf('"')); // URIs do not contain "s
		String rest = value.substring(value.lastIndexOf('"')+1);
		if(rest.startsWith("@"))
			return Node.createLiteral(content, rest.substring(1), null);
		else if(rest.startsWith("^^"))
			return Node.createLiteral(content, "", TypeMapper.getInstance().getTypeByName(rest.substring(2)));
		else {
			log.error("Could not create Literal from value <"+value+">");
			return null;
		}
		} catch (StringIndexOutOfBoundsException e) {
			log.error("No last \" at <"+value+">");
		}
		return null;
	}

	public static Node decodeURI(String value, PrefixMapping pm) {
    	if(pm == null)
    		return NodeCreateUtils.create(value);
    	else
    		return NodeCreateUtils.create(pm, value);
	}

	public static Node decodeBlankNode(String value) {
		return Node.createAnon(new AnonId(value.substring(1)));
	}

	public static RDFDatatype parseLiteral(String value) {
		return null;
	}
}
