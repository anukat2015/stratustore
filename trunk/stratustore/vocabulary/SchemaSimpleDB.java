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

package stratustore.vocabulary;
 
import com.hp.hpl.jena.rdf.model.*;
 
/**
 * Vocabulary definitions from Z:workspacejenaschemaSimpleDB.n3 
 * @author Auto-generated by schemagen on 13 Jan 2009 16:31 
 */
public class SchemaSimpleDB {
    /** <p>The RDF model that holds the vocabulary terms</p> */
    private static Model m_model = ModelFactory.createDefaultModel();
    
    /** <p>The namespace of the vocabulary as a string</p> */
    public static final String NS = "http://www.fzi.de/ipe/SimpleDB/2009/Schema#";
    
    /** <p>The namespace of the vocabulary as a string</p>
     *  @see #NS */
    public static String getURI() {return NS;}
    
    /** <p>The namespace of the vocabulary as a resource</p> */
    public static final Resource NAMESPACE = m_model.createResource( NS );
    
    public static final Property accessKey = m_model.createProperty( NS + "accessKey" );
    
    public static final Property domainName = m_model.createProperty( NS + "domainName" );
    
    public static final Property secretKey = m_model.createProperty( NS + "secretKey" );
    
}
