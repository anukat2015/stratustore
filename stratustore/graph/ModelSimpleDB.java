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

package stratustore.graph;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.impl.ModelCom;

/**
 * A Jena Model backed by an Amazon SimpleDB Database.
 * This Model is just a wrapper around a GraphSimpleDB for convenience. 
 * 
 * @author Raffael Stein
 * @version 2009
 */
public class ModelSimpleDB extends ModelCom implements Model {
	
	public ModelSimpleDB(String domainname, String accessKey, String secretKey) {
		super(new GraphSimpleDB(domainname, accessKey, secretKey));
	}
	
	public ModelSimpleDB(String domainname, String accessKey, String secretKey,
			int threadcount) {
		super(new GraphSimpleDB(domainname, accessKey, secretKey, threadcount));
	}

	public void deleteDomain() {
		((GraphSimpleDB)this.getGraph()).deleteDomain();
	}
	
	public void setOffset(int offset) {
		((GraphSimpleDB)this.getGraph()).setOffset(offset);
	}
	
}
