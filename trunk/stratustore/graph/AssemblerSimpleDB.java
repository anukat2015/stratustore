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

import stratustore.vocabulary.SchemaSimpleDB;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.Mode;
import com.hp.hpl.jena.assembler.assemblers.AssemblerBase;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.sparql.engine.optimizer.Optimizer;

/**
 * A Jena assembler that can build ModelSimpleDBs.
 * 
 * @author Raffael Stein
 * @version 12/01/2009
 * 
 */ 
public class AssemblerSimpleDB extends AssemblerBase {
	public Object open(Assembler ignore, Resource description, Mode ignore2) {
		if (!description.hasProperty(SchemaSimpleDB.domainName)) {
			throw new JenaException("Error in assembler specification " + description + ": missing property sdb:domainName");
		}
		String domainName = description.getProperty(SchemaSimpleDB.domainName).getObject().toString();
		if (!description.hasProperty(SchemaSimpleDB.accessKey)) {
			throw new JenaException("Error in assembler specification " + description + ": missing property sdb:accessKey");
		}
		String accessKey = description.getProperty(SchemaSimpleDB.accessKey).getObject().toString();
		if (!description.hasProperty(SchemaSimpleDB.secretKey)) {
			throw new JenaException("Error in assembler specification " + description + ": missing property sdb:secretKey");
		}
		String secretKey = description.getProperty(SchemaSimpleDB.secretKey).getObject().toString();
		Optimizer.enable();
		return new ModelSimpleDB(domainName, accessKey, secretKey);
	}
}

