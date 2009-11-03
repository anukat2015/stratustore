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

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.query.BindingQueryPlan;
import com.hp.hpl.jena.graph.query.Query;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public class QueryPlanSimpleDB implements BindingQueryPlan{
    private Graph graph;
    private Query query;
    private Node [] variables;
    
    public QueryPlanSimpleDB( Graph graph, Query query, Node [] variables )
        {
        this.graph = graph;
        this.query = query;
        this.variables = variables;
        }
 
	public ExtendedIterator executeBindings() {
		// TODO Auto-generated method stub
		return null;
//		return new QueryEngineSimpleDB(query.getPattern(), ... );
	}

}
