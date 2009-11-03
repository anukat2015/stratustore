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

package stratustore.graph.queryHandler;

//import org.apache.log4j.Logger;

import stratustore.graph.GraphSimpleDB;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.query.BindingQueryPlan;
import com.hp.hpl.jena.graph.query.ExpressionSet;
import com.hp.hpl.jena.graph.query.Mapping;
import com.hp.hpl.jena.graph.query.Query;
import com.hp.hpl.jena.graph.query.SimpleQueryHandler;
import com.hp.hpl.jena.graph.query.SimpleQueryPlan;
import com.hp.hpl.jena.graph.query.Stage;

public class QueryHandlerSimpleDB extends SimpleQueryHandler {
//	private static Logger log = Logger.getLogger(QueryHandlerSimpleDB.class.getName());
	protected GraphSimpleDB graph;
	
	public QueryHandlerSimpleDB(GraphSimpleDB graph) {
		super(graph);
		this.graph = graph;
	}
	
	/*
	 * This method is called after prepareBindings.
	 * Mapping: Nodes map to index in query which they are bound to
	 * Binding: map (variable)names to (RDF)values
	 * Query: Collection of Triple Patterns
	 * Stage is a Pipeline(stage)
	 * triple: the triples to be matched (like in Query)
	 * Domain: The index of a Domain entry corresponds to a possible binding of the result of a query
	 *     to a variable. A Pipe contains numerous possible bindings. The Domain is modified by "match"ing
	 *     a result of a query to it. The match() call updates the Domain.
	 *     
	 * patternStage returns Stage like a Pipeline
	 * if there is a group of patterns, a stage is used
	 * 			     single pattern, only graph.find is used
	 */
	
	public Stage patternStage(Mapping map, ExpressionSet constraints, Triple[] p) {
		
		return new PatternStageSimpleDB(graph, map, constraints, p);
	}

	/*
	 * This method returns a plan which can .executeBindings() which in turn gives back an
	 * ExtendedIterator. This iterator contains all the results of the query 
	 * 
	 * Query: has the patterns to the TripleMatches, use getPattern() for this
	 *        contains also argMap(NamedGraphMap), constraint(ExpressionMap) and sortMethod
	 * variables: unbound variables (e.g. ?label)
	 */
	public BindingQueryPlan prepareBindings(Query q, Node[] variables) {
		q.setTripleSorter(new SubjectSorter());
		return new SimpleQueryPlan(graph, q, variables);
//		ExtendedIterator i = new SimpleQueryPlan( graph, q, variables ).executeBindings(new ArrayList(), query.args().put( NamedTripleBunches.anon, graph ), variables);
//		return new SimpleQueryPlan( graph, q, variables );
		
	}

/*
  	public boolean containsNode(Node n) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public TreeQueryPlan prepareTree(Graph pattern) {
		// TODO Auto-generated method stub
		return null;
	}

	public ExtendedIterator subjectsFor(Node p, Node o) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public ExtendedIterator predicatesFor(Node s, Node o) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public ExtendedIterator objectsFor(Node s, Node p) {
		// TODO Auto-generated method stub
		return null;
	}

 */
}
