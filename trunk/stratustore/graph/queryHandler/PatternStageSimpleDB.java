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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import stratustore.graph.GraphSimpleDB;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.query.BufferPipe;
import com.hp.hpl.jena.graph.query.Domain;
import com.hp.hpl.jena.graph.query.ExpressionSet;
import com.hp.hpl.jena.graph.query.Mapping;
import com.hp.hpl.jena.graph.query.Matcher;
import com.hp.hpl.jena.graph.query.Pipe;
import com.hp.hpl.jena.graph.query.QueryNode;
import com.hp.hpl.jena.graph.query.QueryNodeFactory;
import com.hp.hpl.jena.graph.query.QueryTriple;
import com.hp.hpl.jena.graph.query.Stage;

public class PatternStageSimpleDB extends Stage {  
    
	static Log log = LogFactory.getLog( PatternStageSimpleDB.class );
    
    protected final GraphSimpleDB graph;
    protected final QueryTriple [] classified;
    protected final QueryNodeFactory factory;
//    protected final ValuatorSet [] guards;

    ExecutorService es = Executors.newCachedThreadPool();
    
    /**
	 * This classifies the triples in the array <code>classified</code>
	 */
    public PatternStageSimpleDB( GraphSimpleDB graph, Mapping map, ExpressionSet constraints, Triple [] triples ) { 
    	this.graph = graph;
        this.factory = QueryNode.factory;
        this.classified = QueryTriple.classify( factory, map, triples );
//        this.guards = new GuardArranger( triples ).makeGuards( map, constraints );
    }
        
    public Pipe deliver( final Pipe sink ) {
        final Pipe source = previous.deliver( new BufferPipe() );
    	final List<QueryTriple> qtList = new ArrayList<QueryTriple>();
    	final List<Future<List<Triple>>> resultTriples = new ArrayList<Future<List<Triple>>>(); // urgs, ugly construction :(
    	final List<List<QueryTriple>> qtLists = new ArrayList<List<QueryTriple>>();
    	
		String subj = classified[0].S.node.toString(); // to ensure "same subject" is true on first subject
		log.trace("Processing subject <"+subj+">");
		
		// TODO: create appropriate data structure for QTs, triples and matchers which belong together
		
		/* divide classified[] array and send queries with same subject to one thread.
		 * The logic is, that once the subject changes, the current qtList is executed and
		 * qtList is cleared, ready to be filled with another subject
		 * 
		 * QueryTriples in classified array are already sorted by subjects
		 * TODO: respect 10-operators-only-per-query Amazon restriction
		 */
		for(int i=0; i<classified.length; i++) {
        	if(classified[i].S.node.toString().equals(subj)) {
        		qtList.add(classified[i]); // add all QTs with "same subject" to list
        	} else {
        		final List<QueryTriple> queries = new ArrayList<QueryTriple>(qtList); 
        		qtLists.add(queries); // later, we only match each triple list to the QTs that produced the list 
        		resultTriples.add(
        			es.submit(new Callable<List<Triple>>() {
        			public List<Triple> call() throws Exception {
        				return execQueryTriples(queries);
				}}));
        		qtList.clear();
        		qtList.add(classified[i]);
        		subj = classified[i].S.node.toString();
        	}
        }
		// submit the queries that remained in the list
		qtLists.add(qtList); 
		resultTriples.add(
			es.submit(new Callable<List<Triple>>() {
			public List<Triple> call() throws Exception {
				return execQueryTriples(qtList);
		}}));
		
		assert(qtLists.size() == resultTriples.size()); // one qtlist for each set of triples
		
		processTriples(sink, source, resultTriples, qtLists);
		es.shutdown();
		return sink;
    }

	/**
	 * Turn a list of QueryTriples into a single SimpleDB query and return the resulting
	 * Triple list
	 * 
     * NOTE: QueryTriples are expected to have the same subject or query for the same variable (e.g. ?product)
     * 
     */
    protected List<Triple> execQueryTriples(List<QueryTriple> QTs) {
    	IterSimpleDB it = new IterSimpleDB(graph.getDomain(), QTs, graph.getPrefixMapping());
    	List<Triple> res = it.executeQuery();
    	log.debug("Query result size: "+res.size()+" triples");
    	return res;
   	}
    
	// putting results to sink MUST be done in separate thread, otherwise starvation will occur
	private void processTriples(final Pipe sink, final Pipe source,
			List<Future<List<Triple>>> resultTriples,
			List<List<QueryTriple>> qtLists) {
		/* 
		 * Iterate over
		 * 
		 * 1       qtLists which is a List of a List of QueryTriples which were grouped by subject
		 * |\
		 * | 2     every Element of qtLists, i.e. a List of QueryTriples all having the same subject 
		 * | |     for each QT, a new thread running matchTriples is created
		 * |  \
		 * |   3   every domain, that has been put in the Pipe.
		 * |   |   Note that the PatternStage is connected to the other stages via the pipes "source" and "sink".
		 * |   |   To allow in-stage processing, the ExchangerPipe is used to connect all the QTs to each other.    
		 * |    \
		 * |-----4 All triples which were returned by the group of QueryTriples sharing the same subject
		 *         and match them against the domain grabbed out of the pipe  
		 * 
        */
		Pipe in = source, intermed;
//		List<Domain> input = new ArrayList<Domain>();
//		List<Domain> intermed = null;
		
//		log.debug("qtLists.size()="+qtLists.size()+" ");
		try {
			for(int i = 0; i<qtLists.size(); i++) {                       // 1
				List<QueryTriple> sameSubjQTList = qtLists.get(i);
				for(int j=0; j<sameSubjQTList.size(); j++) {			  // 2
					intermed = (i == qtLists.size()-1) && (j==sameSubjQTList.size()-1) ? sink : new ExchangerPipe(500);  // to make sink the last pipe;
					final Pipe input = in;
					final Pipe output = intermed;
					final List<Triple> tl = resultTriples.get(i).get();
					final QueryTriple qt = sameSubjQTList.get(j);
					
					es.execute(new Runnable() { public void run() {
						matchTriples(tl, qt, input, output);}
					});
					in = intermed; // connect the output of the current pipe to the input of the next
					log.debug("Now processing QT: "+qt);
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		es.shutdown();
	}
    
	protected void matchTriples(List<Triple> tl, QueryTriple qt, Pipe input, Pipe output) {
		Domain d;
		while(input.hasNext()) { // 3                           
			d = input.get();
			Matcher m = createSPOMatcher(qt);
			for(Triple t : tl) { // 4   
				if(m.match(d, t)) {
					output.put(d.copy());
					log.trace("Domain after matching: "+d);
				}
			}
		}
		output.close();
	}

	private Matcher createSPOMatcher(final QueryTriple qt) {
    	return new Matcher() { 
				public boolean match(Domain d, Triple t) {
					return qt.S.matchOrBind( d, t.getSubject() )
                    	&& qt.P.matchOrBind( d, t.getPredicate() )
                    	&& qt.O.matchOrBind( d, t.getObject() ); 
					}
				};
	}
}
