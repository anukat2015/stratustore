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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import stratustore.util.NodeConversion;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.query.QueryTriple;
import com.hp.hpl.jena.graph.query.QueryNode.Fixed;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.ItemAttribute;
import com.xerox.amazonws.sdb.QueryWithAttributesResult;

/** 
 * IterSimpleDB is a subclass of NiceIterator which queries SimpleDB for Triples that
 * map to the TripleMatch m. 
 * 
 * NOTE: Triples are expected to have the same subject or bind to the same variable!
 * 
 * TODO: Do not fetch all query results at once. Fetch more on i.hasNext() or do it async.
 * 
 * @author Raffael Stein
 *
 */
public class IterSimpleDB extends NiceIterator {
	protected List<Triple> tList = new ArrayList<Triple>();
	protected List<Triple> resultList = new ArrayList<Triple>();
	protected Iterator<Triple> i;
	private Domain domain;
	private PrefixMapping pm;
	protected static final int MAXRESULTS = 250;
	protected static final String SYSTEM = "sys"; // TODO: namespace here! redundant in IterSimpleDB!
	private List<String> objectSearch = new ArrayList<String>();
	
	/* If the subject stays the same for all queries, or a certain predicate/object pair will always be
	 * the same, add the restriction to the select-query, but add the information to the result manually  
	 */ 
	private Node fixedSubject;
	private List<Node> fixedPredList = new ArrayList<Node>();
	private List<Node> fixedObjList = new ArrayList<Node>();
	
//	TODO: private List<String> queryStringCache = new ArrayList<String>();
	
	private static Logger log = Logger.getLogger(IterSimpleDB.class);
	
	/**
	 * Get List<String> of Items that match m.
	 *  
	 * <li>TODO: It might be possible to make the iterator return Future objects 
	 * 		and access them multi threaded only when they are needed
	 * <li>TODO: Maybe ArrayList is not the best choice in terms of performance; 
	 *      Initial size of 10 might also be too small
	 * <li>TODO: Make this reentrant and a singleton -> no need to construct for every request 
	 * 
	 */
	public IterSimpleDB(Domain domain, TripleMatch m, PrefixMapping pm) {
		tList.add(m.asTriple());
		this.domain = domain;
		this.pm = pm;
	}

	/**
	 * NOTE: Triples are expected to have the same subject or bind to the same variable!
	 * 
	 * @param domain The TypicaSimpleDB domain to use
	 * @param qtlist Either a list of QueryTriples or a list of Triples to query the DB
	 * @param pm     The prefix mapping to use
	 */
	public IterSimpleDB(Domain domain, List qtlist, PrefixMapping pm) {
		this.domain = domain;
		this.pm = pm;
		
		if(qtlist.size() == 0)
			return;

		if(qtlist.get(0) instanceof QueryTriple) {
			for(QueryTriple qt : (List<QueryTriple>)qtlist) {
				tList.add(Triple.create(
						qt.S instanceof Fixed ? qt.S.node : Node.ANY,
						qt.P instanceof Fixed ? qt.P.node : Node.ANY,
						qt.O instanceof Fixed ? qt.O.node : Node.ANY));
			}
		} else if(qtlist.get(0) instanceof Triple) {
			this.tList = qtlist;
		}
		
		String subject = tList.get(0).getSubject().toString(pm);
		if(!tList.get(0).getSubject().equals(Node.ANY))
			fixedSubject = NodeConversion.decodeNodeType(tList.get(0).getSubject().toString(pm), pm);
		
		for(Triple t : tList) {
			if(!t.getSubject().toString(pm).equals(subject))
				log.error("Triples to query must have the same subject: "+"<"+t.getSubject().toString(pm)+"> <"+subject+">");
			if( !t.getPredicate().equals(Node.ANY) && !t.getObject().equals(Node.ANY) ) { // fixed pred/obj pair
				fixedPredList.add( t.getPredicate() );
				fixedObjList.add( t.getObject() );
			}
		}
	}

	public List<Triple> executeQuery() {
		Map<String, List<ItemAttribute>> itemList = null; 
		List<Node> predList = new ArrayList<Node>(); // Subject may be unknown when pred+obj is read, so store them here
		List<Node> objList = new ArrayList<Node>(); 
		QueryWithAttributesResult queryRes = null;
		String nextToken = "";
		boolean searchforObject = false;
		String queryString;
		Node nodeS = null;
		Iterator<Node> pIt, oIt;
		
		queryString = buildQueryString(domain.getName(), pm);
		log.debug("QueryString: " + queryString);

		if(queryString.lastIndexOf("WHERE") < 0) {
			log.error("Query wants to download the whole database! Aborting...");
			return resultList; // empty at that moment
		}
		nodeS = fixedSubject;
		
		// object was given without predicate, search in result necessary
		if(!objectSearch.isEmpty())
			searchforObject = true;

		try {
			do {
				queryRes = domain.selectItems(queryString, nextToken);
				nextToken = queryRes.getNextToken();
				itemList = queryRes.getItems();
				for( Map.Entry<String, List<ItemAttribute>> item : itemList.entrySet()) { // iterate list of itemNames with attributes
//					addFixedAttributes(predList, objList);
					for( ItemAttribute ia : item.getValue()) { // go through all attributes of one item/subject
						if(ia.getName().equals("S")) {
							nodeS = NodeConversion.decodeNodeType(ia.getValue(), pm);
							log.trace("Got subject from query: <"+ia.getValue()+">");
						} else {
							if(searchforObject) {
								for(String obj : objectSearch) {
									if(ia.getValue().equals(obj)) { // object needed to match and was found in object search list
										predList.add(NodeConversion.decodeNodeType(ia.getName(), pm));
										objList.add(NodeConversion.decodeNodeType(ia.getValue(), pm));
										break;
									}
								}
							} else { 
								predList.add(NodeConversion.decodeNodeType(ia.getName(), pm));
								objList.add(NodeConversion.decodeNodeType(ia.getValue(), pm));
							}
						}
					}
					// either the subject was given in the query or it was retrieved by now
					// now add the fixed p/o pairs to the list. an own iterator does not work because it cannot be reset
					predList.addAll(fixedPredList);
					objList.addAll(fixedObjList);
					pIt = predList.iterator();
					oIt = objList.iterator();
//					log.debug("The PredList ("+predList.size()+") now contains: "+predList);
//					log.debug("The ObjList  ("+objList.size() +") now contains: "+objList);
					while(pIt.hasNext()) {// both iterators should contain the same number of elements, not checked
						resultList.add( Triple.create(nodeS, pIt.next() , oIt.next()));
					}
					predList.clear();
					objList.clear();
				}
			} while( nextToken != null && nextToken.trim().length() > 0 );
		} catch (Exception e) {
			e.printStackTrace();
		}
		i = resultList.iterator();
		return resultList;
	}

	// TODO: output warning if the whole store has to be searched
	public String buildQueryString(String domain, PrefixMapping pm) {
		String subject;
		StringBuilder selectVars = new StringBuilder();
		StringBuilder matchVars = new StringBuilder();
		List<String> alreadySelected = new ArrayList<String>(); // to have the projection vars only once
		
		// either search for the subject or specify it as a search requirement
		subject = tList.get(0).getSubject().toString(pm);
		if(subject.equals("ANY")) {
			selectVars.append("`S`");
		} else {
			matchVars.append("WHERE `S` = '" + subject+"'");
		}			
			
		for(Triple t : tList) {
			String predicate = t.getPredicate().toString(pm);
			String obj = t.getObject().toString(pm);
			
			if(predicate.equals("ANY")) { // we will need all predicates no matter what
				selectVars = new StringBuilder("*");
				if(!obj.equals("ANY")) // object was given without predicate, search in result necessary
					objectSearch.add(obj);
			}
			else {
				// append to select section
				if(!selectVars.equals("*") && obj.equals("ANY") && !alreadySelected.contains(predicate)) {
					selectVars.append(selectVars.length() > 0 ? "," : "");
					selectVars.append("`"+predicate+"`");
					alreadySelected.add(predicate);
				}
				// append to matching section
				matchVars.append(matchVars.length() == 0 ? "WHERE " : " INTERSECTION ");
				matchVars.append("`"+predicate+"`");
				matchVars.append(obj.equals("ANY") ? " is not null" : " = '"+obj+"'");
			}
		}
		
		if(selectVars.length() == 0) {
			selectVars.append("*");
		}
		return "SELECT " + selectVars + " FROM " + domain + " " + matchVars;
	}
	
	public boolean hasNext() {
		return i.hasNext();
	}
	
	public Object next() {
		return i.next();
	}
	
	public void remove() {
		i.remove();
	}
}
