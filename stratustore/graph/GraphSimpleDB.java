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

import org.apache.log4j.*;

import stratustore.graph.queryHandler.IterSimpleDB;
import stratustore.graph.queryHandler.QueryHandlerSimpleDB;

//import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;

import com.xerox.amazonws.sdb.*;
import com.hp.hpl.jena.graph.*;
import com.hp.hpl.jena.graph.impl.*;
import com.hp.hpl.jena.graph.query.QueryHandler;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

// TODO catch errors and act accordingly
// TODO catch network failures and exit gracefully

/**
 * @author Raffael Stein
 *
 */
public class GraphSimpleDB extends GraphBase {

	protected static final String SYSTEM = "sys"; // TODO: namespace here! redundant in IterSimpleDB!
	protected SimpleDB sdb;
	protected Domain domain;
	private boolean overwrite = true; // Testing only, overwrite all existing data
	
	private QueryHandler qh = null;
	
	/* This can be set from the outside to allow different threads using unique ids 
	 * TODO: check if synchronized access is needed
	 * */
	private int offsetID;
	private long totalTriplesProcessed = 0;
	private static Logger log = Logger.getLogger(GraphSimpleDB.class.getName());
	
	private ExecutorService es;
	private List<Triple> tList; 
	public enum ThreadState { PROCESSING, IDLE }
	protected static final int QUEUESIZE = 256;
	protected int THREADCOUNT = 15;
	// The maximum attribute-value count for one item before a new one has to be created; For SimpleDB use 255
	protected static final int MAXATTRCOUNT = 255;

/**
    The event manager that this Graph uses to, well, manage events; allocated on
    demand.
*/
	
	
	/**
	 * Starts ExecutorService when reading is started and flushes the tQueue after the reading is finished
	 * @author Raffael Stein
	 *
	 */
	private class GraphNotifyer implements GraphListener {
		public void notifyAddArray(Graph g, Triple[] triples) {}
		public void notifyAddGraph(Graph g, Graph added) {}
		public void notifyAddIterator(Graph g, Iterator it) {}
		public void notifyAddList(Graph g, List triples) {}
		public void notifyAddTriple(Graph g, Triple t) {}
		public void notifyDeleteArray(Graph g, Triple[] triples) {}
		public void notifyDeleteGraph(Graph g, Graph removed) {}
		public void notifyDeleteIterator(Graph g, Iterator it) {}
		public void notifyDeleteList(Graph g, List L) {}
		public void notifyDeleteTriple(Graph g, Triple t) {}
		public void notifyEvent(Graph source, Object value) {
			if(value.equals(GraphEvents.startRead) && es == null) {
//				es = Executors.newCachedThreadPool();
				es = Executors.newFixedThreadPool(THREADCOUNT);
				tList = new ArrayList<Triple>(QUEUESIZE);
			}
			if(value.equals(GraphEvents.finishRead)) {
				flushUploadQueue();
			}			
		}
	}
	
	public GraphSimpleDB(String domainName, String accessKey, String secretKey) {
		// Setting up the threadpool for concurrent connections to SimpleDB
		// TODO maybe start the threads only if performAdd is called? would blow up performAdd with if statements
		// TODO check input values for correctness!
		// TODO check if itemCnt in SimpleDB equals actual # of items
		if(accessKey.length() != 20)
			log.warn("Invalid Amazon Access Key, trying anyway");
		if(secretKey.length() != 40)
			log.warn("Invalid Amazon Secret Key, trying anyway");
		
//		done in eventlistener when reading is started
//		es = Executors.newCachedThreadPool();
//		tQueue = new ArrayList<Triple>(QUEUESIZE);
		PropertyConfigurator.configure("config/log4j.properties");
		createDomain(domainName, accessKey, secretKey);
		this.getEventManager().register(new GraphNotifyer());
		loadPrefixMapping();
	}

	public GraphSimpleDB(String domainname, String accessKey, String secretKey,
			int threadcount) {
		this(domainname, accessKey, secretKey);
		log.debug("Threadcount set to " + threadcount);
		THREADCOUNT = threadcount;		
	}

/**
 * Loads the prefixes from SimpleDB and adds them to the prefix map of the graph
 */
	private void loadPrefixMapping() {
		try {
			List<ItemAttribute> pm = domain.getItem("sys").getAttributes();
			for(ItemAttribute attr : pm) {
				this.getPrefixMapping().setNsPrefix(attr.getName(), attr.getValue());
			}
		} catch (SDBException e) {
			log.error("Error occured while loading prefixes:" + e.getMessage() );
		}
	}

	public QueryHandler queryHandler()
	{
		if (qh == null) qh = new QueryHandlerSimpleDB ( this );
		return qh;
	}

	/**
	 * Creates a new SimpleDB domain with <domainName> if it does
	 * not exist already.
	 * 
	 * @param domainName
	 * @param accessKey The SimpleDB Access Key
	 * @param secretKey The SimpleDB Secret Key
	 */
	private void createDomain(String domainName, String accessKey,
			String secretKey) {
		String nextToken = "";
		try {
			sdb = new SimpleDB(accessKey, secretKey, true);
			do {
				ListDomainsResult res = sdb.listDomains(nextToken);
				nextToken = res.getNextToken();
				for( Domain dom : res.getDomainList()) {
					if(dom.getName().equals(domainName)) {
						domain=dom;
						log.info("Found SimpleDB domain "+domainName);
						nextToken = "";
						break;
					}
				}
			} while( nextToken != null && nextToken.trim().length() > 0 );
			if( domain == null) {
				log.info("Creating SimpleDB domain "+domainName);
				domain = sdb.createDomain(domainName);
			}

		} catch (SDBException e) {
			// TODO check for domain creation, # of domains
			e.printStackTrace();
		}
	}

	/**
	 * Amazon provides Domain Metadata. Usually this information is not up to date.
	 * But iterating through the whole db to find out the size is too slow
	 * 
	 * @return The size of the graph or null if an error occurred
	 */
	public int graphBaseSize() {
		DomainMetadataResult res = null;

		try {
			res = domain.getMetadata();
		} catch (SDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res.getItemCount();
	}


	public void deleteDomain() {
		final Semaphore deletionComplete = new Semaphore(0);
		int activeThreads=0;

		try {
			log.info("Deleting all items of domain "+domain.getName());
			String nextToken = "";
			QueryResult res;
			// Making concurrent calls to SimpleDB speeds everything up
			Executor exec = Executors.newFixedThreadPool(THREADCOUNT);
			do {
				res = domain.listItems("", nextToken);
				nextToken = res.getNextToken();
				final QueryResult finalResult = res;
				activeThreads++;
				exec.execute(new Runnable() {
					public void run() {
						delItems(finalResult);
						deletionComplete.release();
					}}	);
				
			} while( nextToken != null && nextToken.trim().length() > 0 );
			offsetID = 0;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.info("Waiting for deletion threads to finish");
		try {
			deletionComplete.acquire(activeThreads);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.info("Domain deletion finished");
	}
	
	/** Delete the items which can be found using the nextToken
	 * 
	 * @param nextToken
	 */
	private void delItems(QueryResult res) {
		try {
			for( Item item : res.getItemList() ) {
				domain.deleteItem(item.getIdentifier());
			}
			log.debug("Deleted a hundred items");
		} catch (SDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected ExtendedIterator graphBaseFind(TripleMatch m) {
		//checkOpen();
		IterSimpleDB it = new IterSimpleDB(domain, m, getPrefixMapping());
		it.executeQuery();
		return it;
	}
	
	private Map<String, ItemNameCount> ItemNames = new HashMap<String, ItemNameCount>();
	private String lastSubject = "";
	private String subj = "";
	class ItemNameCount {
		String itemName;
		int count = 1;
		public ItemNameCount(String itemname) {
			this.itemName = itemname;
		}
	}
	/** Add Triple t to the SimpleDB
	 *  Object might be larger than 1024 Bytes and needs to be cut into small pieces 
	 *   - is done in createAttrList()
	 * 
	 * @param Triple to add
	 */
    public void performAdd( Triple t )
    {
    	// a thread uploads items grouped by subject; if subject changes then switch to another thread
    	boolean switchThread = true;
    	// get ItemName from HashMap or create new
    	String itemName;
    	lastSubject = subj;
    	subj = t.getSubject().toString(this.getPrefixMapping());
    	
    	ItemNameCount inamec = ItemNames.get(subj);
    	if(inamec != null) {
    		if(inamec.count < MAXATTRCOUNT) { 
    			inamec.count++;
    			itemName = inamec.itemName;
    			if(subj.equals(lastSubject))
    				switchThread = false;
    		} else { // item is full, create new
    			inamec.count = 1;
    			inamec.itemName = "Item"+offsetID++;
    			itemName = inamec.itemName;
    		}
    	} else {
    		itemName = "Item"+offsetID++;
    		ItemNames.put(subj, new ItemNameCount(itemName));
    	}
    	if(switchThread) {
    		// Executor starts new thread
    		// tList may be used here (although it's not "final") because the constructor copies the array
    		if(!tList.isEmpty())
    			es.execute(new TripleUploader(tList, itemName));
    		tList.clear();
    	}
   		tList.add(t);
   		totalTriplesProcessed++;
   	}
    
    private void flushUploadQueue() {
    	if(tList != null && !tList.isEmpty()) {
	    	es.execute( new TripleUploader(tList, "Item"+offsetID++));
	    	tList.clear();
    	}
    }
    
    /** 
     * Uploads a list of Triples.
     * Assumes that all Triples in the list have the same Subject!
     * Objects are truncated after 1024 characters (Amazon Restriction)
     * TODO: put longer Objects in S3 instead of SimpleDB 
     * 
     * @param tripleList The list that contains the triples
     * @param itemName   The name of the item which holds all the triples of the list
     * @return List of ItemAttributes
     */
    public class TripleUploader implements Runnable {
    	private final List<Triple> tripleList;
    	private final String itemName;
    	public TripleUploader(List<Triple> tripleList, String itemName) {
    		this.tripleList = new ArrayList<Triple>(tripleList);
    		this.itemName = itemName;
    	}
    	public void run() {
    		Item item;
    		int retries = 5;
    		boolean done = false;
    		List<ItemAttribute> attributes = createAttrList(tripleList);
    		if(attributes==null) {
				log.error("Cannot upload Item "+itemName+" with <null> attributes.");
				return;
			}
    		while(!done) {
    			try {
    				item = domain.getItem(itemName);
    				item.setMaxRetries(retries); 
    				item.putAttributes( attributes );
    				done = true;
    			} catch (SDBException e) {
    				// TODO: check on putAttributes error code/message
    				if(true) {
    					retries++;
    					log.warn("putAttributes failed, retrying with retries set to " + retries);
    				} else {
    				/* TODO: catch the following errors
    				 *  - Create new item if item is overflown (>256 attributes)
    				 */
    				log.error(e.getMessage());
    				}
    			}
    		}
    	}
    }

    /** 
     * Create a list of ItemAttributes from a List of Triples
     * Assumes that all Triples in the list have the same Subject!
     * Objects are truncated after 1024 characters (Amazon Restriction)
     * 
     * @param tripleList
     * @return List of ItemAttributes
     */
	private List<ItemAttribute> createAttrList(final List<Triple> tripleList) {
		List<ItemAttribute> attributes = new ArrayList<ItemAttribute>();
		
		if(tripleList.isEmpty())
			return null;

    	attributes.add(new ItemAttribute("S", 
    			tripleList.get(0).getSubject().toString(this.getPrefixMapping()), 
    			overwrite));

    	for(Triple t : tripleList) {
    		String predicate = t.getPredicate().toString(this.getPrefixMapping(), true);
    		String object = t.getObject().toString(this.getPrefixMapping(), true);
    		if(object.length() > 1024) {
    			if(t.getObject().isLiteral() && !t.getObject().getLiteralLanguage().equals("")) {
    				// add an existing language tag
    				object = object.substring(0,1020)+"\"@"+t.getObject().getLiteralLanguage();
    			} else {
    				object = object.substring(0, 1023)+"\""; // Literals have quotes on both sides
    			}
    		}
        	attributes.add(new ItemAttribute(predicate, 
        			object, 
        			overwrite));
    	}
    	return attributes;
	}

	
	
	
	
	/** Closing the graph
	 * writing the global item counter back to the database
	 * TODO: Adjust messages, did not necessarily write
	 */
	public void close() {
		int currentWaitingTime = 20;
		
		flushUploadQueue();
		// TODO: Update item counter in DB
		updatePrefixMapping();
		
		if(es != null) {
			log.info("Waiting for threads to finish, this may take some time...");
			es.shutdown();
			try {
				//TODO: implement progress bar every 10%, probably not easy since we don't know the size of the file
				while(es.awaitTermination(currentWaitingTime, TimeUnit.SECONDS) == false) {
					currentWaitingTime *= 2;
					log.debug("Uploading in progress");
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		if(offsetID>0) {
			log.info("Last written triple ID was " + (offsetID-1));
			log.info("A total number of "+totalTriplesProcessed+" triples were processed");
		}
		
/*		log.info("Size of graph is " + this.size());
		if(totalTriplesProcessed>0)
			log.debug("It took a mean of "+Math.round(100.0*totalRetries/totalTriplesProcessed)/100.0+" retries to upload");
		
		log.debug(THREADCOUNT-stoppedThreads + " threads remained");

// Don't write counter back anymore; Server should set/write it when everything is finished

		List<ItemAttribute> attributes = new ArrayList<ItemAttribute>();
		try {
			attributes.add(new ItemAttribute("itemCnt", String.valueOf(offsetID), true));
			domain.getItem(system).putAttributes(attributes);
		} catch (Exception e) {
//			log.error("Connection error while closing Graph. Database probably inconsistent! The connection to SimpleDB failed: " +
//					e.getLocalizedMessage());			
			e.printStackTrace();
		}
*/
	}

	/**
	 * Abusing the Triple layout to generate a prefix mapping
	 * Generate a List of triples to create/update the prefix map in the DB.
	 * The subject is always the "SYSTEM" entry, the predicate is the name of the prefix and the
	 * object is the actual URI the name points to
	 * 
	 * e.g.
	 * sys
	 * |- rdf=http://www.w3.org/1999/02/22-rdf-syntax-ns#
	 * |- rdfs=http://www.w3.org/2000/01/rdf-schema#
	 */
	private void updatePrefixMapping() {
		log.debug("Updating prefix mapping");
		String subject;
		List<ItemAttribute> pm = new ArrayList<ItemAttribute>();
		for( Object o : this.getPrefixMapping().getNsPrefixMap().entrySet()) {
			pm.add( new ItemAttribute( 
					((Entry<String,String>)o).getKey(),
					((Entry<String,String>)o).getValue(), false ));
		}
		try {
			domain.getItem(SYSTEM).putAttributes(pm);
			// TODO: find good value for retries; lost packet if exceeded!
		} catch (SDBException e) {
			log.error("Update of prefix mapping failed:"+e.getMessage());
		}
	}

	/**
	 * Set the offset which the threads use to enumerate the items (e.g. Item2000, Item2001, etc.)
	 * 
	 * @param offset
	 */
	public void setOffset(int offset) {
		 /* Ensure no thread is running atm! Otherwise, overwriting of items might occur! */
		synchronized(this) {
			this.offsetID = offset;
		}
	}

	public Domain getDomain() {
		return domain;
	}

/*	public ExtendedIterator findQuery(List<QueryTriple> qtlist) {
		return new IterSimpleDB(domain, qtlist, getPrefixMapping());		
	} 
*/
}
