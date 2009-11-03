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


import com.hp.hpl.jena.graph.query.Domain;
import com.hp.hpl.jena.graph.query.Pipe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Exchanger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This Pipe collects the Domains until the buffer is full and then switches the whole buffer to
 * the thread which is waiting on the get() method. The buffer is also swapped on close().
 * It uses a java.util.Exchanger to do the swap
 *  
 * @author Raffael Stein
 *
 */
public class ExchangerPipe implements Pipe {
    private int swapCounter = 0;
    
    private int bufferSize;
    private Exchanger<DomainBuffer> exch = new Exchanger<DomainBuffer>();
    
    // This class is needed to swap the open variable together with the buffer
    private class DomainBuffer {
        public boolean open = true;
        public List<Domain> buf;
    }
    
    private DomainBuffer incoming;
    private DomainBuffer outgoing;

    static Log log = LogFactory.getLog( ExchangerPipe.class );

    public ExchangerPipe() {
    	this(50); 
    }
    
    public ExchangerPipe( int size ) {
    	this.bufferSize = size;
    	this.incoming = new DomainBuffer();
    	this.outgoing = new DomainBuffer();
    	this.incoming.buf = new ArrayList<Domain>( size ); 
    	this.outgoing.buf = new ArrayList<Domain>( size ); 
    }
    
    /** Closing the pipe
     * 
     * Warning: Assuming that only 1 thread writes to the pipe and closes it thereafter!
     *          Simultaneous calls of close() and put() will lead to race conditions
     */
    public void close() {
    	log.debug("Closing ExchangerPipe");
		incoming.open = false; // set to false to signal final swap, now there are no more packets coming
		try {
			log.trace("Last Swap for closing");
			incoming = exch.exchange(incoming);
			swapCounter++;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		log.debug("Swaps encountered: " + swapCounter);
	}

	public void close(Exception e) {
    	close();
	}
	
	public void put(Domain d) {
		if(incoming.buf.size() >= bufferSize) { // buffer is full, swap now
			try {
				log.trace("Buffer is full, swapping on put");
				incoming = exch.exchange(incoming);
				swapCounter++;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		incoming.buf.add(d);
	}

	public Domain get() {
		if( outgoing.buf.isEmpty() ) { 
			log.error("Get() called but empty buffer! This should not happen.");
			return null;
		}
		return outgoing.buf.remove( outgoing.buf.size() - 1 ); // return last element
	}

	public boolean hasNext() {
		if( !(outgoing.buf.isEmpty() ) )
			return true;
		else if( outgoing.open == false ) { // buffer is empty and pipe is closed
			return false;
		} else {
			try {
				log.trace("Swapping on getNext");
				outgoing = exch.exchange(outgoing);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return !outgoing.buf.isEmpty();
		}
	}
}
