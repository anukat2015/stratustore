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

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.query.TripleSorter;

import java.util.Arrays;
import java.util.Comparator;

public class SubjectSorter implements TripleSorter {

	protected class SubjectComparator implements Comparator<Triple> {
		public int compare(Triple o1, Triple o2) {
			return o1.getSubject().toString().compareTo(o2.getSubject().toString());
		}
	}
	
	public Triple[] sort(Triple[] triples) {
		Comparator<Triple> c = new SubjectComparator();
		Arrays.sort(triples, c );
		return triples;
	}
}
