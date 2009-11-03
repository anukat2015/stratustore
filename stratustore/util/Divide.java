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

package stratustore.util;

import java.util.ArrayList;
import java.util.List;

public class Divide {
	/** Cuts a String into a List of Strings
	 * If the String contains spaces, the words are not split if they fit in an entry
	 * Each entry is surrounded by "s. This is to represent RDF Literals
	 * 
	 * @param str The String to divide up
	 * @param length The length of the cut up list entries
	 * @return The List containing the split up Strings
	 */ 
	public static List<String> divide(String str, int length) {
		List<String> returnList = new ArrayList<String>();
		if( str.length() > length) {
			// If present, remove "s in front and at the end
			StringBuffer sb = new StringBuffer(str.charAt(0) == '"' ? str.substring(1,str.length()-2) : str );
			int cutTo;
			while(sb.length() != 0) {
				if(sb.charAt(0) == ' ')
					sb.deleteCharAt(0);
				if( sb.length() < (length-2) ) {
					returnList.add("\"" + sb + "\"");
					cutTo = length-2;
				} else {
					cutTo = sb.substring(0, sb.length() < (length-2) ? sb.length() : (length-2)).lastIndexOf(' ');
					if( cutTo == -1 )
						cutTo = length-2;
					returnList.add("\"" + sb.substring(0,cutTo) + "\"");
				}
				sb = sb.delete(0, cutTo);
			}
		} else {
			returnList.add(str);
		}
		return returnList;
	}
}
