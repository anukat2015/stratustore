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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.util.FileUtils;

public class Converter {

	public static void convert(String file, String outlang) {
		Model m = ModelFactory.createDefaultModel();
		try {
			m.read(new FileReader(file), "", FileUtils.guessLang(file));
		} catch (FileNotFoundException e) {
			System.err.println("Could not find file "+file);
			System.exit(1);
		}
		if(outlang.equals("turtle"))
			outlang = "TTL";
		String newfile = file.substring(0, file.lastIndexOf(FileUtils.getFilenameExt(file)))+outlang;
		System.out.println("Extension:"+FileUtils.getFilenameExt(file));
		try {
			m.write(new FileWriter(newfile),outlang);
		} catch (IOException e) {
			System.err.println("Error writing file \""+newfile+"\"");
		}		
		System.out.println("Created file is "+newfile);
	}
	public static void main(String[] args) {
		if(args.length != 2 || 
			!(args[1].equals("nt") || args[1].equals("turtle") || args[1].equals("xml"))) {
			System.out.println("Usage: sdb_tester.Converter <input file> <output format>");
			System.out.println("format is one of: xml, nt, turtle");
			System.out.println("e.g. sdb_tester.Converter dataset.nt xml");
			System.exit(1);
		}
		convert(args[0], args[1]);
	}
}
