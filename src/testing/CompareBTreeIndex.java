package testing;

import static org.junit.Assert.*;

import org.junit.Test;

import database.DatabaseCatalog;
import database.Table;
import index.BTreeIndex;
import index.IndexConfig;

public class CompareBTreeIndex {
//	String configFile = "./configuration_test_compare.txt";
//	DatabaseCatalog db = new DatabaseCatalog(configFile);
//	IndexConfig inConf = new IndexConfig(db);
	
	@Test
	public void test() {
//		BTreeIndex boats = inConf.getIndex("Boats");
//		BTreeIndex sailors = inConf.getIndex("Sailors");
//		assert boats != null;
//		assert sailors != null;
//		System.out.println(boats.toString());
//		System.out.println(sailors.toString());
		
		String[] boat_desc = new String[]{"Boats", "D", "E", "F"};
		Table boat = new Table(boat_desc,"./samples/input/");
		BTreeIndex boats_exp = new BTreeIndex("./samples/expected_indexes/",boat,"Boats.E",0,10);
		BTreeIndex boats = new BTreeIndex("./samples/input/db/indexes/",boat,"Boats.E",0,10);
		
		String[] sailors_desc = new String[]{"Sailors","A","B","C"};
		Table sailor = new Table(sailors_desc,"./samples/input/");
		BTreeIndex sailors_exp = new BTreeIndex("./samples/expected_indexes/",sailor,"Sailors.A",1,15);
		BTreeIndex sailors = new BTreeIndex("./samples/input/db/indexes",sailor,"Sailors.A",1,15);
		
		assertEquals(boats_exp.toString(),boats.toString());
		assertEquals(sailors.toString(),sailors_exp.toString());
	}

}
