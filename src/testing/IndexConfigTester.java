package testing;

import static org.junit.Assert.*;

import java.util.HashMap;

import org.junit.Test;

import database.DatabaseCatalog;
import index.BTreeIndex;
import index.IndexConfig;

public class IndexConfigTester {
	String configFile = "./configuration_test.txt";
	DatabaseCatalog db = new DatabaseCatalog(configFile);
	IndexConfig inConf = new IndexConfig(db);

//	@Test
//	public void testCreateIndex() {
//		BTreeIndex boats = inConf.getIndex("Boats");
//		BTreeIndex sailors = inConf.getIndex("Sailors");
//		assert boats != null;
//		assert sailors != null;
//		System.out.println(boats.toString());
//		System.out.println(sailors.toString());
//		
//	}
	
//	I have changed the index_info.txt to be the following:
//	Boats E 0 10
//	Boats D 1 20
//	Sailors A 1 15 
//	Sailors B 0 10
//	Sailors C 1 15
//	Reserves G 1 10
	
	@Test
	public void testCreateMultipleIndex(){
		HashMap<String,BTreeIndex> boats = inConf.getIndex("Boats");
		HashMap<String,BTreeIndex> sailors = inConf.getIndex("Sailors");
		HashMap<String,BTreeIndex> reserves = inConf.getIndex("Reserves");
		
		assert boats != null;
		assert sailors != null;
		assert reserves != null;
		
		BTreeIndex boatsE = boats.get("E");
		BTreeIndex boatsD = boats.get("D");
		BTreeIndex sailorsA = sailors.get("A");
		BTreeIndex sailorsB = sailors.get("B");
		BTreeIndex sailorsC = sailors.get("C");
		BTreeIndex reservesG = reserves.get("G");
		
		System.out.println("Boats.E" + boatsE.toString());
		System.out.println("Boats.D"+ boatsD.toString());
		System.out.println("Sailors.A"+sailorsA.toString());
		System.out.println("Sailors.B"+sailorsB.toString());
		System.out.println("Sailors.C"+sailorsC.toString());
		System.out.println("Reserves.G"+reservesG.toString());
		
	}

}
