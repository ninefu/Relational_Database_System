package testing;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;

import database.DatabaseCatalog;

public class DBCatalogTester {
//	File testConfig = new File("Configuration_test.txt");
	String testConfig = "Configuration_test.txt";
	String[] schemaReserves = new String[]{"G","H"};
	String[] schemaBoats = new String[]{"D","E","F"};
	String[] schemaSailors = new String[]{"A","B","C"};
	DatabaseCatalog db = new DatabaseCatalog(testConfig);

	@Test
	public void testGetMinMax() {
		int[] boatsMM = db.getMinMax("Boats", schemaBoats);
		int[] reservesMM = db.getMinMax("Reserves", schemaReserves);
		int[] sailorsMM = db.getMinMax("Sailors", schemaSailors);
		
		// Found these min and max values by using Excel
		assertEquals(10000,boatsMM[0]);
		assertEquals(0,boatsMM[1]);
		assertEquals(10000,boatsMM[2]);
		assertEquals(0,boatsMM[3]);
		assertEquals(9999,boatsMM[4]);
		assertEquals(1,boatsMM[5]);
		assertEquals(10000,boatsMM[6]);
		
		assertEquals(10000,reservesMM[0]);
		assertEquals(0,reservesMM[1]);
		assertEquals(9999,reservesMM[2]);
		assertEquals(0,reservesMM[3]);
		assertEquals(10000,reservesMM[4]);

		assertEquals(10000,sailorsMM[0]);
		assertEquals(0,sailorsMM[1]);
		assertEquals(9999,sailorsMM[2]);
		assertEquals(0,sailorsMM[3]);
		assertEquals(9999,sailorsMM[4]);
		assertEquals(0,sailorsMM[5]);
		assertEquals(9998,sailorsMM[6]);
	}
	
	@Test
	public void testGetStatsFile() throws IOException{
		File f = db.getStatsFile("stats");
//		 = new File("samples/input/db/stats.txt");
		FileInputStream baseFileStream = new FileInputStream(f);
		BufferedReader reader = new BufferedReader(new InputStreamReader(baseFileStream));
		
		assertEquals("Reserves 10000 G,0,9999 H,0,10000",reader.readLine());
		assertEquals("Boats 10000 D,0,10000 E,0,9999 F,1,10000",reader.readLine());
		assertEquals("Sailors 10000 A,0,9999 B,0,9999 C,0,9998",reader.readLine());
	}
	
	@Test
	public void testMinMaxTotal(){
		assertEquals(10000, (int) db.getAllTotal().get("Reserves"));
		assertEquals(10000, (int) db.getAllTotal().get("Boats"));
		assertEquals(10000, (int) db.getAllTotal().get("Sailors"));
		
		assertEquals(0,(int) db.getAllLower().get("Reserves.G"));
		assertEquals(0,(int) db.getAllLower().get("Reserves.H"));
		
		assertEquals(0,(int) db.getAllLower().get("Boats.D"));
		assertEquals(0,(int) db.getAllLower().get("Boats.E"));
		assertEquals(1,(int) db.getAllLower().get("Boats.F"));
		
		assertEquals(0,(int) db.getAllLower().get("Sailors.A"));
		assertEquals(0,(int) db.getAllLower().get("Sailors.B"));
		assertEquals(0,(int) db.getAllLower().get("Sailors.C"));

		
		assertEquals(9999,(int) db.getAllUpper().get("Reserves.G"));
		assertEquals(10000,(int) db.getAllUpper().get("Reserves.H"));
		
		assertEquals(10000,(int) db.getAllUpper().get("Boats.D"));
		assertEquals(9999,(int) db.getAllUpper().get("Boats.E"));
		assertEquals(10000,(int) db.getAllUpper().get("Boats.F"));
		
		assertEquals(9999,(int) db.getAllUpper().get("Sailors.A"));
		assertEquals(9999,(int) db.getAllUpper().get("Sailors.B"));
		assertEquals(9998,(int) db.getAllUpper().get("Sailors.C"));






		
	}

}
