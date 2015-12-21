/**
 * 
 */
package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import database.DatabaseCatalog;
import database.Tuple;
import physicalOperator.PhysicalPlanConfig.JoinMethod;
import physicalOperator.PhysicalPlanConfig.SortMethod;

/**
 * A class to randomly generate a tuple
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class RandomGenerator {
	private Random random;
	private int tupleNum;
	private int attrNum;
	private int tableNum;
	private int range;
	private File dbDir;
	private File dataDir;
	private File inputDir;
	private File outputDir;
	private File tmpDir;
	private File indexDir;
	private Map<String, String[]> schema;
	
	
	public RandomGenerator(int tablenum, int tuplenum, int attrnum, int rng){
		random = new Random();
		tupleNum = tuplenum;
		attrNum = attrnum;
		range = rng;
		tableNum = tablenum;
		
		inputDir = createOrGetDir("test_tmp/input");
		outputDir = createOrGetDir("test_tmp/output");
		tmpDir = createOrGetDir("test_tmp/tmp");
		dbDir = createOrGetDir("test_tmp/input/db");
		dataDir = createOrGetDir("test_tmp/input/db/data");
		indexDir = createOrGetDir("test_tmp/input/db/indexes");

		schema = new HashMap<String, String[]>();
		String prefix = "A";
		for(int i = 0; i < tableNum; i++) {
			String[] attrs = new String[attrNum];
			for(int j = 0; j < attrNum; j++) {
				attrs[j] = prefix;
				prefix = incPrefix(prefix);
			}
			schema.put("Table" + i, attrs);
		}
	}
	
	/**
	 * Convenience constructor: creates a database structure with two tables, 
	 * each with 4 attributes and 640 tuples each. This will create tables that
	 * consist of ~2.5 pages of relations with values in the range [0..rng).
	 * @param rng Attribute values are randomly generated in the range [0..rng).
	 */
	public RandomGenerator(int rng) {
		this(2, 640, 4, rng);
	}
	
	/** 
	 * Convenience constructor: creates a database structure with two tables, 
	 * each with 4 attributes and 640 tuples each. This will create tables that
	 * consist of ~2.5 pages of relations with values in the range [0..10) for 
	 * a high number of matches when testing joins.
	 */
	public RandomGenerator() {
		this(10);
	}
	
	private File createOrGetDir(String name) {
		File f = new File(name);
		if(!f.exists()) {
			f.mkdirs();
		}
		return f;
	}
	
	private String incPrefix(String pre) {
		for(int i = pre.length() -1; i >= 0; i--) {
			char cur = pre.charAt(i);
			if(cur < 'Z') {
				return pre.substring(0, i) + (char)(cur + 1) + pre.substring(i+1);
			}
		}
		StringBuilder n = new StringBuilder();
		for(int i = 0; i < pre.length() + 1; i++) {
			n.append("A");
		}
		return n.toString();
	}
	
	/**
	 * Writes schema data for however many temp tables are specified by tableNum.
	 * This data will be located at test_tmp/input/db/schema.txt
	 */
	private void writeTempTableSchema() {
		try {
			FileWriter fw = new FileWriter(new File(dbDir, "schema.txt"));
			for(int j = 0; j < tableNum; j++) {
				String[] header = schema.get("Table" + j);
				StringBuilder tableHeader = new StringBuilder();
				tableHeader.append("Table" + j);
				tableHeader.append(" ");
				for(int i = 0; i < attrNum; i++) {
					tableHeader.append(header[i]);
					tableHeader.append(" ");
				}
				fw.write(tableHeader.substring(0, tableHeader.length() -1));
				fw.write('\n');
				tableHeader.setLength(0);
			}
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Writes test data for the table name specified by name "Table[0,tableNum)"
	 * Will write number of tuples specified by tupleNum, and will be written to
	 * test_tmp/input/db/data/[name]
	 * @param name String name of the table to write
	 * @param humanReadable Whether or not the file is human-readable (as opposed to binary)
	 */
	private void writeTempTable(String name, boolean humanReadable) {
		File dest = new File(dataDir, name);
		String[] header = schema.get(name);
		TupleWriter tw;
		if(humanReadable) {
			tw = new ReadableTupleWriter(dest);
		} else {
			tw = new BinaryTupleWriter(dest, attrNum);
		}
		for(int i = 0; i < tupleNum; i++) {
			int[] attrs = new int[attrNum];
			for(int j = 0; j < attrNum; j++) {
				attrs[j] = random.nextInt(range);
			}
			tw.writeTuple(new Tuple(attrs, header));
		}
		if(tw != null) {
			tw.close();
		}
	}
	
	/**
	 * Writes schema and randomly generated data files
	 * for the number of tables specified by tableNum
	 * @param humanReadable Whether or not the data files
	 * 						should be human readable or
	 * 						binary format
	 */
	public void setupTestData(boolean humanReadable) {
		writeTempTableSchema();
		for(int i = 0; i < tableNum; i++) {
			writeTempTable("Table" + i, humanReadable);
		}
	}
	
	public void writeConfigFile(JoinMethod jm, SortMethod sm, int sortpages, int joinpages) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(inputDir, "plan_builder_config.txt")));
			switch(jm) {
			case TNLJ:
				bw.write("0\n");
				break;
			case BNLJ:
				bw.write(String.format("%d %d\n", 1, joinpages));
				break;
			case SMJ:
				bw.write("2\n");
				break;
			}
			switch(sm) {
			case MEMORY:
				bw.write("0\n");
				break;
			case EXTERNAL:
				bw.write(String.format("%d %d\n", 1, sortpages));
				break;
			}
			bw.close();
		} catch (IOException e) {
			System.err.println("Error writing config file.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Deletes all schema and data files in the test_tmp directory.
	 * Does not delete empty directories.
	 */
	public void teardownTestData() {
		File s = new File(dbDir, "schema.txt");
		if(s.exists()) {
			s.delete();
		}
		for(int i = 0; i < tableNum; i++) {
			File f = new File(dataDir, "Table" + i);
			if(f.exists()) {
				f.delete();
			}
		}
	}
	
	/**
	 * Sets up test directory structure and generates random data files. Returns a usable
	 * DatabaseCatalog object that can be used to reference this data
	 * 
	 * @param humanReadable Whether or not the data files should be in human-readable format
	 * @return DatabaseCatalog object consisting of references to the schema and data in the randomly
	 * 							generated files.
	 */
	public DatabaseCatalog getDatabase(boolean humanReadable) {
		setupTestData(humanReadable);
		FileOutputStream fou;
		try {
			fou = new FileOutputStream(new File("randomGeneratorConfig.txt"));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fou));
			bw.write(inputDir.getPath().toString());
			bw.newLine();
			bw.write(outputDir.getPath().toString());
			bw.newLine();
			bw.write(tmpDir.getPath());
			bw.newLine();
			bw.write("0");
			bw.newLine();
			bw.write("1");
			bw.close();
		} catch (FileNotFoundException e) {
			System.out.println("Error in finding randomGeneratorConfig.txt");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error in writing directories to randomGeneratorConfig.txt");
			e.printStackTrace();
		}
//		return new DatabaseCatalog(inputDir.getPath(), outputDir.getPath(), tmpDir.getPath());
		return new DatabaseCatalog("randomGeneratorConfig.txt");
	}
	
	public String toString(){
		return "A relation with " + tupleNum + " tuples, each tuple has " + attrNum + " attributes.";
	}
	
	public static void main(String[] args) {
		RandomGenerator rg = new RandomGenerator(10, 100, 3, 100);
		DatabaseCatalog db = rg.getDatabase(true);
	}
	
}
