package database;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import index.IndexConfig;
import logicalOperator.LogicalOperator;
import logicalOperator.LogicalPlanBuilder;
import logicalOperator.LogicalPlanPrinter;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import physicalOperator.PhysicalOperator;
import physicalOperator.PhysicalPlanBuilder;
import physicalOperator.PhysicalPlanConfig;
import physicalOperator.PhysicalPlanPrinter;
import util.BinaryTupleReader;
import util.BinaryTupleWriter;
import util.TupleWriter;


/**
 * DatabaseCatalog maintains information about a database, including the
 * input directory of data and schema, the output directory where query
 * result should go, the temporary directory where temporary files should go,
 * whether to build indexes, whether to evaluate the queries, and a mapping 
 * of a table name in the schema and its corresponding Table object.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class DatabaseCatalog {
	
	private Map<String, Table> tables;
	private String inputDirectory;
	private String outputDirectory;
	private String queryDirectory;
	private String tempDirectory;
//	private Boolean buildIndex;
//	private Boolean evaluateQuery;
	private IndexConfig indexes;
	private HashMap<String, Integer> numTuples;
	private HashMap<String, Integer> lower;
	private HashMap<String, Integer> upper;
	
	/**
	 * Constructs a DatabaseCatalog that keeps track of where to look for input files, 
	 * where to write output files and temporary files, keep track of all tables
	 * as defined by the schema, and determine whether to build indexes or whether to 
	 * evaluate the SQL queries
	 * @param configureFile
	 * 			A configuration file specifying the input directory, output directory, 
	 * 			temporary sort directory, a flag to indicate whether build indexes or not, 
	 * 			and a flag to indicate whether to evaluate SQL queries or not.
	 */
	public DatabaseCatalog(String configureFile){
		tables = new HashMap<String, Table>();
		
		FileInputStream baseFileStream;
		int flag;
		try {
			//read and parse interpreter_config_file.txt
			baseFileStream = new FileInputStream(configureFile);
			BufferedReader reader = new BufferedReader(new InputStreamReader(baseFileStream));
			inputDirectory = reader.readLine();
			outputDirectory = reader.readLine();
			tempDirectory = reader.readLine();
			
			queryDirectory = new File(inputDirectory, "queries.sql").getPath();
			parseSchemaFile(new File(inputDirectory, "db/schema.txt"));
			reader.close();
			indexes = new IndexConfig(this);
			
			numTuples = new HashMap<String, Integer>();
			lower = new HashMap<String, Integer>();
			upper = new HashMap<String, Integer>();
			//gather statistics
			getStatsFile("stats");
		} catch (FileNotFoundException e) {
			System.out.println("Cannot find the configuration file");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error in reading the configuration file");
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Method to get query file's location.
	 * @return String representing query file's location.
	 */
	public String getQueryDir(){
		return queryDirectory;
	}
	
	/**
	 * Get the db directory in input
	 * @return File representing the /input/db/ directory
	 */
	public File getDatabaseDir() {
		return new File(new File(inputDirectory), "db");
	}
	
	/**
	 * Get the indexes directory to store indexes
	 * @return File representing the /input/db/indexes/ directory
	 */
	public File getIndexesDir() {
		File indexDir = new File(getDatabaseDir(), "indexes");
		indexDir.mkdir();
		return indexDir;
	}
	
	/**
	 * Parses a schema file into Table objects
	 * @param schema 
	 * 			File object representing the schema file
	 */
	private void parseSchemaFile(File schema) {
		try(BufferedReader br = new BufferedReader(new FileReader(schema))) {
		    for(String line; (line = br.readLine()) != null; ) {
		    	line = line.trim();
		    	String[] tokens = line.split(" ");
		    	Table table = new Table(tokens, inputDirectory); 
		    	tables.put(tokens[0], table);
		    }
		} catch (IOException e) {
			System.out.println("Error parsing schema file.");
			e.printStackTrace();
		}
	}
	
	/**
	 * Method to get the directory for temporary files
	 * @return String representing temporary files' location
	 */
	public String getTempDir(){
		return tempDirectory;
	}
	
	/**
	 * Method to get the input directory
	 * @return String representing the input directory
	 */
	public String getInputDir(){
		return inputDirectory;
	}
	
	/**
	 * Returns a File object pointing to the path outputDirection/query[queryCount]
	 * @param queryCount Current number of the query to be output to this file
	 * @return File object pointing to the path outputDirection/query[queryCount]
	 */
	public File getOutputFile(int queryCount) {
		return new File(outputDirectory, String.format("query%s",queryCount));
	}
	
	/**
	 * Returns a File object pointing to the path outputDirection/query[queryCount]_
	 * logicalPlan
	 * @param queryCount Current number of the query to be output to this file
	 * @return File object pointing to the path outputDirection/query[queryCount]_
	 * logicalPlan
	 */
	public File getLogicalFile(int queryCount){
		return new File(outputDirectory, String.format("query%s_logicalplan",queryCount));
	}
	
	/**
	 * Returns a File object pointing to the path outputDirection/query[queryCount]_
	 * physicalPlan
	 * @param queryCount Current number of the query to be output to this file
	 * @return File object pointing to the path outputDirection/query[queryCount]_
	 * physicalPlan
	 */
	public File getPhysicalFile(int queryCount){
		return new File(outputDirectory, String.format("query%s_physicalplan",queryCount));
	}
	
	/**
	 * Method to get all the tables in the database catalog
	 * 
	 * @return Map<String, Table>
	 * 						Mapping from a tables name to the Table object
	 */
	public Map<String,Table> getAllTable(){
		return tables;
	}
	
	/**
	 * Gets the table object associated with the given name
	 * Requires: name is a valid table name for this database
	 * @param name
	 * 			String name of the table to retrieve
	 * @return
	 * 			Table object with name "name"
	 */
	public Table getTable(String name) {
		return tables.get(name);
	}
	
	/**
	 * Method to get the String representation of a DatabaseCatalog
	 * 
	 * @return String
	 * 			String representation of the Table names included in
	 * 			the DatabaseCatalog.
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(Table table : tables.values()) {
			sb.append(table.toString());
			sb.append("\n");
		}
		return sb.toString();
	}
	
	/**
	 * Generate a file describing the stats of the database, including table names,
	 * table attributes, and corresponding maximum and minimum values.
	 * @param FileName
	 * 			String representing the file name
	 * @return File
	 */
	public File getStatsFile(String FileName){
		File stats = new File(inputDirectory + "/db",FileName+".txt");
		FileOutputStream fou;
		try {
			fou = new FileOutputStream(stats);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fou));
			Set<Map.Entry<String,Table>> allTables = tables.entrySet();
			Iterator<Entry<String, Table>> tableIterator = allTables.iterator();
			
			while (tableIterator.hasNext()){
				Map.Entry<String, Table> entry = tableIterator.next();
				String tableName = entry.getKey();
				Table table = entry.getValue();
				String[] schema = entry.getValue().getSchema();
				
				int[] minMax = getMinMax(tableName,schema);
				
				StringBuilder sb = new StringBuilder();
				//append table name
				sb.append(tableName);
				//append total tuple numbers
				sb.append(" " + minMax[0]);
				numTuples.put(tableName, minMax[0]);
				table.setNumTuples(minMax[0]);
				//append min and max of each attribute
				int i = 1;
				while (i+1 < minMax.length){
					table.setHighAndLow(schema[i/2], minMax[i+1], minMax[i]);
					sb.append(" " + schema[i/2]);
					sb.append("," + minMax[i]);
					lower.put(tableName+"." + schema[i/2],minMax[i]);
					sb.append("," + minMax[i+1]);
					upper.put(tableName+"." + schema[i/2],minMax[i+1]);
					i += 2;
				}
				writer.write(sb.toString());
				writer.newLine();
			}
			
			writer.close();
		} catch (FileNotFoundException e) {
			System.out.println("Error occured in finding the file");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error occured while writing to the stats.txt");
			e.printStackTrace();
		}
		return stats;
	}
	
	/**
	 * A helper function to get the stats of a file
	 * @param tableName
	 * @param schema of the table
	 * @return int[], the first value is the total number of tuples in the data file，
	 * and the following values are min and max values of each attribute, in the same order
	 * as the schema.
	 */
	public int[] getMinMax(String tableName, String[] schema){
		assert tableName != null;
		assert schema.length > 0;
		int[] res = new int[schema.length * 2 + 1];
		
		for (int j = 1; j < res.length; j++){
			if (j%2 == 1){
				res[j] = Integer.MAX_VALUE;
			}else{
				res[j] = Integer.MIN_VALUE;
			}
		}
		
		File tableFile = new File(inputDirectory + "/db/data",tableName);
		BinaryTupleReader reader = new BinaryTupleReader(tableFile, schema);
		Tuple cur = reader.readNextTuple();
		int count = 0;
		while (cur != null){
			count += 1;
			int[] values = cur.getValues();
			for (int i = 0; i < values.length; i++){
//				System.out.println("Res length: " + res.length);
//				System.out.println("Values length " + values.length);
//				System.out.println("Current position in values" + i);
//				System.out.println("Current position in res " + (2 * i + 1));
				if (values[i] < res[2 * i + 1]){
					res[2 * i + 1] = values[i];
				}
				if (values[i] > res[2 * i + 2]){
					res[2 * i + 2] = values[i];
				}
			}
			cur = reader.readNextTuple();
		}
		res[0] = count;
		reader.close();
		return res;
	}
	
	/**
	 * Return the hashMap that stores the numbers of tuples in each table
	 * @return HashMap<String, Integer>
	 */
	public HashMap<String, Integer> getAllTotal(){
		return numTuples;
	}
	
	/**
	 * Return the hashMap that stores the lower range for each attribute
	 * @return HashMap<String, Integer>
	 */
	public HashMap<String, Integer> getAllLower(){
		return lower;
	}
	
	/**
	 * Return the hashMap that stores the upper range for each attribute
	 * @return HashMap<String, Integer>
	 */
	public HashMap<String, Integer> getAllUpper(){
		return upper;
	}
	
	/**
	 * get the total number of tuples of a table
	 * @param table, String
	 * @return Integer, number of total tuples
	 */
	public Integer getTotal(String table){
		if (numTuples.containsKey(table)){
			return numTuples.get(table);
		}
		return null;
	}
	
	/**
	 * get the lower bound of an attribute
	 * @param attr, in the format of [Table].[Column], no alias
	 * @return Integer, the lower bound
	 */
	public Integer getLower(String attr){
		if (lower.containsKey(attr)){
			return lower.get(attr);
		}
		return null;
	}
	
	/**
	 * get the upper bound of an attribute
	 * @param attr, in the format of [Table].[Column], no alias
	 * @return Integer, the upper bound
	 */
	public Integer getUpper(String attr){
		if (upper.containsKey(attr)){
			return upper.get(attr);
		}
		return null;
	}
	
	/**
	 * Main method to read in data, schema file, and queries, build or read indexes, and parse the
	 * queries, and return query result.
	 * 
	 * @param args
	 * 				command-line arguments.
	 */
	public static void main(String[] args) {
		DatabaseCatalog db = new DatabaseCatalog(args[0]); // for submission
		
		// build indexes
		IndexConfig indexConfig = new IndexConfig(db);
		
		//execute the query
			String queriesFile = db.getQueryDir();
	
			try {
				//parse it using JSqlParser
				CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
				Statement statement;
				int queryCount = 0;
				while ((statement = parser.Statement()) != null) {
					try{
						queryCount++;
						Select select = (Select) statement;
						PlainSelect ps = (PlainSelect) select.getSelectBody();
						
						// build the logical plan tree and physcial plan
						LogicalPlanBuilder logicalBuilder = new LogicalPlanBuilder(db, ps);
						LogicalOperator logicalPlan = logicalBuilder.getFinalOperator();
						//print out the logical query plan 
						FileOutputStream fouLogical = new FileOutputStream(db.getLogicalFile(queryCount));
						BufferedWriter bwLogical = new BufferedWriter(new OutputStreamWriter(fouLogical));
						LogicalPlanPrinter printer = new LogicalPlanPrinter();
						logicalPlan.accept(printer);
						bwLogical.write(printer.toString());
						bwLogical.close();
						
						PhysicalPlanConfig config = new PhysicalPlanConfig();
						config.setTempDir(db.getTempDir());
						
						PhysicalPlanBuilder physicalBuilder = new PhysicalPlanBuilder(config,indexConfig);
						logicalPlan.accept(physicalBuilder);
						PhysicalOperator physicalPlan = physicalBuilder.getPlan();
						
						//print out the physical query plan
						FileOutputStream fouPhysical = new FileOutputStream(db.getPhysicalFile(queryCount));
						BufferedWriter bwPhysical = new BufferedWriter(new OutputStreamWriter(fouPhysical));
						PhysicalPlanPrinter phyPrinter = new PhysicalPlanPrinter();
						physicalPlan.accept(phyPrinter);
						bwPhysical.write(phyPrinter.toString());
						bwPhysical.close();
						
						Tuple firstTuple = physicalPlan.getNextTuple();
						int columns = 0;
						if(firstTuple != null) {
							columns = firstTuple.getValues().length;
						}
						physicalPlan.reset();
						
						TupleWriter writer = new BinaryTupleWriter(db.getOutputFile(queryCount), columns);
						//TupleWriter hwriter = new ReadableTupleWriter(new File(String.format("%s_humanreadable", db.getOutputFile(queryCount).getPath())));
						double secs = (double)physicalPlan.dump(writer)/1000000000.0;
						//physicalPlan.reset();
						//physicalPlan.dump(hwriter);
						writer.close();
						System.out.println(String.format("query%d completed in %f seconds", queryCount, secs));
						//hwriter.close();
					} catch (Exception e){
						System.err.println(String.format("Exception occured on query%d: %s ", queryCount, statement));
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				System.err.println("Exception occurred during parsing");
				e.printStackTrace();
			}
//		}
	}
	
}
