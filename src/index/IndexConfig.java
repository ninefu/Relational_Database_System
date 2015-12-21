package index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import database.DatabaseCatalog;
import database.Table;

/**
 * IndexConfig specifies the requirements for indexs, including which table
 * and which column to be indexed, whether the index is clustered or unclustered,
 * and the order of each BTree Node.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class IndexConfig {
//	private HashMap<String, BTreeIndex> indexes;
	private HashMap<String, HashMap<String, BTreeIndex>> indexes;
	private HashMap<String, HashMap<String, BTreeIndex>> clusteredIndex;
	private HashMap<String, HashMap<String, BTreeIndex>> unclusteredIndex;
	
	/**
	 * Constructor
	 * @param db, DatabaseCatalog
	 */
	public IndexConfig(DatabaseCatalog db) {
		File toParse = new File(db.getInputDir(), "/db/index_info.txt");
//		indexes = new HashMap<String, BTreeIndex>();
		indexes = new HashMap<String, HashMap<String, BTreeIndex>>();
		clusteredIndex = new HashMap<String, HashMap<String, BTreeIndex>>();
		unclusteredIndex = new HashMap<String, HashMap<String, BTreeIndex>>();
		parseConfig(toParse, db);	
	}
	
	/**
	 * Convenient constructor for creating only one index not from a file.
	 * Mostly for testing
	 * @param db, Database Catalog
	 * @param table, the table to create index on
	 * @param col, the column to be indexed
	 * @param clusterFlag, 0 or 1, whether the index should be clustered or not
	 * @param order, the number of order for each node
	 */
	public IndexConfig(DatabaseCatalog db, Table table, String col, int clusterFlag, int order) {
//		indexes = new HashMap<String, BTreeIndex>();
		indexes = new HashMap<String, HashMap<String, BTreeIndex>>();
		clusteredIndex = new HashMap<String, HashMap<String, BTreeIndex>>();
		unclusteredIndex = new HashMap<String, HashMap<String, BTreeIndex>>();
		BTreeIndex index = new BTreeIndex(db, table, col, clusterFlag, order);
		index.bulkLoad();
		HashMap<String, BTreeIndex> subindex = new HashMap<String, BTreeIndex>();
		subindex.put(col.split("\\.")[1],index);
		if (clusterFlag == 1){
			clusteredIndex.put(table.getName(), subindex);
		}else{
			unclusteredIndex.put(table.getName(), subindex);
		}
//        indexes.put(table.getName(), subindex);
	}
	
	// Convenient constructor for creating indexs for two tables
	// testing only
	public IndexConfig(DatabaseCatalog db, Table table1, Table table2, String col1, String col2, int clus1, int clus2, int order1, int order2){
//		indexes = new HashMap<String,BTreeIndex>();
		indexes = new HashMap<String, HashMap<String, BTreeIndex>>();
		clusteredIndex = new HashMap<String, HashMap<String, BTreeIndex>>();
		unclusteredIndex = new HashMap<String, HashMap<String, BTreeIndex>>();
		
		BTreeIndex index1 = new BTreeIndex(db,table1,col1,clus1,order1);
		BTreeIndex index2 = new BTreeIndex(db,table2,col2,clus2,order2);
		index1.bulkLoad();
		index2.bulkLoad();
		
		HashMap<String, BTreeIndex> subindexOne = new HashMap<String, BTreeIndex>();
		HashMap<String, BTreeIndex> subindexTwo = new HashMap<String, BTreeIndex>();
		subindexOne.put(col1, index1);
		subindexTwo.put(col2, index2);
//		indexes.put(table1.getName(), subindexOne);
//		indexes.put(table2.getName(), subindexTwo);
		if (clus1 == 1){
			clusteredIndex.put(table1.getName(), subindexOne);
		}else{
			unclusteredIndex.put(table1.getName(), subindexOne);
		}
		
		if (clus2 == 1){
			clusteredIndex.put(table2.getName(), subindexTwo);
		}else{
			unclusteredIndex.put(table2.getName(), subindexTwo);
		}
	}
	
	/**
	 * Get the set of BTreeIndexes for a table
	 * @param tableName
	 * @return a set of BTreeIndexes of the table, null if no index is built on this table
	 */
	public HashMap<String, BTreeIndex> getIndex(String tableName) {
//		if (indexes != null && indexes.containsKey(tableName)){
//			return indexes.get(tableName);
//		}
//		return null;
		HashMap<String, BTreeIndex> ind = new HashMap<String, BTreeIndex>();
		if (clusteredIndex.containsKey(tableName)){
			ind.putAll(clusteredIndex.get(tableName));
		}
		if (unclusteredIndex.containsKey(tableName)){
			ind.putAll(unclusteredIndex.get(tableName));
		}
		return ind;
	}
	
	/**
	 * Get the BTreeIndex for a column in a table
	 * @param tableName, column name
	 * @return a  BTreeIndexes of the table, null if no index is built on this table
	 */
	public BTreeIndex getIndex(String tableName,String colName) {
		if (indexes != null && indexes.containsKey(tableName)){
			if (indexes.get(tableName).containsKey(colName)){
				return indexes.get(tableName).get(colName);
			}
		}
		return null;
	}
	
	/**
	 * Parse the "index_info.txt" file, build index accordingly if the fourth line in
	 * "interpreter_config_file.txt" is 1 or read indexes from /db/indexes/ if it's 0. 
	 * @param f representing "index_info.txt"
	 * @param db, the DatabaseCatalog
	 */
	private void parseConfig(File f, DatabaseCatalog db) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(f));
			ArrayList<String[]> clustered = new ArrayList<String[]>();
			ArrayList<String[]> unclustered = new ArrayList<String[]>();
			
		    String line;
		    while((line = br.readLine()) != null){
		        String[] tokens = line.split(" ");
		        if(tokens.length != 4) {
		        	System.err.println(String.format("Malformed index_info.txt line: %s", line));
		        	br.close();
		        	return;
		        }
		        
		        // if Integer.parseInt(tokens[2]) == 1, put it in the clustered group
		        if (Integer.parseInt(tokens[2]) == 1){
		        	clustered.add(tokens);
			    //else, put it in the unclustered group
		        }else{
		        	unclustered.add(tokens);
		        }
		    }
		    
		    // build clustered index first
		    for (String[] token : clustered){
		    	String tableName = token[0];
		    	Table table = db.getTable(tableName);
		    	String column = String.format("%s.%s", tableName,token[1]);
		    	BTreeIndex index = new BTreeIndex(db,table,column,Integer.parseInt(token[2]),Integer.parseInt(token[3]));
		    	index.bulkLoad();
		    	index.serialize();
		    	if (clusteredIndex.containsKey(tableName)){
		    		HashMap<String, BTreeIndex> temp = clusteredIndex.get(tableName);
		    		temp.put(token[1], index);
		    		clusteredIndex.put(tableName, temp);
		    	}else{
		    		HashMap<String, BTreeIndex> subIn = new HashMap<String, BTreeIndex>();
			    	subIn.put(token[1], index);
			    	clusteredIndex.put(tableName, subIn);
		    	}
		    }
		    
		    // build unclustered index.
		    for (String[] token : unclustered){
		    	String tableName = token[0];
		    	Table table = db.getTable(tableName);
		    	String column = String.format("%s.%s", tableName,token[1]);
		    	BTreeIndex index = new BTreeIndex(db,table,column,Integer.parseInt(token[2]),Integer.parseInt(token[3]));
		    	index.bulkLoad();
		    	index.serialize();
		    	if (unclusteredIndex.containsKey(tableName)){
		    		HashMap<String, BTreeIndex> temp = unclusteredIndex.get(tableName);
		    		temp.put(token[1], index);
		    		unclusteredIndex.put(tableName, temp);
		    	}else{
		    		HashMap<String, BTreeIndex> subIn = new HashMap<String, BTreeIndex>();
			    	subIn.put(token[1], index);
			    	unclusteredIndex.put(tableName, subIn);
		    	}
		    }
		        
		    br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Get the clustered BTreeIndex for a column in a table
	 * @param tableName, column name
	 * @return a clustered BTreeIndexes of the table, null if no index is built on this table
	 */
	public BTreeIndex getClusteredIndex(String tableName,String colName) {
		if (clusteredIndex != null && clusteredIndex.containsKey(tableName)){
			if (clusteredIndex.get(tableName).containsKey(colName)){
				return clusteredIndex.get(tableName).get(colName);
			}
		}
		return null;
	}
	
	/**
	 * Get the unclustered BTreeIndex for a column in a table
	 * @param tableName, column name
	 * @return an unclustered BTreeIndexes of the table, null if no index is built on this table
	 */
	public BTreeIndex getUnClusteredIndex(String tableName,String colName) {
		if (unclusteredIndex != null && unclusteredIndex.containsKey(tableName)){
			if (unclusteredIndex.get(tableName).containsKey(colName)){
				return unclusteredIndex.get(tableName).get(colName);
			}
		}
		return null;
	}
}
