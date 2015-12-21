package physicalOperator;

import database.Table;
import database.Tuple;
import util.BinaryTupleReader;
import util.ReadableTupleReader;
import util.TupleReader;

/**
 * ScanOperator keeps track of stream representing the base table that
 * it is supposed to read from. Each call of getNextTuple() reads the next
 * line in the file and constructs a Tuple object from the data. Calling reset
 * will reset the stream to the beginning of the file.
 *
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public abstract class ScanOperator extends PhysicalOperator {
	
	protected String[] schema;
	protected String tableName;
	protected Table table;
	protected String alias;
	protected String tableRef;
	
	public ScanOperator(Table table, String alias) {
		this.table = table;
		this.alias = alias;
		tableName = table.getName();
		schema = table.getQualifiedSchema(alias);
		tableRef = alias == null ? tableName : alias;
	}
	

	public String getAlias(){
		return alias;
	}
	
	public Table getBaseTable(){
		return table;
	}
	
	
	/**
	 * The name of the table that is being scanned
	 * @return String representation of the name of the table being
	 * 			scanned by this operator
	 */
	public String getTableName() {
		return tableName;
	}

	
	public int getHighValue(String attribute) {
		String[] parts = attribute.split("\\.");
		if(!parts[0].equals(tableRef)) { return -1; };
		return table.getHigh(parts[1]);
	}
	
	public int getLowValue(String attribute) {
		String[] parts = attribute.split("\\.");
		if(!parts[0].equals(tableRef)) { return -1; };
		return table.getLow(parts[1]);
	}

	@Override
	public int V(String attribute) {
		String tableRef = alias == null ? tableName : alias;
		String[] parts = attribute.split("\\.");
		if(!parts[0].equals(tableRef)) { return -1; }
		String a = parts[1];
		return table.getHigh(a) - table.getLow(a) + 1;
	}
	
	@Override 
	public int relationSize() {
		return table.getNumTuples();
	}
	
	
	@Override
	public int attributeValLow(String attribute) {
		return getLowValue(attribute);
	}
	
	@Override
	public int attributeValHigh(String attribute) {
		return getHighValue(attribute);
	}

}
