package logicalOperator;

import database.Table;

/**
 * A scan operator to build the logical operator tree
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class LogicalScan extends LogicalOperator {
	private Table table;
	private String alias;
	
	public LogicalScan(Table table, String alias){
		this.table = table;
		this.alias = alias;
	}
	
	/**
	 * Method to get the base table
	 * @return Table
	 * 			a Table item where the scan is based on
	 */
	public Table table() {
		return table;
	}
	
	/**
	 * Method to get the alias for this table
	 * @return String representing the alias
	 */
	public String alias() {
		return alias;
	}

	/**
	 * Method to accept the LogicalPlanVisitor
	 */
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);	
	}
	
	public String toString(){
		return String.format("Leaf[%s]", table.getName());
	}

	@Override
	public LogicalOperator child() {
		return null;
	}

	@Override
	public int childSize() {
		return 0;
	}
}
