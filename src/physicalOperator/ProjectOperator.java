package physicalOperator;

import database.Tuple;

/**
 * ProjectOperator projects the output tuple of its child operator to a new tuple
 * only with columns specified in the query's SELECT clause.
 *
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */

public class ProjectOperator extends PhysicalOperator {
	
	private String[] columnNames;
	private PhysicalOperator child;
	
	/**
	 * Constructor of a ProjectOperator.
	 * @param columns List of column names to project of the form [Table Name].[Column Name]
	 * @param c Child operator of this operator
	 */
	public ProjectOperator(String[] columns, PhysicalOperator c) {
		columnNames = columns;
		child = c;
	}

	/** Method to reset the sort operator to the first tuple.
	 */
	@Override
	public void reset() {
		child.reset();	
	}

	public String[] getColumn(){
		return columnNames;
	}
	
	public PhysicalOperator child(){
		return child;
	}
	
	/**
	 * Method to read the next tuple from its child and project to desired columns.
	 * 
	 * @return Tuple
	 * 				a projected tuple from its child's output
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple tup = child.getNextTuple();
		if(tup == null) {
			return null;
		}
		int[] newCols = new int[columnNames.length];
		int index = 0;
		for(String col : columnNames) {
			newCols[index] = tup.getValueAtField(col);
			index++;
		}
		
		return new Tuple(newCols, columnNames);
	}

	/**
	 * Not implemented
	 */
	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int V(String attribute) {
		throw new UnsupportedOperationException("V value can not be retrieved from a projection operator.");
	}

	@Override
	public int relationSize() {
		return child.relationSize();
	}

	@Override
	public int attributeValLow(String attribute) {
		return child.attributeValLow(attribute);
	}

	@Override
	public int attributeValHigh(String attribute) {
		return child.attributeValHigh(attribute);
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
}
