package physicalOperator;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import database.Tuple;

/**
 * InMemorySortOperator sorts the output of its child operator in an ascending order
 * according to the order of conditions specified in the query's ORDER BY clause.
 *
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */

public class InMemorySortOperator extends SortOperator {
	private ArrayList<Tuple> buffer;
	private int count;
	
	/**
	 * Creates a new sort operator by reading in all the available
	 * tuples from the child operator and sorting them by the 
	 * specified columns.
	 * @param orderbyOrder Array of columns in the order that the tuples
	 * 						should be sorted.
	 * @param c Child operator of this operator.
	 */
	public InMemorySortOperator(String[] orderbyOrder,PhysicalOperator c){
		super(orderbyOrder, c);
		buffer = new ArrayList<Tuple>();
		Tuple current = child.getNextTuple();
		while (current != null){
			buffer.add(current);
			current = child.getNextTuple();
		}
		Collections.sort(buffer, new tupleComparator(order));
		count = 0;
	}
	
	public List<Tuple> getSortedElements() {
		return buffer;
	}
	
	/** 
	 * Method to reset the sort operator to the first tuple.
	 */
	@Override
	public void reset() {
		count = 0;
	}
	
	/** 
	 * Method to read the next tuple from the sorted output of its child
	 * 
	 * @return Tuple
	 * 				the next available tuple output in ascending order
	 */
	@Override
	public Tuple getNextTuple() {
		if (count < buffer.size()){
			Tuple tup= buffer.get(count);
			count++;
			return tup;
		}
		return null;
	}

	/**
	 * Reset the operator to a certain tuple
	 * @param index 
	 * 			a pointer to the tuple
	 */
	@Override
	public void reset(int index) {
		count = index;
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
}
