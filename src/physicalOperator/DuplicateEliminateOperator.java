package physicalOperator;

import java.util.Arrays;

/**
 * DuplicateEliminateOperator eliminates duplicates from the sorted tuple output
 * of its child operator.
 *
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
import database.Tuple;

/**
 * DuplicateEliminateOperator eliminates duplicated tuples from its child operator
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class DuplicateEliminateOperator extends PhysicalOperator {
	private Tuple prev;
	private SortOperator child;
	
	public DuplicateEliminateOperator(SortOperator op){
		prev = null;
		child = op;
	}

	/** 
	 * Method to reset the sort operator to the first tuple.
	 */
	@Override
	public void reset() {
		child.reset();
		prev = null;
	}

	/** 
	 * Method to read the next tuple from the sorted output of its child
	 * and eliminate duplicates if found any.
	 * 
	 * @return Tuple
	 * 				the next distinct tuple or null if the next tuple is 
	 * 				the same as the previous one.
	 */
	@Override
	public Tuple getNextTuple() {	
		Tuple tup = null;
		Boolean same = true;
		while (same){
			tup = child.getNextTuple();
			if (tup == null){
				prev = null;
				return null;
			} else if(prev == null) {
				prev = tup;
				return tup;
			} else {
				same = Arrays.equals(prev.getValues(), tup.getValues());
			}
		}
		prev = tup;
		return tup;
	}

	/**
	 * Not implemented
	 */
	@Override
	public void reset(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int V(String attribute) {
		throw new UnsupportedOperationException("Can't get the V-value on a duplicate elimination operator");
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
	
	public SortOperator child(){
		return child;
	}

}
