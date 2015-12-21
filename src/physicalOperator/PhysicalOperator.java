package physicalOperator;

import java.io.PrintStream;
import database.Tuple;
import logicalOperator.LogicalPlanVisitor;
import util.TupleWriter;

/**
 * Top-level abstract class for an operator.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */

public abstract class PhysicalOperator {
	/** 
	 * Abstract method to reset the operator to the first tuple.
	 */
	public abstract void reset();
	
	public abstract void reset(int index);
	
	/**
	 * Returns the V-value of a specific attribute for this relation
	 * @param A String representation of the attribute to get the V-value for
	 * @return V-value V(R, A)
	 */
	public abstract int V(String attribute);
	
	public abstract int relationSize();
	
	public abstract int attributeValLow(String attribute);
	
	public abstract int attributeValHigh(String attribute);
	
	public abstract void accept(PhysicalPlanVisitor visitor);
	
	/** 
	 * Abstract method to get the next tuple repeatedly.
	 * 
	 * @return Tuple
	 * 				the next available tuple output
	 */
	public abstract Tuple getNextTuple();
	
	/**
	 * Repeatedly calls getNextTuple() and writes
	 * the toString representation of each to its own line
	 * in a PrintStream.
	 * @param output
	 * 			The PrintStream to write the output of getting all tuples
	 */
	public void dump(PrintStream output) {
		Tuple cur;
		while((cur = getNextTuple()) != null) {
			output.println(cur);
		}
	}
	
	/**
	 * Repeatedly calls getNextTuple() and writes
	 * that tuple with the given TupleWriter
	 * @param output
	 * @return The time, in nanoseconds, taken to complete the dump.
	 */
	public long dump(TupleWriter tw) {
		Tuple cur;
		long start = System.nanoTime();
		while((cur = getNextTuple()) != null) {
			tw.writeTuple(cur);
		}
		long stop = System.nanoTime();
		return stop-start;
	}
	
	/**
	 * Method to get the time
	 * @return long representing the total time
	 */
	public long dump() {
		Tuple cur;
		long start = System.nanoTime();
		while((cur = getNextTuple()) != null) {
			// Do nothing!
		}
		long stop = System.nanoTime();
		return stop-start;
	}
	
}
