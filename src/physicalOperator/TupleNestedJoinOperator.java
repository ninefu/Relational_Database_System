package physicalOperator;

import database.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class TupleNestedJoinOperator extends JoinOperator {
	private Tuple curOuter;
	
	/**
	 * Construct a join operator from an outer child (pull tuples from this child in an
	 * outer loop) and an inner child (pull tuples from this child in an outer loop) as well
	 * as a condition (only tuples satisfying the condition will be returned)
	 * @param left Operator where tuples will be on the outside of the join
	 * @param right Operator where tuples will be on the inside of the join
	 * @param c Condition for all tuples to satisfy; null indicates that there is no condition. i.e.
	 * 			the cross product of tuples from the inner and outer operators will be returned.
	 */
	public TupleNestedJoinOperator(PhysicalOperator left, PhysicalOperator right, Expression c) {
		super(left, right, c);
		curOuter = outerChild.getNextTuple();
	}

	/**
	 * reset to the first tuple
	 */
	@Override
	public void reset() {
		super.reset();
		curOuter = outerChild.getNextTuple();
	}

	/**
	 * Returns the next tuple in the cross product of tuples from the inner and 
	 * outer children that satisfies the condition, if one exists.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple inner = null;
		boolean found = false;
		Tuple combinedTuple = null;
		if(curOuter == null) {
			return null;
		}
		while(!found) {
			inner = innerChild.getNextTuple();
			if(inner == null) {
				curOuter = outerChild.getNextTuple();
				if(curOuter == null) {
					return null;
				}
				innerChild.reset();
				inner = innerChild.getNextTuple();
			}
			combinedTuple = combineTuples(curOuter, inner);
			found = checkCondition(combinedTuple);
		}
		return combinedTuple;
	}

	/**
	 * Not implemented
	 */
	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

}
