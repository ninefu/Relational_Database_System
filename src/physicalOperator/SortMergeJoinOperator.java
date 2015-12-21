package physicalOperator;

import database.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * SortMergeJoin operator takes in two sorted relations and join/merge them based 
 * on specified columns.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class SortMergeJoinOperator extends JoinOperator {
	private int innerPosition; //inner Partition index
	private Tuple curOuter; //current outer tuple
	private String[] leftOrder; //sort order for the outer relation
	private String[] rightOrder; // sort order for the inner relation

	/**
	 * Constructs a join operator that uses the Sort Merge Join algorithm and 
	 * joins the tuples from the left (outer) and right (inner) operators according
	 * to the Expression condition. 
	 * @param left
	 * @param right
	 * @param cond
	 * @param leftO
	 * @param rightO
	 */
	public SortMergeJoinOperator(PhysicalOperator left, PhysicalOperator right, Expression cond, String[] leftO, String[] rightO) {
		super(left, right, cond);
		innerPosition = 0;
		curOuter = left.getNextTuple();
		leftOrder = leftO;
		rightOrder = rightO;
	}

	/**
	 * Method to get the next tuple that satisfy the condition
	 * @return Tuple
	 * 			 the next tuple
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple combined = null;
		boolean found = false;
		Tuple right = innerChild.getNextTuple();
		if(right == null) {
			curOuter = outerChild.getNextTuple();
			if(curOuter == null) {return null; }
			innerChild.reset(innerPosition);
			right = innerChild.getNextTuple();
		}
		if (curOuter == null){return null;}
		while (!found){
			int comp = compare(curOuter,right,leftOrder,rightOrder);
			while (comp == -1){
				curOuter = outerChild.getNextTuple();
				if (curOuter == null){return null;}
				//reset the inner child whenever we read a new tuple from the outer child
				innerChild.reset(innerPosition); 
				right = innerChild.getNextTuple();
				comp = compare(curOuter,right,leftOrder,rightOrder);
			}
			while (comp == 1){
				right = innerChild.getNextTuple();
				if (right == null){return null;}
				comp = compare(curOuter,right,leftOrder,rightOrder);
				innerPosition++;
			}
			combined = combineTuples(curOuter,right);
			found = checkCondition(combined);
			if (found == false){
				right = innerChild.getNextTuple();
			}
		}
		return combined;
		
	}

	/**
	 * Method to reset the join operator to the first tuple.
	 */
	@Override
	public void reset() {
		((SortOperator)outerChild).reset();
		((SortOperator)innerChild).reset();
		innerPosition = 0;
		curOuter = outerChild.getNextTuple();
	}
	
	/**
	 * Method to compare two tuples on the given columns. Sorted based on the
	 * order of columns.
	 * @param tuLeft
	 * 				tuple from the outer relation
	 * @param tuRight
	 * 				tuple from the inner relation
	 * @param leftorder
	 * 				attributes in the outer relation that need comparison
	 * @param rightOrder
	 * 				attributes in the inner relation that need comparison
	 * @return integer representing the relationship between the tuples
	 * 			0: equal on the given attributes
	 * 			-1: outer tuple is smaller than the inner tuple on one attribute
	 * 			1: outer tuple is larger than the inner tuple on one attribute
	 */
	public int compare(Tuple tuLeft, Tuple tuRight, String[] leftOrder, String[] rightOrder){
		assert leftOrder.length == rightOrder.length;
		for (int i = 0; i < leftOrder.length; i++){
			int vLeft = tuLeft.getValueAtField(leftOrder[i]);
			int vRight = tuRight.getValueAtField(rightOrder[i]);
			if (vLeft < vRight){
				return -1;
			}else if (vLeft > vRight){
				return 1;
			}
		}
		return 0;
	}

	/**
	 * Method to reset the ith tuple. Not implemented for this operator.
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
