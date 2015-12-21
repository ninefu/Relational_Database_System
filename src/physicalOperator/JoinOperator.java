package physicalOperator;

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import ExpressionVisitor.SelectExpressionVisitor;
import ExpressionVisitor.VExpressionVisitor;
import database.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * An abstract class for the join operator which joins tuples from its children
 * and output the combined tuple according to the condition.
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 *
 */
public abstract class JoinOperator extends PhysicalOperator {
	
	PhysicalOperator outerChild;
	PhysicalOperator innerChild;
	Expression condition;
	SelectExpressionVisitor sev;
	Set<Entry<String, String>> eqPairs;
	
	public JoinOperator(PhysicalOperator left, PhysicalOperator right, Expression cond) {
		outerChild = left;
		innerChild = right;
		condition = cond;
		sev = new SelectExpressionVisitor(null);
		VExpressionVisitor vev = new VExpressionVisitor();
		condition.accept(vev);
		eqPairs = vev.getEqualityPairs();
	}

	/**
	 * Reset to the first tuple.
	 */
	@Override
	public void reset() {
		outerChild.reset();
		innerChild.reset();
	}
	
	public PhysicalOperator leftChild(){
		return outerChild;
	}
	
	public PhysicalOperator rightChild(){
		return innerChild;
	}
	
	/**
	 * Create a new tuple from the values from two other tuples.
	 * @param outer First tuple to combine (values come first in the resultant tuple)
	 * @param inner Second tuple to combine (values come second in the resultant tuple)
	 * @return A new tuple which includes all of the values in the first tuple
	 * 			as well as the second tuple, with outer tuple's values coming before
	 * 			those of the inner tuple.
	 */
	protected Tuple combineTuples(Tuple outer, Tuple inner) {
		int[] outerVals = outer.getValues();
		int[] innerVals = inner.getValues();
		String[] outerFields = outer.getFields();
		String[] innerFields = inner.getFields();
		
		int[] values = new int[outerVals.length + innerVals.length];
		String[] fields = new String[outerFields.length + innerFields.length];
		int index = 0;
		for(int i = 0; i < outerFields.length; i++) {
			values[index] = outerVals[i];
			fields[index] = outerFields[i];
			index++;
		}
		for(int i = 0; i < innerFields.length; i++) {
			values[index] = innerVals[i];
			fields[index] = innerFields[i];
			index++;
		}
		return new Tuple(values, fields,true);
	}
	
	/**
	 * Checks whether the given tuple satisfies the join's condition.
	 * @param tuple Tuple to check against the join's condition
	 * @return True if the tuple satisfies the join condition, false otherwise.
	 */
	protected boolean checkCondition(Tuple tuple) {
		if(condition != null) {
			sev.reset(tuple);
			condition.accept(sev);
			return sev.getResult();
		} 
		return true;
	}
	
	/**
	 * Returns the V-value of this join relation for the specified attribute.
	 * 
	 */
	@Override
	public int V(String attribute) {
		throw new UnsupportedOperationException("Joins do not support V-values.");
	}

	/**
	 * Implemented in join operators
	 */
	@Override
	public abstract Tuple getNextTuple();

	
	@Override 
	public int relationSize() {
		int numerator = outerChild.relationSize() * innerChild.relationSize();
		int denominator = 1;
		for(Entry<String, String> ep : eqPairs) {
			int v1 = Math.max(outerChild.V(ep.getKey()), innerChild.V(ep.getKey()));
			int v2 = Math.max(outerChild.V(ep.getValue()), innerChild.V(ep.getValue()));
			denominator = denominator * Math.max(v1,  v2);
		}
		return (int)(numerator/denominator);
	}
	
	
	@Override
	public int attributeValLow(String attribute) {
		throw new UnsupportedOperationException("Joins do not support attribute values");
	}
	
	@Override
	public int attributeValHigh(String attribute) {
		throw new UnsupportedOperationException("Joins do not support attribute values");
	}
	
	public Expression condition(){
		return condition;
	}
}
