package physicalOperator;

import ExpressionVisitor.IndexScanExpressionVisitor;
import ExpressionVisitor.SelectExpressionVisitor;
import ExpressionVisitor.SelectSizeExpressionVisitor;
import ExpressionVisitor.VExpressionVisitor;
import database.Tuple;
import net.sf.jsqlparser.expression.Expression;

/**
 * Select Operator selects and return tuples that match the conditions specified
 * by the query in the WHERE clause.
 *
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */

public class SelectOperator extends PhysicalOperator {
	
	private Expression expression;
	private PhysicalOperator child;
	private int relationSize;
	
	public SelectOperator(PhysicalOperator c, Expression ex) {
		child = c;
		expression = ex;
		relationSize = calcRelationSize();
	}

	/** 
	 * Method to reset the select operator to the first tuple.
	 */
	@Override
	public void reset() {
		child.reset();
	}
	
	public PhysicalOperator child(){
		return child;
	}
	
	public Expression expression() {
		return expression;
	}

	/** 
	 * Method to get the next tuple of its child operator and return the tuple 
	 * if it matches the WHERE clause.
	 * 
	 * @return Tuple
	 * 				the next available tuple that matches the WHERE clause.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple tup = null;
		boolean found = false;
		while(!found) {
			tup = child.getNextTuple();
			if(tup != null) {
				SelectExpressionVisitor sev = new SelectExpressionVisitor(tup);
				expression.accept(sev);
				found = sev.getResult();
			} else {
				return null;
			}
		}
		return tup;
	}

	/**
	 * Not implemented
	 */
	@Override
	public void reset(int index) {
		// TODO Auto-generated method stub
		
	}
	
	private double reductionFactor(String attribute) {
		IndexScanExpressionVisitor iev = new IndexScanExpressionVisitor(attribute);
		expression.accept(iev);
		double[] hl = iev.getHighAndLow();
		int range = child.V(attribute);
		Double low = hl[0] == Double.MIN_VALUE ? null : hl[0];
		Double high = hl[1] == Double.MAX_VALUE? null : hl[1];
		double attHigh = child.attributeValHigh(attribute);
		double attLow = child.attributeValLow(attribute);
		if(low == null && high == null) {
			return 1; // No reduction factor, this covers the entire range.
		} else if(low == null) {
			// <= some value
			return (high - attLow + 1)/range;
		} else if(high == null) {
			// >= some value
			return (attHigh - low + 1)/range;
		} else {
			// some value <= x <= some value
			return (high - low + 1)/range;
		}
	}

	@Override
	public int V(String attribute) {
		int range = child.V(attribute);
		if(range == -1) return -1;
		double rf = reductionFactor(attribute);
		return Math.min((int)(range * rf), relationSize);
	}
	
	private int calcRelationSize() {
		SelectSizeExpressionVisitor ssev = new SelectSizeExpressionVisitor(child, expression);
		expression.accept(ssev);
		return ssev.getSelectionSize();
	}

	@Override
	public int relationSize() {
		return relationSize;
	}

	@Override
	public int attributeValLow(String attribute) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int attributeValHigh(String attribute) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

}
