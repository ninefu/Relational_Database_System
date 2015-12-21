package ExpressionVisitor;

import java.util.ArrayList;
import java.util.Stack;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * IndexScanExpressionVisitor walks an expression to determine which
 *  parts of the expression can be handled by an index scan on our index
 *  (and combines them into a single high key/low key)
 *   and which parts have to be done separately
 *   (returning that remainder for another selection operator to do)
 *   
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class IndexScanExpressionVisitor implements ExpressionVisitor {
	
	// Per the instructions: if attribute A is indexed,
	// then we can use the index for A >,<,>=,<=,== 42
	// but not for any other form of expression
	private String indexedAttribute;
	
	private Stack<Boolean> containsIndexAttribute;
	private Stack<Boolean> canUseIndex; // checks if current path uses indexable expressions
	private Stack<Double> highResult;
	private Stack<Double> lowResult;
	private ArrayList<Expression> remainderConditions;
	
	public IndexScanExpressionVisitor(String idx) {
		indexedAttribute = idx;
		
		containsIndexAttribute = new Stack<Boolean>();
		canUseIndex = new Stack<Boolean>();
		highResult = new Stack<Double>();
		lowResult = new Stack<Double>();
		remainderConditions = new ArrayList<Expression>();
	}
	
	/**
	 * Find out if we need to use full-scan operator for the selection
	 * @return an ArrayList contains the columns that cannot be processed by index
	 */
	public Expression getRemainderCondition() {
		// We've collected the conditions that can't be found with index.
		// AND them all together.
		if(remainderConditions.size() == 0) {
			return null;
		} else if(remainderConditions.size() == 1) {
			return remainderConditions.get(0);
		} else if(remainderConditions.size() == 2) {
			AndExpression e = new AndExpression();
			e.setLeftExpression(remainderConditions.get(0));
			e.setRightExpression(remainderConditions.get(1));
			return e;
		} else {
			AndExpression e = new AndExpression();
			e.setLeftExpression(remainderConditions.get(0));
			e.setRightExpression(remainderConditions.get(1));
			for(int i = 2; i < remainderConditions.size(); i++) {
				AndExpression next = new AndExpression();
				next.setLeftExpression(e);
				next.setRightExpression(remainderConditions.get(i));
				e = next;
			}
			return e;
		}
	}
	
	/**
	 * Get the lowkey and highkey
	 * @return an double array with length = 2, the first element is the low key and 
	 * the second element is the high key. 
	 */
	public double[] getHighAndLow() {
		double[] highAndLow = new double[2]; // makeshift tuple because java is dumb
		highAndLow[0] = highResult.pop();
		highAndLow[1] = lowResult.pop();
		// on the other end will have to translate between MAX_VALUE/MIN_VALUE and null
		return highAndLow;
	}
	/**
	 * Method to visit InverseExpression and push the numerical result
	 * to the high/low stack.
	 */
	@Override
	public void visit(InverseExpression arg0) {
		arg0.getExpression().accept(this);
		highResult.push(1.0/highResult.pop());
		lowResult.push(1.0/lowResult.pop());
		containsIndexAttribute.push(containsIndexAttribute.pop());
		canUseIndex.push(canUseIndex.pop());
	}

	/**
	 * Method to visit DoubleValue and push it to the high/low stack.
	 */
	@Override
	public void visit(DoubleValue arg0) {
		highResult.push(arg0.getValue());
		lowResult.push(arg0.getValue());
		containsIndexAttribute.push(false);
		canUseIndex.push(true);
	}
	
	/**  
	 * Method to visit LongValue and push it to the high/low stack.
	 */
	@Override
	public void visit(LongValue arg0) {
		highResult.push((double)arg0.getValue());
		lowResult.push((double)arg0.getValue());
		containsIndexAttribute.push(false);
		canUseIndex.push(true);
	}

	/**
	 * Method to visit parenthesis. Visit the expression within the parenthesis.
	 */
	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
	}

	/**
	 * Method to visit addition. Visit the left side and the right side of the
	 * addition and push the summation to the high and low stacks.
	 * Also, check whether either side is indexable.
	 */
	@Override
	public void visit(Addition arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		highResult.push(highResult.pop() + highResult.pop());
		lowResult.push(lowResult.pop() + lowResult.pop());
		containsIndexAttribute.push(containsIndexAttribute.pop() || containsIndexAttribute.pop());
		canUseIndex.push(canUseIndex.pop() && canUseIndex.pop());
	}
	
	/**
	 * Method to visit division. Visit the numerator and denominator, then push
	 * the division result to the high/low stack;
	 */
	@Override
	public void visit(Division arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		highResult.push(1/highResult.pop() * highResult.pop());
		lowResult.push(1/lowResult.pop() * lowResult.pop());
		containsIndexAttribute.push(containsIndexAttribute.pop() || containsIndexAttribute.pop());
		canUseIndex.push(canUseIndex.pop() && canUseIndex.pop());
	}
	
	/**
	 * Method to visit multiplication. Visit the left side and the right side, then
	 * push the multiplication result to the high/low stack.
	 */
	@Override
	public void visit(Multiplication arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		highResult.push(highResult.pop() * highResult.pop());
		lowResult.push(lowResult.pop() * lowResult.pop());
		containsIndexAttribute.push(containsIndexAttribute.pop() || containsIndexAttribute.pop());
		canUseIndex.push(canUseIndex.pop() && canUseIndex.pop());
	}

	/**
	 * Method to visit subtraction. Visit the left side and the right side of the 
	 * subtraction, then push the result to the high/low stacks.
	 */
	@Override
	public void visit(Subtraction arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		highResult.push(- highResult.pop() + highResult.pop());	
		lowResult.push(- lowResult.pop() + lowResult.pop());	
		containsIndexAttribute.push(containsIndexAttribute.pop() || containsIndexAttribute.pop());
		canUseIndex.push(canUseIndex.pop() && canUseIndex.pop());
	}
	
	/**
	 * Method to visit AndExpression. 
	 * Visit the left side and the right side of the AndExpression
	 * If only one side is indexable, carry over that side's H/L bounds
	 * If both are indexable, take the higher low and lower high.
	 */
	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		boolean rightCont = containsIndexAttribute.pop();
		boolean leftCont = containsIndexAttribute.pop();
		boolean rightValid = canUseIndex.pop();
		boolean leftValid = canUseIndex.pop();
		if(rightCont && leftCont && rightValid && leftValid) {
			double rh = highResult.pop();
			double lh = highResult.pop();
			double rl = lowResult.pop();
			double ll = lowResult.pop();
			highResult.push(Math.min(rh, lh));
			lowResult.push(Math.max(rl, ll));
		} else if(rightCont && rightValid) {
			double rh = highResult.pop();
			highResult.pop();
			highResult.push(rh);
			double rl = lowResult.pop();
			lowResult.pop();
			lowResult.push(rl);
		} else if(leftCont && leftValid) {
			highResult.pop();
			highResult.push(highResult.pop());
			lowResult.pop();
			lowResult.push(lowResult.pop());
		} else {
			highResult.pop(); highResult.pop();
			lowResult.pop(); lowResult.pop();
			highResult.push(Double.MAX_VALUE);
			lowResult.push(-Double.MAX_VALUE);
		}
		containsIndexAttribute.push(rightCont || leftCont);
		canUseIndex.push(rightValid || leftValid);
	}

	/**
	 * Method to visit OrExpression. Never indexable.
	 */
	@Override
	public void visit(OrExpression arg0) {
		
		// OR is not supported so we're gonna treat it like !=
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		containsIndexAttribute.push(containsIndexAttribute.pop() || containsIndexAttribute.pop());
		canUseIndex.push(false);
		highResult.pop(); highResult.pop();
		highResult.push(Double.MAX_VALUE);
		lowResult.pop(); lowResult.pop();
		lowResult.push(-Double.MAX_VALUE);
	}

	/**
	 * Method to visit EqualsTo. 
	 * Exactly one side should contain the attribute indexed on.
	 * Use the other side to set the bounds.
	 * If neither side does, add this to the remaining unindexed expression.
	 */
	@Override
	public void visit(EqualsTo arg0) {
		arg0.getLeftExpression().accept(this);;
		arg0.getRightExpression().accept(this);;
		boolean rightCont = containsIndexAttribute.pop();
		boolean leftCont = containsIndexAttribute.pop();
		boolean rightValid = canUseIndex.pop();
		boolean leftValid = canUseIndex.pop();
		if(leftCont && leftValid) {
			double rh = highResult.pop();
			highResult.pop(); // throw out left
			highResult.push(rh);
			double rl = lowResult.pop();
			lowResult.pop();
			lowResult.push(rl);
		} else if(rightCont && rightValid) {
			highResult.pop();
			highResult.push(highResult.pop());
			lowResult.pop();
			lowResult.push(lowResult.pop());
		} else {
			// what goes here
			highResult.pop(); highResult.pop();
			highResult.push(Double.MAX_VALUE);
			lowResult.pop(); lowResult.pop();
			lowResult.push(-Double.MAX_VALUE);
		}
		containsIndexAttribute.push(rightCont || leftCont);
		canUseIndex.push(rightValid && leftValid);
		if(!canUseIndex.peek() || !containsIndexAttribute.peek()) {
			remainderConditions.add(arg0);
		}
	}

	/**
	 * Method to visit GreaterThan. Exactly one side should contain the attribute indexed on.
	 * Use the other side to set the bounds.
	 * If neither side does, add this to the remaining unindexed expression.
	 */
	@Override
	public void visit(GreaterThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		boolean rightCont = containsIndexAttribute.pop();
		boolean leftCont = containsIndexAttribute.pop();
		boolean rightValid = canUseIndex.pop();
		boolean leftValid = canUseIndex.pop();
		// note: IndexScanOperator is inclusive on high/low key
		// and it assumes integer keys
		// so for exclusive round highs down and lows up
		if(leftCont && leftValid) {
			double rh = highResult.pop();
			highResult.pop(); // throw out left
			highResult.push(Double.MAX_VALUE);
			double rl = lowResult.pop();
			lowResult.pop();
			lowResult.push(Math.ceil(rl));
		} else if(rightCont && rightValid) {
			highResult.pop(); highResult.pop();
			highResult.push(Double.MAX_VALUE);
			lowResult.pop();
			lowResult.push(Math.ceil(lowResult.pop()));
		} else {
			// what goes here
			highResult.pop(); highResult.pop();
			highResult.push(Double.MAX_VALUE);
			lowResult.pop(); lowResult.pop();
			lowResult.push(-Double.MAX_VALUE);
		}	
		containsIndexAttribute.push(rightCont || leftCont);
		canUseIndex.push(rightValid && leftValid);
		if(!canUseIndex.peek() || !containsIndexAttribute.peek()) {
			remainderConditions.add(arg0);
		}
	}

	/**
	 * Method to visit GreaterThanEquals. Exactly one side should contain the attribute indexed on.
	 * Use the other side to set the bounds.
	 * If neither side does, add this to the remaining unindexed expression.
	 */
	@Override
	public void visit(GreaterThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		boolean rightCont = containsIndexAttribute.pop();
		boolean leftCont = containsIndexAttribute.pop();
		boolean rightValid = canUseIndex.pop();
		boolean leftValid = canUseIndex.pop();
		// note: IndexScanOperator is inclusive on high/low key
		// and it assumes integer keys
		// so for exclusive round highs down and lows up
		if(leftCont && leftValid) {
			double rh = highResult.pop();
			highResult.pop(); // throw out left
			highResult.push(Double.MAX_VALUE);
			double rl = lowResult.pop();
			lowResult.pop();
			lowResult.push(rl);
		} else if(rightCont && rightValid) {
			highResult.pop(); highResult.pop();
			highResult.push(Double.MAX_VALUE);
			lowResult.pop();
			lowResult.push(lowResult.pop());
		} else {
			// what goes here
			highResult.pop(); highResult.pop();
			highResult.push(Double.MAX_VALUE);
			lowResult.pop(); lowResult.pop();
			lowResult.push(-Double.MAX_VALUE);
		}
		containsIndexAttribute.push(rightCont || leftCont);
		canUseIndex.push(rightValid && leftValid);
		if(!canUseIndex.peek() || !containsIndexAttribute.peek()) {
			remainderConditions.add(arg0);
		}
	}
	
	/**
	 * Method to visit MinorThan. Exactly one side should contain the attribute indexed on.
	 * Use the other side to set the bounds.
	 * If neither side does, add this to the remaining unindexed expression.
	 */
	@Override
	public void visit(MinorThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		boolean rightCont = containsIndexAttribute.pop();
		boolean leftCont = containsIndexAttribute.pop();
		boolean rightValid = canUseIndex.pop();
		boolean leftValid = canUseIndex.pop();
		// note: IndexScanOperator is inclusive on high/low key
		// and it assumes integer keys
		// so for exclusive round highs down and lows up
		if(leftCont && leftValid) {
			double rh = highResult.pop();
			highResult.pop(); // throw out left
			highResult.push(Math.floor(rh));
			double rl = lowResult.pop();
			lowResult.pop();
			lowResult.push(-Double.MAX_VALUE);
		} else if(rightCont && rightValid) {
			highResult.pop(); 
			highResult.push(Math.floor(highResult.pop()));
			lowResult.pop(); lowResult.pop();
			lowResult.push(-Double.MAX_VALUE);
		} else {
			// what goes here
			highResult.pop(); highResult.pop();
			highResult.push(Double.MAX_VALUE);
			lowResult.pop(); lowResult.pop();
			lowResult.push(-Double.MAX_VALUE);
		}
		containsIndexAttribute.push(rightCont || leftCont);
		canUseIndex.push(rightValid && leftValid);
		if(!canUseIndex.peek() || !containsIndexAttribute.peek()) {
			remainderConditions.add(arg0);
		}
		
	}

	/**
	 * Method to visit MinorThanEquals. Exactly one side should contain the attribute indexed on.
	 * Use the other side to set the bounds.
	 * If neither side does, add this to the remaining unindexed expression.
	 */
	@Override
	public void visit(MinorThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		boolean rightCont = containsIndexAttribute.pop();
		boolean leftCont = containsIndexAttribute.pop();
		boolean rightValid = canUseIndex.pop();
		boolean leftValid = canUseIndex.pop();
		// note: IndexScanOperator is inclusive on high/low key
		// and it assumes integer keys
		// so for exclusive round highs down and lows up
		if(leftCont && leftValid) {
			double rh = highResult.pop();
			highResult.pop(); // throw out left
			highResult.push(rh);
			double rl = lowResult.pop();
			lowResult.pop();
			lowResult.push(-Double.MAX_VALUE);
		} else if(rightCont && rightValid) {
			highResult.pop(); 
			highResult.push(highResult.pop());
			lowResult.pop(); lowResult.pop();
			lowResult.push(-Double.MAX_VALUE);
		} else {
			// what goes here
			highResult.pop(); highResult.pop();
			highResult.push(Double.MAX_VALUE);
			lowResult.pop(); lowResult.pop();
			lowResult.push(-Double.MAX_VALUE);
		}
		containsIndexAttribute.push(rightCont || leftCont);
		canUseIndex.push(rightValid && leftValid);
		if(!canUseIndex.peek() || !containsIndexAttribute.peek()) {
			remainderConditions.add(arg0);
		}
	}

	/**
	 * Method to visit NotEqualsTo. We can never use an index for this.
	 * Just go ahead and add it to the ones we can't index.
	 */
	@Override
	public void visit(NotEqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		containsIndexAttribute.push(containsIndexAttribute.pop() || containsIndexAttribute.pop());
		canUseIndex.push(false);	// no matter what, we can't use an index for !=
		highResult.pop(); highResult.pop();
		highResult.push(Double.MAX_VALUE);
		lowResult.pop(); lowResult.pop();
		lowResult.push(-Double.MAX_VALUE);
		if(!canUseIndex.peek() || !containsIndexAttribute.peek()) {
			remainderConditions.add(arg0);
		}
		
	}
	
	/**
	 * Method to visit Column. 
	 * Check whether the column is the attribute we indexed on.
	 * That determines whether everything above it can be done with index.
	 */
	@Override
	public void visit(Column arg0) {
		String ref = arg0.getTable().getAlias();
		if(ref == null) {
			ref = arg0.getTable().getName();
		}
		String column = arg0.getColumnName();
		
		if(column.equals(indexedAttribute)) {
			containsIndexAttribute.push(true);
		} else {
			containsIndexAttribute.push(false);
		}
		canUseIndex.push(true);
		highResult.push(Double.MAX_VALUE);
		lowResult.push(-Double.MAX_VALUE);
	}
	
	/**
	 * Method to visit NullValue. Not supported.
	 */
	@Override
	public void visit(NullValue arg0) {
		throw new UnsupportedOperationException("Do not support null values.");
		
	}

	/**
	 * Method to visit Function. Not supported.
	 */
	@Override
	public void visit(Function arg0) {
		throw new UnsupportedOperationException("Do not support functions.");
		
	}
	
	/**
	 * Method to visit JdbcParameter. Not supported.
	 */
	@Override
	public void visit(JdbcParameter arg0) {
		throw new UnsupportedOperationException("I don't know what this is.");
		
	}

	/**
	 * Method to visit DateValue. Not supported.
	 */
	@Override
	public void visit(DateValue arg0) {
		throw new UnsupportedOperationException("Don't support date values.");
		
	}

	/**
	 * Method to visit TimeValue. Not supported.
	 */
	@Override
	public void visit(TimeValue arg0) {
		throw new UnsupportedOperationException("Don't support time values.");
		
	}

	/** 
	 * Method to visit TimestampValue. Not supported.
	 */
	@Override
	public void visit(TimestampValue arg0) {
		throw new UnsupportedOperationException("Don't support timestamp values.");
		
	}
	
	/**
	 * Method to visit StringValue. Not supported.
	 */
	@Override
	public void visit(StringValue arg0) {
		throw new UnsupportedOperationException("Don't support string values.");
		
	}
	
	/**
	 * Method to visit Between. Not supported.
	 */
	@Override
	public void visit(Between arg0) {
		throw new UnsupportedOperationException("Not sure if we should support this or not.");	
	}

	/**
	 * Method to visit InExpression. Not supported.
	 */
	@Override
	public void visit(InExpression arg0) {
		throw new UnsupportedOperationException("Not sure if we should support this or not.");		
	}

	/** 
	 * Method to visit IsNullExpression. Not supported.
	 */
	@Override
	public void visit(IsNullExpression arg0) {
		throw new UnsupportedOperationException("Do not support null values.");
		
	}

	/**
	 * Method to visit LikeExpression. Not supported.
	 */
	@Override
	public void visit(LikeExpression arg0) {
		throw new UnsupportedOperationException("Do not support 'LIKE'.");
		
	}
	
	/** 
	 * Method to visit Subselect. Not supported.
	 */
	@Override
	public void visit(SubSelect arg0) {
		throw new UnsupportedOperationException("Do not support Subselect");
	}

	/**
	 * Method to visit CaseExpression. Not supported.
	 */
	@Override
	public void visit(CaseExpression arg0) {
		throw new UnsupportedOperationException("Do not support case expressions");
		
	}

	/**
	 * Method to visit WhenClause. Not supported.
	 */
	@Override
	public void visit(WhenClause arg0) {
		throw new UnsupportedOperationException("Do not support 'WHEN'.");
		
	}

	/**
	 * Method to visit ExistsExpression. Not supported.
	 */
	@Override
	public void visit(ExistsExpression arg0) {
		throw new UnsupportedOperationException("Do not support 'EXISTS'.");
		
	}

	/**
	 * Method to visit AllComparisonExpression. Not supported.
	 */
	@Override
	public void visit(AllComparisonExpression arg0) {
		throw new UnsupportedOperationException("I don't know what this is!");	
	}

	/** 
	 * Method to visit AnyComparisonExpression. Not supported.
	 */
	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw new UnsupportedOperationException("I don't know what this is!");	
	}

	/** 
	 * Method to visit Concat. Not supported.
	 */
	@Override
	public void visit(Concat arg0) {
		throw new UnsupportedOperationException("I don't know what this is!");
		
	}

	/**
	 * Method to visit Matches. Not supported.
	 */
	@Override
	public void visit(Matches arg0) {
		throw new UnsupportedOperationException("I don't know what this is!");
		
	}

	/**
	 * Method to visit Bitwise. Not supported.
	 */
	@Override
	public void visit(BitwiseAnd arg0) {
		throw new UnsupportedOperationException("Do not support bitwise AND");
		
	}

	/**
	 *  Method to visit BitwiseOr. Not supported.
	 */
	@Override
	public void visit(BitwiseOr arg0) {
		throw new UnsupportedOperationException("Do not support bitwise OR");
		
	}

	/** 
	 * Method to visit BitwiseXor. Not supported.
	 */
	@Override
	public void visit(BitwiseXor arg0) {
		throw new UnsupportedOperationException("Do not support bitwise XOR");
		
	}
}