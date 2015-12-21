package ExpressionVisitor;

import java.util.HashMap;
import java.util.Stack;

import database.Tuple;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
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
 * SelectExpressionVisitor takes a tuple as input and recursively walk the expression
 * to evaluate it to true or false on that tuple.
 *
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class SelectExpressionVisitor implements ExpressionVisitor {
	
	private Tuple tuple;
	private Stack<Double> numericalResult; //a stack to store evaluation results
	private Stack<Boolean> logicalResult;
	private HashMap<String, Integer> refCache;
	
	public SelectExpressionVisitor(Tuple tup) {
		tuple = tup;
		numericalResult = new Stack<Double>();
		logicalResult = new Stack<Boolean>();
		refCache = new HashMap<String, Integer>();
	}
	
	/**
	 * Resets the visitor with a new tuple.
	 * @param tup
	 */
	public void reset(Tuple tup) {
		numericalResult.clear();
		logicalResult.clear();
		tuple = tup;
		// don't clear the refCache!
	}
	
	/**
	 * Method to get the logical result.
	 * 
	 * @return boolean
	 * 				true or false.
	 */
	public boolean getResult() {
		return logicalResult.pop();
	}

	/**
	 * Method to visit InverseExpression and push the inverse result
	 * to the numerical result stack.
	 */
	@Override
	public void visit(InverseExpression arg0) {
		arg0.getExpression().accept(this);
		numericalResult.push(1.0/numericalResult.pop());	
	}

	/**
	 * Method to visit DoubleValue and push it to the numerical result stack.
	 */
	@Override
	public void visit(DoubleValue arg0) {
		numericalResult.push(arg0.getValue());
	}
	
	/**  
	 * Method to visit LongValue and push it to the numerical result stack.
	 */
	@Override
	public void visit(LongValue arg0) {
		numericalResult.push((double)arg0.getValue());	
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
	 * addition and push the summation to the numerical result stack.
	 */
	@Override
	public void visit(Addition arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		numericalResult.push(numericalResult.pop() + numericalResult.pop());	
	}
	
	/**
	 * Method to visit division. Visit the numerator and denominator, then push
	 * the division result to the numerical result stack;
	 */
	@Override
	public void visit(Division arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		numericalResult.push(1/numericalResult.pop() * numericalResult.pop());
	}
	
	/**
	 * Method to visit multiplication. Visit the left side and the right side, then
	 * push the multiplication result to the numerical result stack.
	 */
	@Override
	public void visit(Multiplication arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		numericalResult.push(numericalResult.pop() * numericalResult.pop());
	}

	/**
	 * Method to visit subtraction. Visit the left side and the right side of the 
	 * subtraction, then push the result to the numerical result stack.
	 */
	@Override
	public void visit(Subtraction arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		numericalResult.push(- numericalResult.pop() + numericalResult.pop());	
	}
	
	/**
	 * Method to visit AndExpression. Visit the left side and the right side of the
	 * AndExpression, then push the result to the logical result stack.
	 */
	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		logicalResult.push(logicalResult.pop() && logicalResult.pop());
	}

	/**
	 * Method to visit OrExpression. Visit the left side and the right side of 
	 * the OrExpression, then push the result to the logical result stack.
	 */
	@Override
	public void visit(OrExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		logicalResult.push(logicalResult.pop() || logicalResult.pop());	
	}

	/**
	 * Method to visit EqualsTo. Pop the last two numerical results, check
	 * whether they are the same, and push the logical result to the logical
	 * result stack.
	 */
	@Override
	public void visit(EqualsTo arg0) {
		arg0.getLeftExpression().accept(this);;
		arg0.getRightExpression().accept(this);;
		logicalResult.push(numericalResult.pop().equals(numericalResult.pop()));
	}

	/**
	 * Method to visit GreaterThan. Pop the last two numerical results, check
	 * whether the left one is larger, and push the logical result to the logical 
	 * result stack.
	 */
	@Override
	public void visit(GreaterThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		logicalResult.push(numericalResult.pop() < numericalResult.pop());		
	}

	/**
	 * Method to visit GreaterThanEquals. Pop the last two numerical results, check
	 * whether the left one is larger than or equals to the right one, and 
	 * push the logical result to the logical result stack.
	 */
	@Override
	public void visit(GreaterThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		logicalResult.push(numericalResult.pop() <= numericalResult.pop());
	}
	
	/**
	 * Method to visit MinorThan. Pop the last two numerical results, check
	 * whether the left one is smaller, and push the logical result to 
	 * the logical result stack.
	 */
	@Override
	public void visit(MinorThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		logicalResult.push(numericalResult.pop() > numericalResult.pop());	
		
	}

	/**
	 * Method to visit MinorThanEquals. Pop the last two numerical results, check
	 * whether the left one is smaller than or equals to the right one, and push 
	 * the logical result to the logical result stack.
	 */
	@Override
	public void visit(MinorThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		logicalResult.push(numericalResult.pop() >= numericalResult.pop());	
	}

	/**
	 * Method to visit NotEqualsTo. Pop the last two numerical results, check
	 * whether the left one is different, and push the logical result to 
	 * the logical result stack.
	 */
	@Override
	public void visit(NotEqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		logicalResult.push(!numericalResult.pop().equals(numericalResult.pop()));	
	}
	
	/**
	 * Method to visit Column. Get the table name and the required column, then get
	 * the value at this column in the tuple, and push it to the numerical result.
	 */
	@Override
	public void visit(Column arg0) {
		String key = arg0.toString();
		Integer index = refCache.get(key);
		if(index == null) {
			index = tuple.getIndexOfField(key);
			if(index == -1) {
				throw new RuntimeException(String.format("Referenced unknown field: %s", key));
			}
			refCache.put(key, index);
		}
		numericalResult.push((double)tuple.getValueAtIndex(index));
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
