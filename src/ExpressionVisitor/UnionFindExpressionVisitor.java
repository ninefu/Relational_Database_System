package ExpressionVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Stack;

import UnionFind.UnionFind;
import UnionFind.UnionFindElement;
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
 * UnionFindExpressionVisitor recursively walks through a WHERE condition, then groups
 * attributes that share the same upper bound, lower bound, and equality constraint
 * in a UnionFind data structure. It only supports expressions like attr = attr, 
 * attr =/</<=/>/>= value, and exp AND exp.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class UnionFindExpressionVisitor implements ExpressionVisitor {
	private ArrayList<Expression> remainderJoin;
	private HashMap<String, ArrayList<Expression>> remainderJoins;
	private HashMap<String, ArrayList<Expression>> remainderSelect;
	private Stack<Column> attrs;
	private Stack<Integer> vals;
	private Stack<String> tableRef;
	private UnionFind unionFinds;
	
	public UnionFindExpressionVisitor(){
		remainderJoin = new ArrayList<Expression>();
		remainderJoins = new HashMap<String, ArrayList<Expression>>();
		remainderSelect = new HashMap<String, ArrayList<Expression>>();
		attrs = new Stack<Column>();
		vals = new Stack<Integer>();
		unionFinds = new UnionFind();
		tableRef = new Stack<String>();
	}
	
	/**
	 * Get the UnionFind data structures in this expressionVisitor
	 * @return UnionFind
	 */
	public UnionFind getUnionFind(){
		return unionFinds;
	}
	
	public HashMap<String, ArrayList<Expression>> getNotEqualJoin(){
		return remainderJoins;
	}
	
	/**
	 * Get the notEqual expression for a table
	 * @param tableRef, alias if exists
	 * @return grouped expressions
	 */
	public Expression getRemainderSelect(String tableRef){
		if (!remainderSelect.containsKey(tableRef)){
			return null;
		}else{
			return mergeExpressions(remainderSelect.get(tableRef));
		}
	}
	
	/**
	 * Return unusable expression in this WHERE condition for join. If there are 
	 * more than one expression, they will be grouped by AND keyword
	 * @return an ArrayList contains unusable comparison
	 */
	public Expression getRemainderJoins() {
		assert remainderJoin.size() >= 0;
		return mergeExpressions(remainderJoin);	
	}
	
	/**
	 * Merge an array of Expressions into a single expression
	 * @param ArrayList<Expression>
	 * @return one single Expression
	 */
	public Expression mergeExpressions(ArrayList<Expression> exps){
		if(exps.size() == 0) {
			return null;
		} else if(exps.size() == 1) {
			return exps.get(0);
		} else{
			Iterator<Expression> iter = exps.iterator();
			Expression left = iter.next();
			Expression right = iter.next();
			AndExpression e = new AndExpression();
			e.setLeftExpression(left);
			e.setRightExpression(right);
			while (iter.hasNext()){
				AndExpression next = new AndExpression();
				next.setLeftExpression(e);
				next.setRightExpression(iter.next());
				e = next;
			}
			return e;
		}	
	}
	
	/**
	 * Visit < expression by visiting the left and the right expression,
	 * then processing accordingly. If both left and right are attributes,
	 * then this is an unusable expression.
	 */
	@Override
	public void visit(MinorThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
		Integer rightVal = vals.pop(); // right value
		Integer leftVal = vals.pop(); // left value
		Column rightAttr = attrs.pop(); //right attribute
		Column leftAttr = attrs.pop(); // left attribute
		tableRef.pop();
		tableRef.pop();
		
		//if val < val or attr < attr
		//then this is an unusable comparison for minorThan operand.
		if ((rightVal != (Integer) null && leftVal != (Integer) null) ||
			(rightAttr != null && leftAttr != null)){
			remainderJoin.add(arg0);
		// val < attr
		}else if (leftVal != (Integer) null && rightAttr != null){
			UnionFindElement cur = unionFinds.findUF(rightAttr);
			int lower = cur.getLower();
			if (leftVal > lower){
				cur.setLower(leftVal + 1);
			}
		// attr < val
		}else if (rightVal != (Integer) null && leftAttr != null){
				UnionFindElement cur = unionFinds.findUF(leftAttr);
				int upper = cur.getUpper();
				if (rightVal < upper){
					cur.setUpper(rightVal - 1);
				}
		}
	}
	
	/**
	 * Visit <= expression by visiting the left and the right expression,
	 * then processing accordingly. If both left and right are attributes,
	 * then this is an unusable expression.
	 * 
	 * The difference between MinorThanEquals and MinorThan lies in setting
	 * the values of upper and lower bounds. MinorThanEquals is inclusive
	 * while MinorThan has to +1 or -1 to the value.
	 */
	@Override
	public void visit(MinorThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
		Integer rightVal = vals.pop(); // right value
		Integer leftVal = vals.pop(); // left value
		Column rightAttr = attrs.pop(); //right attribute
		Column leftAttr = attrs.pop(); // left attribute
		tableRef.pop();
		tableRef.pop();
		
		//if both left and right expression are numeric values/attributes,
		//then this is an unusable comparison for minorThan operand.
		if ((rightVal != (Integer) null && leftVal != (Integer) null) ||
			(rightAttr != null && leftAttr != null)){
			remainderJoin.add(arg0);
		// val < attr
		}else if (leftVal != (Integer) null && rightAttr != null){
			UnionFindElement cur = unionFinds.findUF(rightAttr);
			int lower = cur.getLower();
			if (leftVal > lower){
				cur.setLower(leftVal);// the difference between minorThan and minorThanEquals
			}
		// attr < val
		}else if (rightVal != (Integer) null && leftAttr != null){
				UnionFindElement cur = unionFinds.findUF(leftAttr);
				int upper = cur.getUpper();
				if (rightVal < upper){
					cur.setUpper(rightVal);// the difference between minorThan and minorThanEquals
				}
		}
	}
	
	/**
	 * Visit != or <>. Unusable comparison
	 */
	@Override
	public void visit(NotEqualsTo arg0) {
		// unusable
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
		vals.pop(); // right value
		vals.pop(); // left value
		attrs.pop(); //right attribute
		attrs.pop(); // left attribute
		String rightRef = tableRef.pop();
		String leftRef = tableRef.pop();
		
		// attr <> != attr
		if (leftRef != null && rightRef != null){
			remainderJoin.add(arg0);
			ArrayList<Expression> exps = remainderJoins.containsKey(leftRef)? remainderJoins.get(leftRef) : new ArrayList<Expression>();
			exps.add(arg0);
			remainderJoins.put(leftRef,exps);
			exps = remainderJoins.containsKey(rightRef)? remainderJoins.get(rightRef) : new ArrayList<Expression>();
			exps.add(arg0);
			remainderJoins.put(rightRef,exps);
		// attr val
		}else{
			String ref = leftRef != null? leftRef : rightRef;
			if (remainderSelect.containsKey(ref)){
				ArrayList<Expression> temp = remainderSelect.get(ref);
				temp.add(arg0);
				remainderSelect.put(ref, temp);
			}else{
				ArrayList<Expression> cur = new ArrayList<Expression>();
				cur.add(arg0);
				remainderSelect.put(ref, cur);
			}
		}
	}
		
			
	
	/**
	 * Visit AndExpression
	 */
	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
	}
	
	/**
	 * Visit = by visiting the left and the right expression,
	 * then processing accordingly. Only supports expressions like attr = attr, 
	 * attr = val, and val = attr. Does not support val = val;
	 */
	@Override
	public void visit(EqualsTo arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
		Integer rightVal = vals.pop(); // right value
		Integer leftVal = vals.pop(); // left value
		Column rightAttr = attrs.pop(); //right attribute
		Column leftAttr = attrs.pop(); // left attribute
		tableRef.pop();
		tableRef.pop();
		
		// if both vals are not null, unusable
		if (rightVal != (Integer) null && leftVal != (Integer) null){
			remainderJoin.add(arg0);
		//if att1 = att2, assert vals are null
		}else if (rightAttr != null && leftAttr != null){
			assert rightVal == (Integer) null;
			assert leftVal == (Integer) null;
			UnionFindElement leftEle = unionFinds.findUF(leftAttr);
			UnionFindElement rightEle = unionFinds.findUF(rightAttr);
			unionFinds.union(leftEle, rightEle);
		// attr = val
		}else if (leftAttr != null && rightAttr == null && leftVal == (Integer) null && rightVal != (Integer) null){
			UnionFindElement onlyLeft = unionFinds.findUF(leftAttr);
			onlyLeft.setEquality(rightVal);
		// val = attr
		}else if (rightAttr != null && leftAttr == null && rightVal == (Integer) null && leftVal != (Integer) null){
			UnionFindElement onlyRight = unionFinds.findUF(rightAttr);
			onlyRight.setEquality(leftVal);
		}
		
	}

	/**
	 * Visit > expression by visiting the left and the right expression,
	 * then processing accordingly. If both left and right are attributes,
	 * then this is an unusable expression.
	 */
	@Override
	public void visit(GreaterThan arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
		Integer rightVal = vals.pop(); // right value
		Integer leftVal = vals.pop(); // left value
		Column rightAttr = attrs.pop(); //right attribute
		Column leftAttr = attrs.pop(); // left attribute
		tableRef.pop();
		tableRef.pop();
		
		//if both left and right expression are numeric values/attributes,
		//then this is an unusable comparison for minorThan operand.
		if ((rightVal != (Integer) null && leftVal != (Integer) null) ||
			(rightAttr != null && leftAttr != null)){
			remainderJoin.add(arg0);
		// val > attr
		}else if (leftVal != (Integer) null && rightAttr != null){
			UnionFindElement cur = unionFinds.findUF(rightAttr);
			int upper = cur.getUpper();
			if (leftVal < upper){
				cur.setUpper(leftVal - 1);
			}
		// attr > val
		}else if (rightVal != (Integer) null && leftAttr != null){
				UnionFindElement cur = unionFinds.findUF(leftAttr);
				int lower = cur.getLower();
				if (rightVal > lower){
					cur.setLower(rightVal + 1);
				}
		}
	}

	/**
	 * Visit > expression by visiting the left and the right expression,
	 * then processing accordingly. If both left and right are attributes,
	 * then this is an unusable expression.
	 * 	
	 * The difference between GreaterThanEquals and GreaterThan lies in setting
	 * the values of upper and lower bounds. GreaterThanEquals is inclusive
	 * while GreaterThan has to +1 or -1 to the value.
	 */
	@Override
	public void visit(GreaterThanEquals arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
		Integer rightVal = vals.pop(); // right value
		Integer leftVal = vals.pop(); // left value
		Column rightAttr = attrs.pop(); //right attribute
		Column leftAttr = attrs.pop(); // left attribute
		tableRef.pop();
		tableRef.pop();
		
		
		//if both left and right expression are numeric values/attributes,
		//then this is an unusable comparison for minorThan operand.
		if ((rightVal != (Integer) null && leftVal != (Integer) null) ||
			(rightAttr != null && leftAttr != null)){
			remainderJoin.add(arg0);
		// val >= attr
		}else if (leftVal != (Integer) null && rightAttr != null){
			UnionFindElement cur = unionFinds.findUF(rightAttr);
			int upper = cur.getUpper();
			if (leftVal < upper){
				cur.setUpper(leftVal);
			}
		// attr >= val
		}else if (rightVal != (Integer) null && leftAttr != null){
			UnionFindElement cur = unionFinds.findUF(leftAttr);
			int lower = cur.getLower();
			if (rightVal > lower){
				cur.setLower(rightVal);
			}
		}
	}

	/**
	 * Column is in the format or "[TableName.ColumnName] or [TableAlias.ColumnName]
	 * if the query uses alias
	 */
	@Override
	public void visit(Column arg0) {		
		attrs.push(arg0);
		vals.push((Integer) null);
		
		String ref = arg0.getTable().getAlias();
		if (ref == null){
			ref = arg0.getTable().getName();
		}
		tableRef.push(ref);
	}
	
	/**
	 * push the numerical value to vals and null to attrs
	 */
	@Override
	public void visit(DoubleValue arg0) {
		vals.push((int)arg0.getValue());
		attrs.push(null);
		tableRef.push(null);
	}

	/**
	 * push the numerical value to vals and null to attrs
	 */
	@Override
	public void visit(LongValue arg0) {
		vals.push((int)arg0.getValue());
		attrs.push(null);
		tableRef.push(null);

	}
	
	/**
	 * Visit the left and right expression first, then push the summation.
	 */
	@Override
	public void visit(Addition arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
		Column rightAttr = attrs.pop(); //right attribute
		Column leftAttr = attrs.pop(); // left attribute
		assert rightAttr == null && leftAttr == null;
		attrs.push(null);
		
		String rightRef = tableRef.pop();
		String leftRef = tableRef.pop();
		assert rightRef == null && leftRef == null;
		tableRef.push(null);
		
		vals.push(vals.pop() + vals.pop());
	}

	/**
	 * Visit the left and right expression first, then push the division result.
	 */
	@Override
	public void visit(Division arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
		Column rightAttr = attrs.pop(); //right attribute
		Column leftAttr = attrs.pop(); // left attribute
		assert rightAttr == null && leftAttr == null;
		attrs.push(null);
		
		String rightRef = tableRef.pop();
		String leftRef = tableRef.pop();
		assert rightRef == null && leftRef == null;
		tableRef.push(null);
		
		vals.push(1 / vals.pop() * vals.pop()); // 1/right * left = left/right
	}

	/**
	 * Visit the left and right expression first, then push the multiplication.
	 */
	@Override
	public void visit(Multiplication arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
		Column rightAttr = attrs.pop(); //right attribute
		Column leftAttr = attrs.pop(); // left attribute
		assert rightAttr == null && leftAttr == null;
		attrs.push(null);
		
		String rightRef = tableRef.pop();
		String leftRef = tableRef.pop();
		assert rightRef == null && leftRef == null;
		tableRef.push(null);
		
		vals.push(vals.pop() * vals.pop());		
	}

	/**
	 * Visit the left and right expression first, then push the subtraction result.
	 */
	@Override
	public void visit(Subtraction arg0) {
		arg0.getLeftExpression().accept(this);
		arg0.getRightExpression().accept(this);
		
		Column rightAttr = attrs.pop(); //right attribute
		Column leftAttr = attrs.pop(); // left attribute
		assert rightAttr == null && leftAttr == null;
		attrs.push(null);
		
		String rightRef = tableRef.pop();
		String leftRef = tableRef.pop();
		assert rightRef == null && leftRef == null;
		tableRef.push(null);
		
		vals.push(- vals.pop() + vals.pop());		
	}
	
	/**
	 * Unsupported
	 */
	@Override
	public void visit(NullValue arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}
	
	/**
	 * Unsupported
	 */
	@Override
	public void visit(Function arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(InverseExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(JdbcParameter arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(DateValue arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(TimeValue arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(TimestampValue arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(Parenthesis arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(StringValue arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(OrExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(Between arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(InExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(IsNullExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(LikeExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(SubSelect arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(CaseExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(WhenClause arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(ExistsExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(AllComparisonExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(Concat arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(Matches arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(BitwiseAnd arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(BitwiseOr arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

	/**
	 * Unsupported
	 */
	@Override
	public void visit(BitwiseXor arg0) {
		throw new UnsupportedOperationException("This operation is not supported.");		
	}

}
