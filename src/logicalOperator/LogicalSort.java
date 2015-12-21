/**
 * 
 */
package logicalOperator;

import java.util.ArrayList;

/**
 * A sort operator to build the logical operator tree
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class LogicalSort extends LogicalOperator{
	private String[] order;
	private LogicalOperator op;
	private ArrayList<String> exactOrder;
	
	public LogicalSort(String[] columnOrder, LogicalOperator operator, ArrayList<String> shortOrder){
		order = columnOrder;
		op = operator;
		exactOrder = shortOrder;
	}
	
	/**
	 * Method to get the child operator
	 * @return LogicalOperator
	 * 			this operator's child
	 */
	public LogicalOperator child() {
		return op;
	}
	
	/**
	 * Method to get the sort order
	 * @return an array of string representing the sort order
	 * 			if more than one order, the first one is sorted first
	 */
	public String[] order() {
		return order;
	}
	
	public ArrayList<String> getExactOrder(){
		return exactOrder;
	}

	/**
	 * Method to accept the LogicalPlanVisitor
	 */
	@Override
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("Sort[");
		if (exactOrder == null){
			sb.append("null");
		}else{
			for (int i = 0; i < exactOrder.size(); i++){
				if (i > 0){
					sb.append(", ");
				}
				sb.append(exactOrder.get(i).toString());
			}
		}
		sb.append("]");
		
		return sb.toString();
	}

	@Override
	public int childSize() {
		return 1;
	}
}
