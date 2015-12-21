package logicalOperator;

import java.util.ArrayList;

/**
 * LogicalPlanPrinter visit the logical operator tree then print them out one
 * layer by one layer
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class LogicalPlanPrinter implements LogicalPlanVisitor {
	StringBuilder sb;
	String dash; //maintains the number of dashes
	
	public LogicalPlanPrinter(){
		sb = new StringBuilder();
		dash = "";
	}

	/**
	 * leaf operator, no longer need to add one more dash
	 */
	@Override
	public void visit(LogicalScan arg0) {
		sb.append(dash);
		sb.append(arg0.toString());
	}

	/**
	 * add one dash for its child
	 */
	@Override
	public void visit(LogicalSort arg0) {
		sb.append(dash);
		sb.append(arg0.toString());
		sb.append("\n");
		dash += "-";
		arg0.child().accept(this);
	}

	/**
	 * take one dash out after visiting its child
	 */
	@Override
	public void visit(LogicalSelect arg0) {
		sb.append(dash);
		sb.append(arg0.toString());
		sb.append("\n");
		dash += "-";
		arg0.child().accept(this);
		dash = dash.substring(0,dash.length()-1);
	}

	/**
	 * add one dash for its child
	 */
	@Override
	public void visit(LogicalProject arg0) {
		sb.append(dash);
		sb.append(arg0.toString());
		sb.append("\n");
		dash += "-";
		arg0.child().accept(this);
	}

	/**
	 * add one dash and visit its children one by one
	 */
	@Override
	public void visit(LogicalJoin arg0) {
		if (arg0.childSize() > 1){
			sb.append(dash);
			sb.append(arg0.toString());
			sb.append("\n");
			dash += "-";
		}
		ArrayList<LogicalOperator> children = arg0.getChildren();
		for (int i = 0; i < children.size(); i++){
			children.get(i).accept(this);
			sb.append("\n");
		}
	}

	/**
	 * add one dash
	 */
	@Override
	public void visit(LogicalDistinct arg0) {
		sb.append(arg0.toString());
		dash += "-";
		sb.append("\n");
		arg0.child().accept(this);
	}
	
	public String toString(){
		return sb.toString();
	}
}
