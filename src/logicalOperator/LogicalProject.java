package logicalOperator;


/**
 *  A project operator to build a logical operator tree.
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class LogicalProject extends LogicalOperator{
	private String[] projectColumns;
	private LogicalOperator op;
	
	public LogicalProject(String[] columns, LogicalOperator operator){
		projectColumns = columns;
		op = operator;
	}

	/**
	 * Method to get the child operator
	 * @return LogicalOperator
	 * 				this operator's child
	 */	
	public LogicalOperator child() {
		return op;
	}
	
	/**
	 * Method to get an array of columns where the projection is based on
	 * @return an array of String representing the columns
	 */
	public String[] columns() {
		return projectColumns;
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
		sb.append("Project[");
		if (projectColumns.length == 0){
			sb.append("null");
		}else{
			for (int i = 0; i < projectColumns.length; i++){
				if (i > 0){
					sb.append(", ");
				}
				sb.append(projectColumns[i].toString());
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
