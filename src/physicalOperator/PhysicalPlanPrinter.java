package physicalOperator;

public class PhysicalPlanPrinter implements PhysicalPlanVisitor {
	private StringBuilder sb;
	private String dash;
	
	public PhysicalPlanPrinter(){
		sb = new StringBuilder();
		dash = "";
	}

	@Override
	public void visit(BlockNestedLoopJoinOperator arg0) {
		sb.append(dash);
		sb.append("BNLJ[");
		sb.append(arg0.condition().toString());
		sb.append("]");
		sb.append("\n");
		dash += "-";
		arg0.leftChild().accept(this);
		arg0.rightChild().accept(this);
		dash = dash.substring(0,dash.length()-1);
		
	}

	@Override
	public void visit(DuplicateEliminateOperator arg0) {
		sb.append("DupElim");
		dash += "-";
		sb.append("\n");
		arg0.child().accept(this);	
	}

	@Override
	public void visit(ExternalSortOperator arg0) {
		sb.append(dash);
		sb.append("ExternalSort[");
		String[] orders = arg0.getOrder();
		if (orders.length == 0){
			sb.append("null");
		}else{
			for (int i = 0; i < orders.length; i++){
				if (i > 0){
					sb.append(", ");
				}
				sb.append(orders[i]);
			}	
		}
		sb.append("]");
		sb.append("\n");
		dash += "-";	
		arg0.child().accept(this);
		dash = dash.substring(0,dash.length()-1);

	}

	@Override
	public void visit(IndexScanOperator arg0) {
		sb.append(dash);
		sb.append("IndexScan[");
		sb.append(arg0.tableName + ",");
		sb.append(arg0.indexedColumn() + ",");
		sb.append(arg0.getLow() + ",");
		sb.append(arg0.getHigh() + "]");
	}

	@Override
	public void visit(InMemorySortOperator arg0) {
		sb.append(dash);
		sb.append("InMemorySort[");
		String[] orders = arg0.getOrder();
		if (orders.length == 0){
			sb.append("null");
		}else{
			for (int i = 0; i < orders.length; i++){
				if (i > 0){
					sb.append(", ");
				}
				sb.append(orders[i]);
			}	
		}
		sb.append("]");
		sb.append("\n");
		dash += "-";	
		arg0.child().accept(this);
		dash = dash.substring(0,dash.length()-1);
	}

	@Override
	public void visit(ProjectOperator arg0) {
		sb.append(dash);
		sb.append("Project[");
		if (arg0.getColumn().length > 0){
			for (int i = 0; i < arg0.getColumn().length; i++){
				if (i > 0){
					sb.append(", ");
				}
				sb.append(arg0.getColumn()[i]);
			}
		}else{
			sb.append("null");
		}
		sb.append("]");
		sb.append("\n");
		dash += "-";
		arg0.child().accept(this);		

	}

	@Override
	public void visit(SelectOperator arg0) {
		sb.append(dash);
		sb.append("Select[");
		sb.append(arg0.expression().toString());
		sb.append("]");
		sb.append("\n");
		dash += "-";
		arg0.child().accept(this);
		dash = dash.substring(0,dash.length()-1);
//		arg0.child().accept(this);
	}

	@Override
	public void visit(SequentialScanOperator arg0) {
		sb.append(dash);
		sb.append(String.format("TableScan[%s]", arg0.getTableName()));	
		sb.append("\n");
	}

	@Override
	public void visit(SortMergeJoinOperator arg0) {
		sb.append(dash);
		sb.append("SMJ[");
		sb.append(arg0.condition().toString());
		sb.append("]");
		sb.append("\n");
		dash += "-";
		arg0.leftChild().accept(this);
		arg0.rightChild().accept(this);
		dash = dash.substring(0,dash.length()-1);
	}

	@Override
	public void visit(TupleNestedJoinOperator arg0) {
		sb.append(dash);
		sb.append("TNLJ[");
		sb.append(arg0.condition().toString());
		sb.append("]");
		sb.append("\n");
		dash += "-";
		arg0.leftChild().accept(this);
		arg0.rightChild().accept(this);
		dash = dash.substring(0,dash.length()-1);
	}
	
	
	public String toString(){
		return sb.toString();
	}

}
