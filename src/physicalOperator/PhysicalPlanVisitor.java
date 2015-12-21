package physicalOperator;

public interface PhysicalPlanVisitor {

	public void visit(BlockNestedLoopJoinOperator arg0);
	
	public void visit(DuplicateEliminateOperator arg0);
	
	public void visit(ExternalSortOperator arg0);
	
	public void visit(IndexScanOperator arg0);
	
	public void visit(InMemorySortOperator arg0);
	
	public void visit(ProjectOperator arg0);
	
	public void visit(SelectOperator arg0);
	
	public void visit(SequentialScanOperator arg0);
	
	public void visit(SortMergeJoinOperator arg0);
	
	public void visit(TupleNestedJoinOperator arg0);
}
