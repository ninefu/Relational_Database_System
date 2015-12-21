package physicalOperator;

import java.util.Comparator;

import database.Tuple;

public abstract class SortOperator extends PhysicalOperator {
	
	protected String[] order;
	protected PhysicalOperator child;
	
	public SortOperator(String[] orderbyOrder, PhysicalOperator c) {
		order = orderbyOrder;
		child = c;
	}
	
	public String[] getOrder(){
		return order;
	}
	public PhysicalOperator child(){
		return child;
	}
	
	protected class tupleComparator implements Comparator<Tuple> {
		String[] columnOrder;
		
		public tupleComparator(String[] order) {
			columnOrder = order;
		}

		@Override
		public int compare(Tuple o1, Tuple o2) {
			for(String o : columnOrder) {
				int v1 = o1.getValueAtField(o);
				int v2 = o2.getValueAtField(o);
				int ret = v1-v2;
				if(ret != 0) {
					return ret;
				}
			}
			return 0;
		}		
	}

	@Override
	public abstract void reset();
	
	@Override
	public abstract void reset(int index);

	@Override
	public abstract Tuple getNextTuple();
	
	@Override
	public int V(String attribute) {
		return child.V(attribute);
	}
	
	@Override
	public int relationSize() {
		return child.relationSize();
	}
	
	@Override
	public int attributeValLow(String attribute) {
		return child.attributeValLow(attribute);
	}
	
	@Override
	public int attributeValHigh(String attribute) {
		return child.attributeValHigh(attribute);
	}
}
