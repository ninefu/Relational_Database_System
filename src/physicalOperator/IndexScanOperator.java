package physicalOperator;

import database.Table;
import database.Tuple;
import database.TupleIdentifier;
import index.BTreeIndex;
import index.BTreeLeafNode;
import index.IndexConfig;
import util.BinaryTupleReader;

/**
 * IndexScanOperator use a B+ tree index to search for tuples that in the range 
 * of low key and high key
 *  @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class IndexScanOperator extends ScanOperator {
	private BinaryTupleReader reader;
	private Integer lowKey;
	private Integer highKey;
	private BTreeIndex index;
	private BTreeLeafNode currentLeafNode;
	private Tuple currentTuple;
	private int keyPos; // The index of the key that we are currently on within the current leaf node
	private int tupPos; // Index of the tuple we are currently on within the current key within the current leaf
	
	
//	public IndexScanOperator(Table table, String alias, Integer lo, Integer hi, IndexConfig conf) {
	public IndexScanOperator(Table table, String alias, Integer lo, Integer hi, BTreeIndex ind) {
		super(table, alias);
		lowKey = lo;
		highKey = hi;
		tableName = table.getName();
//		index = conf.getIndex(tableName);
		index = ind;
		schema = table.getQualifiedSchema(alias);
		reader = new BinaryTupleReader(table.getDataFile(), schema);
		keyPos = 0;
		if(lowKey == null) {
			currentLeafNode = index.getSmallestLeafNode();
		} else {
			currentLeafNode = index.deserializeTraversal(lo);
			while(currentLeafNode.getKeyAt(keyPos) < lo) {
				keyPos++;
			}
		}
		tupPos = 0;
		TupleIdentifier first = currentLeafNode.tupleAt(keyPos, tupPos);
		if(index.isClustered()) {
			currentTuple = reader.readAt(first);
		}
	}

	/**
	 * Method to reset the operator
	 */
	@Override
	public void reset() {
		keyPos = 0;
		tupPos = 0;
		if(lowKey == null) {
			currentLeafNode = index.getSmallestLeafNode();
		} else {
			currentLeafNode = index.deserializeTraversal(lowKey);
			while(currentLeafNode.getKeyAt(keyPos) < lowKey) {
				keyPos++;
			}
		}
		TupleIdentifier first = currentLeafNode.tupleAt(keyPos, tupPos);
		if(index.isClustered()) {
			currentTuple = reader.readAt(first);
		}
	}
	
	public Integer getLow(){
		return lowKey;
	}
	
	public Integer getHigh(){
		return highKey;
	}
	
	/**
	 * Not implemented
	 */
	@Override
	public void reset(int index) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Get the next tuple from the operator
	 * @return Tuple representing the next tuple from the operator
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple res = null;
		boolean found = false;
		while(!found) {
			//unclustered
			if(!index.isClustered()) {
				if(keyPos >= currentLeafNode.numKeys()) {
					if(currentLeafNode.address() + 1 > index.numLeaves()) {
						return null; // We've run out of leaves to look at
					}
					// TODO: get rid of more terrible casting
					currentLeafNode = (BTreeLeafNode) index.deserializeNode(currentLeafNode.address() + 1);
					keyPos = 0;
					tupPos = 0;
					continue;
				}
				if(highKey != null && currentLeafNode.getKeyAt(keyPos) > highKey) {
					return null; // We've exceeded the high end of the range
				}
				if(tupPos >= currentLeafNode.numEntries(keyPos)) {
					keyPos++;
					tupPos = 0;
					continue;
				}
				TupleIdentifier rid = currentLeafNode.tupleAt(keyPos, tupPos);
				tupPos++;
				res = reader.readAt(rid);
				if(res == null) {
					System.out.println();
				}
				found = true;
			//clustered
			} else {
				if(currentTuple == null) {
					return null;
				}
				if(highKey != null && currentTuple.getValueAtField(index.indexedOn()) > highKey) {
					return null;
				}
				res = currentTuple;
				found = true;
				currentTuple = reader.readNextTuple();
			}
		}
		return res;
	}

	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}
	
	public String indexedColumn(){
		String column = index.indexColumn();
		return column.split(".")[1];
	}

}
