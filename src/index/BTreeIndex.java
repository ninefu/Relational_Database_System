package index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import database.DatabaseCatalog;
import database.Table;
import database.Tuple;
import database.TupleIdentifier;
import physicalOperator.InMemorySortOperator;
import physicalOperator.PhysicalOperator;
import physicalOperator.ScanOperator;
import physicalOperator.SequentialScanOperator;
import util.BinaryTupleWriter;
import util.Constants;
import util.TupleWriter;

/**
 * BTreeIndex maintains information of B+ tree index. It includes the table which the index
 * is based on, the column to be indexed, whether the index is clustered or not, order of 
 * each node, root of the index tree, layers of index node, a serialized local file of the tree
 * ,and number of leaves.
 * 
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class BTreeIndex {
	
	private Table sourceTable;
	private String columnIndexed;
	private boolean clustered;
	private int order;
	private BTreeNode root;
	private ArrayList<ArrayList<BTreeNode>> layers;
	private File indexFile;
	private int numLeaves;
	
	/**
	 * Constructor
	 * @param db, DatabaseCatalog
	 * @param table, base table the index is based on 
	 * @param colName, the indexed column, in the format of [TableName].[ColumnName]
	 * @param cluster, whether the index should be clustered or not
	 * @param ord, order of each node
	 */
	public BTreeIndex(DatabaseCatalog db, Table table, String colName, int cluster, int ord) {
		sourceTable = table;
		columnIndexed = colName;
		clustered = cluster == 1;
		order = ord;
		root = null; // Gets set when either bulkLoad() or deserialize() is called
		layers = new ArrayList<ArrayList<BTreeNode>>();
		indexFile = new File(db.getIndexesDir(), columnIndexed);
	}
	
	
	/**
	 * Convenient constructor for testing
	 * @param dir, directory of storing index files
	 * @param other params are the same as the above constructor
	 */
	public BTreeIndex(String dir,Table table,String colName,int cluster, int ord){
		sourceTable = table;
		columnIndexed = colName;
		clustered = cluster == 1;
		order = ord;
		root = null; // Gets set when either bulkLoad() or deserialize() is called
		layers = new ArrayList<ArrayList<BTreeNode>>();
		indexFile = new File(dir, columnIndexed);
	}
	
	/**
	 * Returns the root node of this index
	 * @return Root node of B Tree index
	 */
	public BTreeNode getRoot() {
		return root;
	}
	
	/**
	 * Get the number of leaves
	 * @return int
	 */
	public int numLeaves() {
		return numLeaves;
	}
	
	/**
	 * Find out whether the index tree is clustered
	 * @return boolean, 
	 * 			true if clustered, false if unclustered
	 */
	public boolean isClustered() {
		return clustered;
	}
	
	/**
	 * Get the indexed column name
	 * @return String
	 */
	public String indexedOn() {
		return columnIndexed;
	}
	
	/**
	 * Deserializes the entire tree and sets the root node of this class.
	 */
	public void deserialize() {
		try {
			FileInputStream fis = new FileInputStream(indexFile);
			FileChannel channel = fis.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(4096);
			channel.read(buffer);
			int rootAddress = buffer.getInt(0);
			numLeaves = buffer.getInt(4);
			int serOrder = buffer.getInt(8);
			if(order != serOrder) {
				fis.close();
				throw new RuntimeException("Order in config file and order of deserialized index don't match for " + sourceTable.getName());
			}
			root = deserializeNode(channel, buffer, rootAddress, null);
			fis.close();
			
		} catch (IOException e) {
			System.err.println("Error occurred while deserializing: " + sourceTable.getName());
			e.printStackTrace();
		}
	}
	
	/**
	 * A helper function to deserialize an index file.
	 * @param targetKey
	 * @return BTreeLeafNode
	 */
	public BTreeLeafNode deserializeTraversal(Integer targetKey) {
		try {
			FileInputStream fis = new FileInputStream(indexFile);
			FileChannel channel = fis.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(4096);
			channel.read(buffer);
			int rootAddress = buffer.getInt(0);
			numLeaves = buffer.getInt(4);
			int serOrder = buffer.getInt(8);
			if(order != serOrder) {
				fis.close();
				throw new RuntimeException("Order in config file and order of deserialized index don't match for " + sourceTable.getName());
			}
			// TODO: fix this uglyness
			BTreeLeafNode res = (BTreeLeafNode)deserializeNode(channel, buffer, rootAddress, targetKey);
			fis.close();
			return res;
		} catch (IOException e) {
			System.err.println("Error occurred while deserializing: " + sourceTable.getName());
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Find the smallest leaf node 
	 * @return BTreeLeafNode
	 */
	public BTreeLeafNode getSmallestLeafNode() {
		// TODO fix this uglyness
		return (BTreeLeafNode)deserializeNode(1); // Smallest leaf node will always be on page 1.
	}
	
	/**
	 * A helper function to deserialize nodes on a page
	 * @param page
	 * @return BTreeNode
	 */
	public BTreeNode deserializeNode(int page) {
		try {
			FileInputStream fis = new FileInputStream(indexFile);
			FileChannel channel = fis.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate(4096);
			channel.read(buffer);
			numLeaves = buffer.getInt(4);
			BTreeNode res = deserializeNode(channel, buffer, page, null);
			fis.close();
			return res;
		} catch (IOException e) {
			System.err.println("Error occurred while deserializing: " + sourceTable.getName());
			e.printStackTrace();
			return null;
		}
	}
	
	
	private BTreeNode deserializeNode(FileChannel channel, ByteBuffer buffer, int page, Integer traversalKey) {
		readPage(channel, buffer, page);
		boolean isIndex = buffer.getInt(0) == 1;
		if(isIndex) {
			return deserializeIndexNode(channel, buffer, page, traversalKey);
		}
		return deserializeLeafNode(page, buffer);
	}
	
	private BTreeNode deserializeIndexNode(FileChannel channel, ByteBuffer buffer, int page, Integer traversalKey) {
		int numKeys = buffer.getInt(4);
		Integer[] keys = new Integer[numKeys];
		Integer[] childrenPointers = new Integer[numKeys + 1];
		int positionInIndex = 8;
		int traversalIndex = -1;
		for(int i = 0; i < numKeys; i++) {
			keys[i] = buffer.getInt(positionInIndex);
			if(traversalKey != null) {
				if(traversalKey < keys[i]) {
					traversalIndex = i;
				}
			}
			positionInIndex += Constants.sizeOfInt;
		}
		if(traversalKey != null && traversalIndex == -1) {
			traversalIndex = numKeys; // Very right side of children
		}
		for(int i = 0; i < numKeys + 1; i++) {
			childrenPointers[i] = buffer.getInt(positionInIndex);
			positionInIndex += Constants.sizeOfInt;
		}
		if(traversalKey == null) { // If we aren't traversing, just deserialize all of the children
			BTreeNode[] children = new BTreeNode[childrenPointers.length];
			for(int index = 0; index < children.length; index++) {
				children[index] = deserializeNode(channel, buffer, childrenPointers[index], traversalKey);
			}
			return new BTreeIndexNode(page, keys, children);
		} else {
			return deserializeNode(channel, buffer, childrenPointers[traversalIndex], traversalKey);
		}
	}
	
	private BTreeNode deserializeLeafNode(int page, ByteBuffer buffer) {
		int numDataEntries = buffer.getInt(4);
		int positionInIndex = 8;
		Integer[] keys = new Integer[numDataEntries];
		List<ArrayList<TupleIdentifier>> entries = new ArrayList<ArrayList<TupleIdentifier>>();
		for(int i = 0; i < numDataEntries; i++) { // Deserialize the next key
			keys[i] = buffer.getInt(positionInIndex);
			positionInIndex += Constants.sizeOfInt;
			int numTups = buffer.getInt(positionInIndex);
			positionInIndex += Constants.sizeOfInt;
			ArrayList<TupleIdentifier> tuples = new ArrayList<TupleIdentifier>();
			for(int j = 0; j < numTups; j++) { // Go through the entries for each key
				int p = buffer.getInt(positionInIndex);
				positionInIndex += Constants.sizeOfInt;
				int t = buffer.getInt(positionInIndex);
				positionInIndex += Constants.sizeOfInt;
				tuples.add(new TupleIdentifier(p, t));
			}
			entries.add(tuples);
		}
		return new BTreeLeafNode(page, keys, entries);
	}
	
	private void readPage(FileChannel channel, ByteBuffer buffer, int page) {
		try {
			zeroBuffer(buffer);
			int pos = Constants.pageSize * page;
			channel.position(pos);
			channel.read(buffer);
		} catch (IOException e) {
			System.err.println("Error reading specific page: " + page);
			e.printStackTrace();
		}
	}
	
	/**
	 * Serializes the given index to the file inputDirectory/db/indexes/[TABLENAME]
	 */
	public void serialize() {
		// Initialize class variables needed for serializing -- should do it here
		// instead of the constructor just in case serialize() never gets called,
		// and we don't get a chance to clean up the resources.
		if(root == null) { throw new RuntimeException("serialize() called on an empty tree."); }
		try {
			FileOutputStream fos = new FileOutputStream(indexFile);
			FileChannel channel = fos.getChannel();
			// Write header page: address of root, # of leaves, order of tree
			ByteBuffer buffer = ByteBuffer.allocate(Constants.pageSize);
			zeroBuffer(buffer);
			buffer.putInt(0, root.size()); // Header Address of the root: n, where n in # of nodes in index
			buffer.putInt(4, root.leafNumber()); // Number of leaves in the tree
			buffer.putInt(8, order);
			channel.write(buffer);
			
			for(ArrayList<BTreeNode> layer : layers) {
				serializeLayer(layer, buffer, channel);
			}
			
			channel.close();
			fos.close();
		} catch (IOException e) {
			System.err.println("Error serializing index: " + sourceTable.getName());
			e.printStackTrace();
		}

	}
	
	/**
	 * Serializes every node in the given layer to a file as described
	 * by Section 2.3 of the writeup.
	 * @param layer List of nodes all at the same depth in the B Tree
	 */
	private void serializeLayer(ArrayList<BTreeNode> layer, ByteBuffer buffer, FileChannel channel) {
		for(BTreeNode node : layer) {
			zeroBuffer(buffer);
			node.serialize(buffer);
			try {
				channel.write(buffer);
			} catch (IOException e) {
				System.err.println("Error writing node " + node.address);
				e.printStackTrace();
			}
		}
	}
	
	public void bulkLoad() {
		// Construct the physical operators just to avoid duplicating work
		PhysicalOperator scanner = new SequentialScanOperator(sourceTable, null);
		InMemorySortOperator sorted = new InMemorySortOperator(new String[] { columnIndexed }, scanner);
		if(clustered) {
			// If the index is clustered, we need to rewrite the source file as sorted by the indexed column
			TupleWriter tw = new BinaryTupleWriter(sourceTable.getDataFile(), sourceTable.getSchema().length);
			Tuple tup;
			while((tup = sorted.getNextTuple()) != null) {
				tw.writeTuple(tup);
			}
			tw.close();
			scanner.reset();
		}
		
		List<Tuple> tuples = sorted.getSortedElements();
		HashMap<Integer, ArrayList<TupleIdentifier>> keyedTuples = new HashMap<Integer, ArrayList<TupleIdentifier>>();
		// Hash the tuples into a map by key so we can easily put them at the leaves of the tree
		for(Tuple t : tuples) {
			Integer val = t.getValueAtField(columnIndexed);
			if(keyedTuples.containsKey(val)) {
				List<TupleIdentifier> addTo = keyedTuples.get(val);
				addTo.add(t.getRID());
			} else {
				ArrayList<TupleIdentifier> lst = new ArrayList<TupleIdentifier>();
				lst.add(t.getRID());
				keyedTuples.put(val, lst);
			}
		}
		// Probably a more efficient/nicer way to do this since we already sorted,
		// but sort the unique keys again so we can bulk load the leaf layer
		ArrayList<Integer> sortedKeys = new ArrayList<Integer>();
		for(Integer i : keyedTuples.keySet()) {
			sortedKeys.add(i);
		}
		Collections.sort(sortedKeys);
		
		// Construct the leaf layer
		ArrayList<BTreeNode> leaves = buildLeafLayer(sortedKeys, keyedTuples);
		layers.add(leaves);
		if(leaves.size() == 1) { // If there is only one leaf node, it is underflowed and should be the root
			root = leaves.get(0);
			return;
		}
		int currentNodeAddress = leaves.size() + 1; // Since we bulk load in the same order we serialize, this makes sense
	
		// Construct the first index layer
		ArrayList<BTreeNode> indexLayer = buildIndexLayer(currentNodeAddress, leaves);
		layers.add(indexLayer);
		currentNodeAddress += indexLayer.size();
		// Until we build the root, build another index layer
		while(indexLayer.size() > 1) {
			indexLayer = buildIndexLayer(currentNodeAddress, indexLayer);
			layers.add(indexLayer);
		}
		
		// indexLayer[0] is the root of the index tree
		root = indexLayer.get(0);	
	}
	
	private ArrayList<BTreeNode> buildLeafLayer(ArrayList<Integer> sortedKeys, HashMap<Integer, ArrayList<TupleIdentifier>> keyedTuples) {
		ArrayList<BTreeNode> leaves = new ArrayList<BTreeNode>();
		int currentNodeAddress = 1;
		int totalKeys = sortedKeys.size();
		int keysLeft = totalKeys;
		int keyPos = 0;
		// While there are more keys to process AND the number of keys does not fall in the range
		// where we would be underfilling the last node if we were to completely fill the second-to-last
		while(keysLeft > 0 && !(keysLeft > 2*order && keysLeft < 3*order)) {
			List<ArrayList<TupleIdentifier>> entries = new ArrayList<ArrayList<TupleIdentifier>>();
			int keysToFill = Math.min(2*order, keysLeft);
			Integer[] keys = new Integer[keysToFill];
			for(int i = 0; i < keysToFill; i++) {
				Integer key = sortedKeys.get(keyPos);
				keys[i] = key;
				entries.add(keyedTuples.get(key));
				keyPos++;
				keysLeft--;
			}
			leaves.add(new BTreeLeafNode(currentNodeAddress, keys, entries));
			currentNodeAddress++;
		}
		
		// Handles the case where the last node would have < d entries (underfull)
		if(keysLeft > 2*order && keysLeft < 3*order) {
			// Give half the remaining nodes to the second to last leaf node
			int numOnLeft = keysLeft/2;
			// Give whatever is left to the last leaf node.
			int numOnRight = keysLeft - numOnLeft;
			// Construct second to last lead node
			Integer[] keysOnLeft = new Integer[numOnLeft];
			List<ArrayList<TupleIdentifier>> entriesOnLeft = new ArrayList<ArrayList<TupleIdentifier>>();
			for(int i = 0; i < numOnLeft; i++) {
				Integer key = sortedKeys.get(keyPos);
				keysOnLeft[i] = key;
				entriesOnLeft.add(keyedTuples.get(key));
				keyPos++;
				keysLeft--;
			}
			leaves.add(new BTreeLeafNode(currentNodeAddress, keysOnLeft, entriesOnLeft));
			currentNodeAddress++;
			
			// Construct last leaf node
			Integer[] keysOnRight = new Integer[numOnRight];
			List<ArrayList<TupleIdentifier>> entriesOnRight = new ArrayList<ArrayList<TupleIdentifier>>();
			for(int i = 0; i < numOnRight; i++) {
				Integer key = sortedKeys.get(keyPos);
				keysOnRight[i] = key;
				entriesOnRight.add(keyedTuples.get(key));
				keyPos++;
				keysLeft--;
			}
			leaves.add(new BTreeLeafNode(currentNodeAddress, keysOnRight, entriesOnRight));
			currentNodeAddress++;
		}
		return leaves;
	}
	
	private ArrayList<BTreeNode> buildIndexLayer(int currentNodeAddress, ArrayList<BTreeNode> childLayer) {
		int totalChildren = childLayer.size();
		int childrenLeft = totalChildren;
		int childPos = 0;
		ArrayList<BTreeNode> layer = new ArrayList<BTreeNode>();
		// While there are more children to process AND we are not in the range where we would
		// underfill the last index node if we were to completely fill the second-to-last
		while(childrenLeft > 0 && !(childrenLeft > 2*order + 1 && childrenLeft < 3*order +2)) {
			int childrenToAdd = Math.min(2*order + 1, childrenLeft);
			Integer[] keys = new Integer[childrenToAdd-1];
			BTreeNode[] children = new BTreeNode[childrenToAdd];
			for(int i = 0; i < childrenToAdd; i++) {
				children[i] = childLayer.get(childPos);
				childPos++;
				childrenLeft--;
			}
			for(int i = 0; i < keys.length; i++) {
				if(children[i+1] != null) {
					keys[i] = children[i+1].getSmallest();
				}
			}	
			layer.add(new BTreeIndexNode(currentNodeAddress, keys, children));
			currentNodeAddress++;
		}
		
		// We have two index nodes left to fill, and we need to split up
		// the remaining children so as not to underfill either of them
		if(childrenLeft > 2*order + 1 && childrenLeft < 3*order +2) {
			int numOnLeft = childrenLeft/2; // Half nodes go to the left
			int numOnRight = childrenLeft - numOnLeft; // Rest go to the right
			
			// Create second-to-last index node on this layer
			Integer[] keysOnLeft = new Integer[numOnLeft - 1];
			BTreeNode[] childrenOnLeft = new BTreeNode[numOnLeft];
			for(int i = 0; i < numOnLeft; i++) {
				childrenOnLeft[i] = childLayer.get(childPos);
				childPos++;
				childrenLeft--;
			}
			for(int i = 0; i < numOnLeft -1; i++) {
				keysOnLeft[i] = childrenOnLeft[i+1].getSmallest();
			}
			layer.add(new BTreeIndexNode(currentNodeAddress, keysOnLeft, childrenOnLeft));
			currentNodeAddress++;
			
			// Create last index node on this layer
			Integer[] keysOnRight = new Integer[numOnRight - 1];
			BTreeNode[] childrenOnRight = new BTreeNode[numOnRight];
			for(int i = 0; i < numOnRight; i++) {
				childrenOnRight[i] = childLayer.get(childPos);
				childPos++;
				childrenLeft--;
			}
			for(int i = 0; i < numOnRight -1; i++) {
				keysOnRight[i] = childrenOnRight[i+1].getSmallest();
			}
			layer.add(new BTreeIndexNode(currentNodeAddress, keysOnRight, childrenOnRight));
			currentNodeAddress++;
		}
		// If this is the root, there should only one node in this list.
		return layer;
	}
	
	private void zeroBuffer(ByteBuffer buffer) {
		for(int i = 0; i < (Constants.pageSize)/4; i++) {
			buffer.putInt(i*4, 0);
		}
		buffer.clear();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("Index on %s\n", columnIndexed));
		Queue<BTreeNode> lastLayer = new LinkedList<BTreeNode>();
		Queue<BTreeNode> nextLayer = new LinkedList<BTreeNode>();
		BTreeNode current = null;
		lastLayer.add(root);
		while(lastLayer.peek() != null) {
			while((current = lastLayer.poll()) != null) {
				current.prettyPrint(sb);
				BTreeNode[] children = current.children();
				if(children != null) {
					for(BTreeNode child : children) {
						nextLayer.add(child);
					}
				}
			}
			sb.append("\n");
			lastLayer = nextLayer;
			nextLayer = new LinkedList<BTreeNode>();
		}
		return sb.toString();
	}
	
	public String indexColumn(){
		return columnIndexed;
	}
}
