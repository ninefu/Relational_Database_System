package index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import database.TupleIdentifier;
import util.Constants;

/**
 * BTreeLeafNode maintains information of a leaf node in a B+ tree. It includes
 * the data entries on this node
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class BTreeLeafNode extends BTreeNode {
	
	List<ArrayList<TupleIdentifier>> dataEntries; //package-private so can be accessed when serialized
	
	public BTreeLeafNode(int address, Integer[] keys, List<ArrayList<TupleIdentifier>> entries) {
		super(address, keys);
		this.dataEntries = entries;
	}
	
	/**
	 * Get the tuple on entry's position in key, 
	 * @param int key
	 * @param int entry
	 * @return an TupleIdentifier
	 */
	public TupleIdentifier tupleAt(int key, int entry) {
		return dataEntries.get(key).get(entry);
	}
	
	/**
	 * Get the number of entries at a position identified by the index
	 * @param int index
	 * @return int, total number
	 */
	public int numEntries(int index) {
		return dataEntries.get(index).size();
	}

	/**
	 * Find whether this node is a leaf node
	 * @return boolean, true if it's a leaf node otherwise false
	 */
	@Override
	public boolean isLeaf() {
		return true;
	}

	/**
	 * Returns the smallest search key in the leftmost leaf node
	 * of this subtree.
	 */
	@Override
	public int getSmallest() {
		int smallest = Integer.MAX_VALUE;
		for(Integer i : keys) {
			if(i != null && i < smallest) {
				smallest = i;
			}
		}
		return smallest;
	}

	/**
	 * Get the total number of nodes from this node as root
	 * @return int
	 */
	@Override
	public int size() {
		return 1;
	}

	/**
	 * Get the total number of leaves from this node as root
	 * @return int
	 */
	@Override
	public int leafNumber() {
		return 1;
	}

	/**
	 * Writes the flag for a leaf (0), the number of data entries on the leaf,
	 * then the data for the node: for each key value, write that value, followed
	 * by the number of tuples with that key, followed by the page number and tuple 
	 * number for every tuple with that key.
	 * @param buffer Buffer to write serialized data to for this node.
	 */
	@Override
	public void serialize(ByteBuffer buffer) {
		buffer.putInt(0, 0); // Flag indicating leaf node
		buffer.putInt(4, dataEntries.size()); // Number of data entries
		int positionInBuffer = 8;
		for(int i = 0; i < keys.length; i++) {
			ArrayList<TupleIdentifier> entries = dataEntries.get(i);
			buffer.putInt(positionInBuffer, keys[i]); // The value of the key
			positionInBuffer += Constants.sizeOfInt;
			buffer.putInt(positionInBuffer, entries.size()); // Number of tuples for this key;
			positionInBuffer += Constants.sizeOfInt;
			for(TupleIdentifier entry : entries) { // For each tuple with this key...
				buffer.putInt(positionInBuffer, entry.getPageNumber()); // Page # location of this tuple
				positionInBuffer += Constants.sizeOfInt;
				buffer.putInt(positionInBuffer, entry.getTupleNumber()); // Tuple # location on page
				positionInBuffer += Constants.sizeOfInt;
			}
		}	
	}

	/**
	 * A customized toString() method
	 */
	@Override
	public void prettyPrint(StringBuilder sb) {
		sb.append(String.format("| Leaf@%d ", address));
		for(int i = 0; i < keys.length; i++) {
			sb.append(String.format("<%d[", keys[i]));
			for(TupleIdentifier entry : dataEntries.get(i)) {
				sb.append(String.format("(%d,%d)", entry.getPageNumber(), entry.getTupleNumber()));
			}
			sb.append("]>");
		}
		sb.append(" |");
	}

	/**
	 * Get the children nodes of this node
	 * @return BTreeNode[]
	 */
	@Override
	BTreeNode[] children() {
		return null;
	}


}
