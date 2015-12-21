package index;

import java.nio.ByteBuffer;

import util.Constants;

/**
 * BTreeIndexNode maintains information of an index node in a B+ tree. It includes
 * its children node and pointers to its children
 * @author Clara Thomas (clt84) , Christopher Anderson (cma227) , Yihui Fu (yf263)
 */
public class BTreeIndexNode extends BTreeNode {
	
	BTreeNode[] children;
	Integer[] childrenPointers; // To be set when deserializing page-by-page
	
	public BTreeIndexNode(int address, Integer[] keys, BTreeNode[] children) {
		super(address, keys);
		this.children = children;
	}
	
	public BTreeIndexNode(int address, Integer[] keys, Integer[] ptrs) {
		super(address, keys);
		this.childrenPointers = ptrs;
	}

	/**
	 * Find whether this node is a leaf node
	 * @return boolean, true if it's a leaf node otherwise false
	 */
	@Override
	public boolean isLeaf() {
		return false;
	}
	
	/**
	 * Returns the smallest search key in the leftmost leaf node
	 * of this subtree.
	 */
	@Override
	public int getSmallest() {
		return children[0].getSmallest();
	}

	/**
	 * Get the total number of nodes from this node as root
	 * @return int
	 */
	@Override
	public int size() {
		int sum = 0;
		for(BTreeNode child : children) {
			sum += child.size();
		}
		return sum + 1;
	}
	
	/**
	 * Get the total number of leaves from this node as root
	 * @return int
	 */
	@Override
	public int leafNumber() {
		int sum = 0;
		for(BTreeNode child : children) {
			sum += child.leafNumber();
		}
		return sum;
	}

	/**
	 * Serializes the node by writing a flag indicating index (1),
	 * the numbers of keys in the node, then the value of each key,
	 * then the address of each of the children (both of these fields
	 * in order)
	 * @param buffer Buffer for serialized node data to be written to
	 */
	@Override
	public void serialize(ByteBuffer buffer) {
		buffer.putInt(0, 1); //Flag indicating index node
		buffer.putInt(4, numKeys()); // Number of keys in node
		int positionInBuffer = 8;
		for(Integer key : keys) { // Serialize the keys in order
			buffer.putInt(positionInBuffer, key);
			positionInBuffer += Constants.sizeOfInt;
		}
		for(BTreeNode child : children) { // Serialize addresses of children in order
			buffer.putInt(positionInBuffer, child.address);
			positionInBuffer += Constants.sizeOfInt;
		}
	}
	
	/**
	 * a customized toString() method
	 */
	@Override
	public void prettyPrint(StringBuilder sb) {
		sb.append(String.format("| Index@%d <", address));
		sb.append(String.format("c%d, ", children[0].address));
		for(int i = 0; i < keys.length; i++) {
			sb.append(String.format("k%d, ", keys[i]));
			sb.append(String.format("c%d, ", children[i+1].address));
		}
		int size = sb.length();
		sb.delete(size-2, size);
		sb.append("> |");
	}

	/**
	 * Get the children nodes of this node
	 * @return BTreeNode[]
	 */
	@Override
	public BTreeNode[] children() {
		return children;
	}

}
