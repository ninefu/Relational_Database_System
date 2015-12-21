package index;

import java.nio.ByteBuffer;

public abstract class BTreeNode {
	Integer[] keys;
	int address;
	
	public BTreeNode(int address, Integer[] keys) {
		this.keys = keys;
		this.address = address;
	}
	
	public int getKeyAt(int index) {
		return keys[index];
	}
	
	public int numKeys() {
		return keys.length;
	}
	
	public int address() {
		return address;
	}
	
	public abstract boolean isLeaf();

	
	abstract BTreeNode[] children();
	
	/**
	 * Returns the smallest search key in the leftmost leaf node
	 * of this subtree.
	 */
	public abstract int getSmallest();
	
	/**
	 * Returns the number of nodes in this subtree.
	 * @return The number of nodes in the subtree rooted
	 * 			at this node.
	 */
	public abstract int size();
	
	/**
	 * Returns the number of leaves in the subtree
	 * rooted at this node
	 * @return the number of leaves in the subtree
	 * rooted at this node
	 */
	public abstract int leafNumber();
	
	public abstract void serialize(ByteBuffer buffer);
	
	public abstract void prettyPrint(StringBuilder sb);
	
}
