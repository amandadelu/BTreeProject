package btree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import bterrors.BTreeBadMetadata;
import bterrors.BTreeFullNode;
import bterrors.BTreeNoInternalNodeChild;
import bterrors.BTreeNotFullNode;
import bterrors.BTreeWrongBlockID;
import bterrors.BTreeWrongFileSize;
import bterrors.BTreeWrongKeyOrder;
import bterrors.BTreeWrongObjectSize;
import bterrors.BTreeWrongRootNode;

public class BTree {
	private int degree;
	private int order;
	private int maxkeycount;
	private int nodesize;
	private long nodecount;
	private final static int def_blocksize = 4096;
	private final static int metaDatasize = 4096;
	
	BTreeNode rootnode;
	
	private RandomAccessFile storage;
	private static final int node_overhead = 8*3;
	private static int getNodeDataSize(int testdegree) {
		int testorder = 2 * testdegree;
		return node_overhead + 8 * testorder + BTreeObject.size *(testorder - 1);
	}
	
	private long getNodeOffset(long testid) {
		return metaDatasize + testid * nodesize;
	}
	
	private static int maxdegree(int testnodesize) {
		int testdegree = 2;
		int minnodesize = getNodeDataSize(testdegree);
		if (minnodesize >= def_blocksize) {
           return testdegree;
		} else {
			return (def_blocksize - node_overhead + BTreeObject.size) / (8 + BTreeObject.size) / 2;
		}
	}
	
	
	class BTreeNode {
		private long id;
		private long id_parent;
		private long[] children;
		private BTreeObject[] keys;
		private int keycount;
		private boolean isLeaf;
		
		public BTreeNode(long id,long id_parent,boolean isLeaf) {
			this.id = id;
			this.id_parent = id_parent;
			this.isLeaf = isLeaf;
			children = new long[order];
			keys = new BTreeObject[maxkeycount];
			keycount = 0;
			
		}
		
		public int searchkey(BTreeObject key) {
			//TODO more efficient search for the nearest node
			int i = 0;
			while (i < keycount && key.compareTo(keys[i]) == -1) {
				i++;
			}
			return i;
		}
		
		private void checkKeyOrder(BTreeObject key, int pos, BTreeNode rightchild) throws BTreeWrongKeyOrder, BTreeNoInternalNodeChild {			
			if (pos > 0 && key.compareTo(keys[pos-1]) != -1)
				throw new BTreeWrongKeyOrder();
			if (pos < keycount && key.compareTo(keys[pos-1]) != 1)
				throw new BTreeWrongKeyOrder();
			
			if (rightchild==null & !isLeaf)
				throw new BTreeNoInternalNodeChild();			
		}
		
		public void insertkey(BTreeObject key, int pos, BTreeNode rightchild) throws BTreeWrongKeyOrder, BTreeFullNode, BTreeNoInternalNodeChild, IOException {
			if (keycount == maxkeycount)
				throw new BTreeFullNode();
			checkKeyOrder(key, pos, rightchild);
			//TODO more efficent insert
			for (int i = keycount; i>pos; i--) {
				keys[i] = keys[i-1];
			}
			keys[pos] = key;
			if (!isLeaf) {
				for (int i = keycount; i>pos; i--) {
					children[i+1] = children[i];
				}
				children[pos+1] = rightchild.id;
				rightchild.id_parent = this.id;
				//TODO Cache? save call from outside?
				rightchild.save();
			}
			keycount++;
			//TODO Cache? save call from outside?
			save();
		}
		
		public BTreeNode splitNode(BTreeObject key, int pos, BTreeNode rightchild) throws BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild {
			if (keycount < maxkeycount)
				throw new BTreeNotFullNode();
			BTreeNode newnode = new BTreeNode(nodecount++, id_parent, isLeaf);
			checkKeyOrder(key, pos, rightchild);
			long[] tmpchildren = new long[order+1];
			BTreeObject[] tmpkeys = new BTreeObject[maxkeycount+1];
			for (int i = 0; i < pos; i++) {
				tmpkeys[i] = keys[i];
				tmpchildren[i] = children[i];
				keys[i] = null;
			}
			tmpchildren[pos] = children[pos+1];
			tmpkeys[pos] = key;
			if (!isLeaf) {
				tmpchildren[pos+1] = rightchild.id;	
			} 
			for (int i = pos; i < keycount; i++) {
				tmpkeys[i+1] = keys[i];
				tmpchildren[i+2] = children[i+1];
			}
			//TODO split
			//keycount = splitindex;
			return newnode;
		}
		
		void save() throws IOException {
			// Make sure the file is long enough
			long needfilesize = getNodeOffset(id + 1);
			if (storage.length() < needfilesize) storage.setLength(needfilesize);
			storage.seek(getNodeOffset(id));
			byte[] buff = new byte[nodesize];
			LongBuffer l = ByteBuffer.wrap(buff).asLongBuffer();
			l.put(id);
			l.put(id_parent);
			l.put((keycount << 8) | (isLeaf?1:0));
			for (long cref: children) {
				l.put(cref);
			}
			for (int i=0; i<keycount; i++) {
				keys[i].toBuffer(l);
			}
			storage.write(buff);
		}
		
		void load() throws IOException, BTreeWrongBlockID {
			storage.seek(getNodeOffset(id));
			byte[] buff = new byte[nodesize];
			storage.read(buff);
			LongBuffer l = ByteBuffer.wrap(buff).asLongBuffer();
			long check_id = l.get();
			if (id!=check_id) throw new BTreeWrongBlockID();
			id_parent = l.get();
			long tmp = l.get();
			keycount = (int)(tmp >> 8);
			isLeaf = ((tmp & 1) == 1);
	
			for (int i=0; i<children.length; i++) {
				children[i]=l.get();
			}
			for (int i=0; i<keycount; i++) {
				keys[i] = new BTreeObject(0);
				keys[i].fromBuffer(l);
			}
			
		}
	}
	
	
	public BTree(String fname, boolean readonly, boolean init) throws IOException, BTreeBadMetadata, BTreeWrongBlockID {
		if (init) {
			setupOptimalTree();
			init_btree(fname);
		} else {
			open_btree(fname, readonly);
		}
	}
	
	public BTree(String fname, int degree) throws IOException {
		setupTreeFromDegree(degree);
		init_btree(fname);
	}
	
	private void open_btree(String fname, boolean readonly) throws IOException, BTreeBadMetadata, BTreeWrongBlockID {
		String mode=readonly?"r":"rw";
		storage = new RandomAccessFile(fname, mode);
		byte[] buf = new byte[metaDatasize];
		LongBuffer l = ByteBuffer.wrap(buf).asLongBuffer();
		storage.seek(0);
		storage.read(buf);
		degree = (int)l.get();
		order = (int)l.get();
		maxkeycount= (int)l.get();
		if (order != 2 * degree || maxkeycount != order -1)
			throw new BTreeBadMetadata();
		nodecount = l.get();
		nodesize = (int)l.get();
		if (nodesize < getNodeDataSize(degree))
			throw new BTreeBadMetadata();
		if (storage.length() != getNodeOffset(nodecount))
			throw new BTreeWrongFileSize();
		int objsize = (int)l.get();
		if (objsize != BTreeObject.size)
			throw new BTreeWrongObjectSize();
		long rootid = l.get();
		if (getNodeOffset(1 + rootid) > storage.length())
			throw new BTreeWrongRootNode();
		rootnode = new BTreeNode(rootid, 0, false);
			
		rootnode.load();
		if (rootnode.id_parent != -1)
			throw new BTreeWrongRootNode();
	}

	private void init_btree(String fname) throws IOException {
		storage = new RandomAccessFile(fname, "rw");
		storage.setLength(getNodeOffset(1));
		nodecount = 1;
		rootnode = new BTreeNode(0, -1, true);
		rootnode.save();
		saveMetaData();
		
		
	}

	private void saveMetaData() throws IOException {
		byte[] buf = new byte[metaDatasize];
		LongBuffer l = ByteBuffer.wrap(buf).asLongBuffer();
		l.put(degree);
		l.put(order);
		l.put(maxkeycount);
		l.put(nodecount);
		l.put(nodesize);
		l.put(BTreeObject.size);
		l.put(rootnode.id);
		storage.seek(0);
		storage.write(buf);
		
	}

	private void setupOptimalTree() {
		this.degree = maxdegree(def_blocksize);
		this.order = this.degree << 1;
		this.maxkeycount = this.order - 1;
		this.nodesize = ((getNodeDataSize(degree) - 1) / def_blocksize + 1) * def_blocksize;
	}


	

	private void setupTreeFromDegree(int degree) {
		if (degree < 2) throw new IllegalArgumentException();
		this.degree = degree;
		this.order = degree << 1;
		this.maxkeycount = this.order - 1;
		this.nodesize = getNodeDataSize(degree);
		
	}

	public static void main(String[] args) throws IOException, BTreeBadMetadata, BTreeWrongBlockID {
		//BTree test1 = new BTree("test1.tree", 2);
		//BTree test2 = new BTree("test2.tree", 4);
		@SuppressWarnings("unused")
		BTree test3 = new BTree("test3.tree", true, false);
	}

}
