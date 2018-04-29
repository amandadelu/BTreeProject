package btree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import bterrors.BTreeBadMetadata;
import bterrors.BTreeFullNode;
import bterrors.BTreeNoInternalNodeChild;
import bterrors.BTreeNonExactNonLeaf;
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
	
	class MedianNode {
		// Class to carry meadian key from node split to left and right 
		public BTreeNode left, right;
		public BTreeObject key;
		public MedianNode(BTreeObject key, BTreeNode left, BTreeNode right) {
			this.key = key;
			this.left=left;
			this.right=right;
		}
	}
	
	class NearestSearchResult {
		// Class to carry the search result nearest to the needed
		public BTreeObject needkey;
		public BTreeNode node;
		public int pos;
		public boolean exact;
		public NearestSearchResult(BTreeObject searchkey) throws BTreeWrongBlockID, BTreeBadMetadata, IOException, BTreeNonExactNonLeaf {
			needkey = searchkey;
			BTreeNode prevnode=null;
			node=rootnode;
			while (true) {
				pos = node.searchkey(needkey);
				if ((pos >= node.keycount || node.keys[pos].compareTo(needkey)!=0)) {
					if (!node.isLeaf) {
						prevnode = node;
						node = new BTreeNode(node.children[pos], -1, false);
						//TODO Cache
						node.load();
						if (node.id_parent!=prevnode.id) throw new BTreeWrongBlockID();
					} else break;
				} else break;
			}
			exact = (pos < node.keycount && node.keys[pos].compareTo(needkey)==0);
			if (!exact && !node.isLeaf) throw new BTreeNonExactNonLeaf();
			
		}
	}
	
	public void insertKey(BTreeObject key) throws BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeWrongBlockID, BTreeBadMetadata, BTreeFullNode, BTreeNonExactNonLeaf, IOException {
		insertSearch(new NearestSearchResult(key));
	}
	
	private void insertSearch(NearestSearchResult res) throws BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeWrongBlockID, BTreeBadMetadata, IOException, BTreeFullNode {
		if (res.exact) return;
        BTreeNode insnode=res.node;
        MedianNode med = new MedianNode(res.needkey, null, null), prevmed=null;
        int pos = res.pos;
        while (insnode.keycount >= maxkeycount) {
        		 prevmed=med;
        	     med = insnode.splitNode(med.key, pos, med.right);
        	     if (prevmed.left != null) {
        	    	     prevmed.left.id_parent = med.left.id;
        	    	     //TODO Cache
        	    	     prevmed.left.save();
        	     }
        	     if (prevmed.right != null) {
    	    	     	prevmed.right.id_parent = med.right.id;
    	    	     	//TODO Cache
    	    	     	prevmed.right.save();
        	     }
        	     if (insnode.id_parent==-1) {
        	    	     insnode = null;
        	    	     break;
        	     }
        	     BTreeNode parent = new BTreeNode(insnode.id_parent, -1, false);
        	     // TODO Cache
        	     parent.load();
        	     if (insnode.id_parent!=parent.id) throw new BTreeWrongBlockID();
        	     insnode = parent;
        	     pos = insnode.searchkey(med.key);
        }
        if (insnode!=null) {
        	    insnode.insertkey(med.key, pos, med.right);
        	    if (insnode.id!=res.node.id) saveMetaData();
        	    if (insnode.id_parent==-1) rootnode = insnode;
        	    if (med.left!=null) med.left.save();
        } else {
        	    rootnode = new BTreeNode(nodecount++, -1, false);
        	    rootnode.keycount=1;
        	    rootnode.keys[0] = med.key;
        	    rootnode.children[0] = med.left.id;
        	    rootnode.children[1] = med.right.id;
        	    med.left.id_parent=rootnode.id;
        	    med.right.id_parent=rootnode.id;
        	    //TODO Cache
        	    rootnode.save();
        	    med.left.save();
        	    med.right.save();
        	    saveMetaData();
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
			while (i < keycount && key.compareTo(keys[i]) == 1) {
				i++;
			}
			return i;
		}
		
		private void checkKeyOrder(BTreeObject key, int pos, BTreeNode rightchild) throws BTreeWrongKeyOrder, BTreeNoInternalNodeChild {			
			if (pos > 0 && key.compareTo(keys[pos-1]) != 1)
				throw new BTreeWrongKeyOrder();
			if (pos < keycount && key.compareTo(keys[pos]) != -1)
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
		
		public MedianNode splitNode(BTreeObject key, int pos, BTreeNode rightchild) throws BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild {
			if (keycount < maxkeycount)
				throw new BTreeNotFullNode();
			checkKeyOrder(key, pos, rightchild);
			long[] tmpchildren = new long[order+1];
			BTreeObject[] tmpkeys = new BTreeObject[maxkeycount+1];
			for (int i = 0; i < pos; i++) {
				tmpkeys[i] = keys[i];
				tmpchildren[i] = children[i];
				keys[i] = null;
			}
			tmpchildren[pos] = children[pos];
			tmpkeys[pos] = key;
			if (!isLeaf) {
				tmpchildren[pos+1] = rightchild.id;	
			} 
			for (int i = pos; i < keycount; i++) {
				tmpkeys[i+1] = keys[i];
				tmpchildren[i+2] = children[i+1];
				keys[i] = null;
			}
			int mid = tmpkeys.length / 2;
			keycount = mid;
			for (int i = 0; i < keycount; i++) {
				children[i] = tmpchildren[i];
				keys[i] = tmpkeys[i];
			}
			children[mid] = tmpchildren[mid];
			BTreeNode newnode = new BTreeNode(nodecount++, id_parent, isLeaf);
		
			for (int j=0, i=(mid + 1); i< tmpkeys.length; i++, j++) {
				newnode.keys[j] = tmpkeys[i];
				newnode.children[j] = tmpchildren[j];
			}
			newnode.keycount = tmpkeys.length - (mid + 1);
			newnode.children[newnode.keycount] = tmpchildren[tmpchildren.length-1];
			// TODO make sure left and right new nodes are saved later
			return new MedianNode(tmpkeys[mid], this, newnode);
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
		
		void load() throws IOException, BTreeWrongBlockID, BTreeBadMetadata {
			storage.seek(getNodeOffset(id));
			byte[] buff = new byte[nodesize];
			storage.read(buff);
			LongBuffer l = ByteBuffer.wrap(buff).asLongBuffer();
			long check_id = l.get();
			if (id!=check_id) throw new BTreeWrongBlockID();
			id_parent = l.get();
			long tmp = l.get();
			keycount = (int)(tmp >> 8);
			if (keycount > maxkeycount) throw new BTreeBadMetadata();
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
		storage = new RandomAccessFile(fname, readonly?"r":"rw");
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

	public static void main(String[] args) throws IOException, BTreeBadMetadata, BTreeWrongBlockID, BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeFullNode, BTreeNonExactNonLeaf {
		//BTree test1 = new BTree("test1.tree", 2);
		//BTree test2 = new BTree("test2.tree", 4);
		//@SuppressWarnings("unused")
		/*System.out.println(Long.compare(0, 1));
		System.out.println(Long.compare(1, 0));
		System.out.println(Long.compare(0, 0));
		System.exit(0);*/
		BTree test = new BTree("test3.tree", 4);
		for (long i=0; i<39; i+=3) {
			System.out.println(i);
			test.insertKey(new BTreeObject(i));
		}
		test.insertKey(new BTreeObject(8));
		test.insertKey(new BTreeObject(28));
		test.insertKey(new BTreeObject(39));
		//test.insertKey(new BTreeObject(27));
		/*test.insertKey(new BTreeObject(10));
		test.insertKey(new BTreeObject(169));
		test.insertKey(new BTreeObject(170));
		test.insertKey(new BTreeObject(171));*/

	}

}
