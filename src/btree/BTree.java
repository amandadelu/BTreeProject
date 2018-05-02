package btree;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import javax.swing.DebugGraphics;

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
import bterrors.DNAWrongSequenceLength;
import btree.BTree.Cache.NodeKeyPos;
import ncbi.DNASequence;


public class BTree {
	private int degree;
	private int order;
	private int maxkeycount;
	private int nodesize;
	private long nodecount;
	private final static int def_blocksize = 4096;
	private final static int metaDatasize = 4096;
	private boolean readonly;
	
	BTreeNode rootnode;
	Cache cache;
	
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
	
	public class NearestSearchResult {
		// Class to carry the search result nearest to the needed
		public BTreeObject needkey, foundkey;
		public BTreeNode node;
		public int pos;
		public boolean exact;
		
		public void saveNode() throws IOException {
			node.save();
		}
		public NearestSearchResult(BTreeObject searchkey) throws BTreeWrongBlockID, BTreeBadMetadata, IOException, BTreeNonExactNonLeaf {
			NodeKeyPos checkcache;
			if (cache!=null) {
				checkcache = cache.byKey(searchkey.getKey());
				if (checkcache!=null) {
					foundkey=checkcache.key;
					node=checkcache.node;
					pos=checkcache.pos;
					exact=true;
					return;
				}
			}
			
			needkey = searchkey;
			BTreeNode prevnode=null;
			node=rootnode;
			while (true) {
				pos = node.searchkey(needkey);
				if ((pos >= node.keycount || node.keys[pos].compareTo(needkey)!=0)) {
					if (!node.isLeaf) {
						prevnode = node;
						node = getNode(node.children[pos]);
						if (node.id_parent!=prevnode.id) {
							System.out.printf("%d %d %d %d%n", prevnode.id, prevnode.id_parent, node.id, node.id_parent);
							throw new BTreeWrongBlockID();
						}
					} else break;
				} else break;
			}
			exact = (pos < node.keycount && node.keys[pos].compareTo(needkey)==0);
			if (!exact && !node.isLeaf) throw new BTreeNonExactNonLeaf();
			if (exact) {
				foundkey = node.keys[pos];
			} else {
				foundkey=null;
			}
			
			
		}
	}
	
	public NearestSearchResult lookup(BTreeObject key) throws BTreeWrongBlockID, BTreeBadMetadata, BTreeNonExactNonLeaf, IOException {
		return new NearestSearchResult(key);
	}
	
	public void insertKey(BTreeObject key) throws BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeWrongBlockID, BTreeBadMetadata, BTreeFullNode, BTreeNonExactNonLeaf, IOException {
		insertToFoundLoc(new NearestSearchResult(key));
	}
	
	public void insertToFoundLoc(NearestSearchResult res) throws BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeWrongBlockID, BTreeBadMetadata, IOException, BTreeFullNode {
		if (res.exact) {
			return;
		}
        BTreeNode insnode=res.node;
        MedianNode med = new MedianNode(res.needkey, null, null);
        int pos = res.pos;
        while (insnode.keycount >= maxkeycount) {
   
        	     med = insnode.splitNode(med.key, pos, med.right);

        	     if (insnode.id_parent==-1) {
        	    	     insnode = null;
        	    	     break;
        	     }
        	     BTreeNode parent = getNode(insnode.id_parent);

        	     if (insnode.id_parent!=parent.id) throw new BTreeWrongBlockID();
        	     insnode = parent;
        	     pos = insnode.searchkey(med.key);
        }
        if (insnode!=null) {
        	    insnode.insertkey(med.key, pos, med.right);
        	    if (insnode.id_parent==-1) rootnode = insnode;

        } else {
        	    rootnode = new BTreeNode(nodecount++, -1, false);
        	    rootnode.keycount=1;
        	    rootnode.keys[0] = med.key;
        	    rootnode.children[0] = med.left.id;
        	    rootnode.children[1] = med.right.id;
        	    med.left.id_parent=rootnode.id;
        	    med.right.id_parent=rootnode.id;
        	    rootnode.save();
        	    med.left.save();
        	    med.right.save();
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
			for (int i=0; i< children.length; i++) children[i]=-1;
			keys = new BTreeObject[maxkeycount];
			keycount = 0;
			
		}
		
		public int searchkey(BTreeObject key) {
			int lo = 0, hi = keycount-1;
			while (lo <= hi) {
				int mid = (lo + hi) / 2;
				int cmp = key.compareTo(keys[mid]);
				switch (cmp) {
				case 0: return mid;
				case -1:  hi = mid - 1; break;
				case 1: lo = mid + 1; break;
				default: throw new IllegalArgumentException();
				}
			}
			return lo;
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
			//TODO more efficient insert
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
				rightchild.save();
			}
			keycount++;
			save();
		}
		
		public MedianNode splitNode(BTreeObject key, int pos, BTreeNode rightchild) throws BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeWrongBlockID, BTreeBadMetadata, IOException {
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
				if (!isLeaf) {
					BTreeNode tmpchild = getNode(tmpchildren[i]);
					newnode.children[j] = tmpchild.id;
					tmpchild.id_parent=newnode.id;
					tmpchild.save();
				}
			}
			newnode.keycount = tmpkeys.length - (mid + 1);
			if (!isLeaf) {
				BTreeNode tmpchild = getNode(tmpchildren[tmpchildren.length-1]); //new BTreeNode(tmpchildren[tmpchildren.length-1], -1, false);
				newnode.children[newnode.keycount] = tmpchild.id;
				tmpchild.id_parent=newnode.id;
				tmpchild.save();
			}
			this.save();
			newnode.save();
			return new MedianNode(tmpkeys[mid], this, newnode);
		}
		
		void save() throws IOException {
			if (cache==null) saveToStorage();
			else cache.AddToCache(this);
		}
		
		void saveToStorage() throws IOException {
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
		
		void loadFromStorage() throws IOException, BTreeWrongBlockID, BTreeBadMetadata {
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
			if (cache!=null) cache.AddToCache(this);
			
		}
	}
	
	private BTreeNode getNode(long id) throws IOException, BTreeWrongBlockID, BTreeBadMetadata {
		BTreeNode ret=null;
		if (cache!=null)
		   ret = cache.byNodeID(id);
		if (ret==null) {
			ret = new BTreeNode(id, -1, false);
			ret.loadFromStorage();
		} 
		return ret;
	}
	
	
	public BTree(String fname, boolean readonly, boolean init, int cachesize) throws IOException, BTreeBadMetadata, BTreeWrongBlockID {
		if (init) {
			setupOptimalTree();
			init_btree(fname);
		} else {
			open_btree(fname, readonly);
		}
		setupCache(cachesize);
		this.readonly=readonly;
	}
	
	public BTree(String fname, int degree, int cachesize) throws IOException {
		setupTreeFromDegree(degree);
		init_btree(fname);
		setupCache(cachesize);
		readonly=false;
	}
	
	private void setupCache(int cachesize) {
		if (cachesize > 0) {
			cache = new Cache(cachesize);
		} else {
			cache = null;
		}
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
		rootnode = getNode(rootid);
			
		//rootnode.load();
		if (rootnode.id_parent != -1)
			throw new BTreeWrongRootNode();
	}

	private void init_btree(String fname) throws IOException {
		storage = new RandomAccessFile(fname + "." + degree, "rw");
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
	
	public void shutdown() throws IOException {
		if (!readonly) {
			if (cache!=null)
			   cache.flush();
			saveMetaData();
		}
		storage.close();
	}
	
	
	class Cache {
		class NodeTS {
			public long node_id;
			public long lastaccess;
			public NodeTS (long node_id, long ts) {
				this.node_id = node_id;
				this.lastaccess = ts;
			}
		}
		class NodeKeyPos {
			BTreeNode node;
			BTreeObject key;
			int pos;
			public NodeKeyPos(BTreeNode node, BTreeObject key, int pos) {
				this.node=node;
				this.key=key;
				this.pos=pos;
			}
		}
		private HashMap<Long, BTreeNode> nodemap;
		private HashMap<Long, Long> nodetsmap;
		private HashMap<Long, NodeKeyPos> keymap;
		private int size;
		private LinkedList<NodeTS> cleanupqueue;
		
		public Cache(int size) {
			this.size=size;
			nodemap = new HashMap<Long, BTreeNode>();
			keymap = new HashMap<Long, NodeKeyPos>();
			nodetsmap = new HashMap<Long, Long>();
			cleanupqueue = new LinkedList<NodeTS>();
		}
		
		public void flush() throws IOException {
			for (BTreeNode node: nodemap.values()) {
				node.saveToStorage();
			}
		}
		public BTreeNode byNodeID(long node_id) throws IOException {
			BTreeNode ret = nodemap.get(node_id);
			if (ret!=null)
				AddToCache(ret);
			return ret;
		}
		
		public NodeKeyPos byKey(long needkey) {
			return keymap.get(needkey);
		}
		
	    public void AddToCache(BTreeNode node) throws IOException {
	    	    nodemap.put(node.id, node);
	    	    long ts = System.nanoTime();
	    	    nodetsmap.put(node.id, ts);
	    	    cleanupqueue.add(new NodeTS(node.id, ts));
	    	    for (int i=0; i< node.keycount; i++) {
	    	    	    keymap.put(node.keys[i].getKey(), new NodeKeyPos(node, node.keys[i], i));
	    	    }
	    	    while (nodemap.size() > this.size)  {
	    	         NodeTS cand = cleanupqueue.removeFirst();
	    	         Long check_ts = nodetsmap.get(cand.node_id);
	    	         if (check_ts!=null && cand.lastaccess >= check_ts) {
	    	        	     BTreeNode delnode = nodemap.get(cand.node_id);
	    	        	     for (int i=0; i< delnode.keycount; i++) {
	    	        	    	     keymap.remove(delnode.keys[i].getKey());
	    	        	     }
	    	        	     nodetsmap.remove(delnode.id);
	    	        	     nodemap.remove(delnode.id);
	    	        	     if (!readonly)
	    	        	        delnode.saveToStorage();
	    	         }
	    	         if (DebugPrint.debuglevel>0) {
	    	        	 DebugPrint.message(String.format("Cache: nodes %d, keys %d, cleanup queue %d nodes", nodemap.size(), keymap.size(), cleanupqueue.size()));
	    	        	 
	    	         }
	    	    }
	    	        
	    }
		
	}
	class KeyRightChild {
		public BTreeObject key;
		public long right;
		public KeyRightChild(BTreeObject key, long right) {
			this.key=key;
			this.right=right;
		}
	}
	public void dump(int seqlen, String dumpfname) throws DNAWrongSequenceLength, IOException, BTreeWrongBlockID, BTreeBadMetadata {
		LinkedList<KeyRightChild> tovisitStack = new LinkedList<KeyRightChild>();
		BufferedWriter buff = new BufferedWriter(new FileWriter(dumpfname));
		BTreeNode curnode=rootnode;
		while (curnode!=null) {
			if (curnode.isLeaf) {
				for (int i=0; i < curnode.keycount; i++) {
					BTreeObject o = curnode.keys[i];
					buff.write(o.getCounter() + " "+DNASequence.getDNAString(seqlen, o.getKey()));
					buff.newLine();
				}
				if (tovisitStack.isEmpty()) curnode=null;
				else {
					KeyRightChild nxt = tovisitStack.removeFirst();
					buff.write(nxt.key.getCounter() + " "+DNASequence.getDNAString(seqlen, nxt.key.getKey()));
					buff.newLine();
					curnode = getNode(nxt.right);
				}
				
			} else {
				for (int i = curnode.keycount; i > 0; i--) {
					tovisitStack.addFirst(new KeyRightChild(curnode.keys[i-1], curnode.children[i]));
				}
				curnode = getNode(curnode.children[0]);
			}
		} 
		buff.close();
	}

	public static void main(String[] args) throws IOException, BTreeBadMetadata, BTreeWrongBlockID, BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeFullNode, BTreeNonExactNonLeaf {
		//BTree test1 = new BTree("test1.tree", 2);
		//BTree test2 = new BTree("test2.tree", 4);
		//@SuppressWarnings("unused")
		/*System.out.println(Long.compare(0, 1));
		System.out.println(Long.compare(1, 0));
		System.out.println(Long.compare(0, 0));
		System.exit(0);*/
		BTree test = new BTree("test3.tree", false, true, 200);
		for (long i=9999999; i>=0; i-=3) {
			//System.out.println(i);
			if (i%10000 == 0) System.out.println(i);
			test.insertKey(new BTreeObject(i));
		}
		test.insertKey(new BTreeObject(8));
		test.insertKey(new BTreeObject(28));
		test.insertKey(new BTreeObject(39));
		test.insertKey(new BTreeObject(1405));
		test.insertKey(new BTreeObject(22405));
		test.insertKey(new BTreeObject(952405));
		BTreeObject tmp = new BTreeObject(1000000);
		tmp.IncCounter();
		tmp.IncCounter();
		test.insertKey(tmp);
		test.lookup(new BTreeObject(1000000)).foundkey.IncCounter();
		test.lookup(new BTreeObject(1000000)).foundkey.IncCounter();
		//test.insertKey(new BTreeObject(27));
		/*test.insertKey(new BTreeObject(10));
		test.insertKey(new BTreeObject(169));
		test.insertKey(new BTreeObject(170));
		test.insertKey(new BTreeObject(171));*/
		test.shutdown();
		
		BTree test1 = new BTree("test3.tree", true, false, 100);
		for (long i=0; i<=10000000; i+=1) {
			//if (i%10000 == 0) System.out.println(i);
		
			boolean check = test1.lookup(new BTreeObject(i)).exact;
			if ((i%3==0 && !check) || (i%3!=0 && check))
			   System.out.println(i + " "+  test1.lookup(new BTreeObject(i)).exact);
		}
		System.out.println(test1.lookup(new BTreeObject(1000000)).foundkey.toString());
		//System.currentTimeMillis()

	}

}
