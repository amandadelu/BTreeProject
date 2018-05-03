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
/**
 * main BTree class to hold BTreeObject as keys
 * @author amandadelu, 
 *
 */

public class BTree {
	private int degree; //tree degree >= 2
	private int order; //degree * 2
	private int maxkeycount; //max keys per node =order-1
	private int nodesize; //physical node size in bytes
	private long nodecount; //total nodes in the tree
	private final static int def_blocksize = 4096; //default block size for tree data
	private final static int metaDatasize = 4096;  //excessive metaDatasize
	private boolean readonly; //read only flag, if tree is opened for search only
	
	BTreeNode rootnode; //root node of the tree
	Cache cache; //cache instance if using Cache, otherwise null
	
	private RandomAccessFile storage; //object to work with the BTree file with disk
	private static final int node_overhead = 8*3; //additional data in node, 3 longs: node id, parent id, numkeys stored + leaf indicator
	private static int getNodeDataSize(int testdegree) { //estimate node size based on testdegree
		int testorder = 2 * testdegree; //calculate order
		//overhead + 8 bytes per child reference + (order-1) BTreeObjects
		return node_overhead + 8 * testorder + BTreeObject.size *(testorder - 1); 
	}
	//calculate the offset of the node
	private long getNodeOffset(long testid) {
		//zero node immediately after the metaData
		return metaDatasize + testid * nodesize;
	}
	//estimate the maximal degree to fit the node into def_blocksize
	private static int maxdegree(int testnodesize) {
		int testdegree = 2; //minimal degree
		int minnodesize = getNodeDataSize(testdegree); //minimum node size
		if (minnodesize >= def_blocksize) { //stick to minimum degree
           return testdegree;
		} else {
			
			return (def_blocksize - node_overhead + BTreeObject.size) / (8 + BTreeObject.size) / 2;
		}
	}
	
	class MedianNode {
		// Class to carry median key from node split to left and right 
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
		//supplies key navigates to nearest possible key
		//dual purpose: finds a key or stops at the insert position for the key if its not in the tree
		public NearestSearchResult(BTreeObject searchkey) throws BTreeWrongBlockID, BTreeBadMetadata, IOException, BTreeNonExactNonLeaf {
			NodeKeyPos checkcache; //create empty cache
			if (cache!=null) {
				checkcache = cache.byKey(searchkey.getKey()); //get it from cache
				if (checkcache!=null) {
					foundkey=checkcache.key; //key found in cache
					node=checkcache.node; //node from cache that contains key
					pos=checkcache.pos; //key pos in node
					exact=true; //mark it as exact
					return; //done 
				}
			}
			
			needkey = searchkey; //when dont use cache or key is missing
			BTreeNode prevnode=null; //temp var for prev node
			node=rootnode; //starting from the root, traverse the tree node by node
			while (true) {
				pos = node.searchkey(needkey); //find position of nearest key inside the node
				if ((pos >= node.keycount || node.keys[pos].compareTo(needkey)!=0)) {
					//check if we have not found matching key
					if (!node.isLeaf) { //positioned to descend 
						prevnode = node; //save current node to previous
						node = getNode(node.children[pos]); //advanced current node to children node at pos where we stopped
						if (node.id_parent!=prevnode.id) { //check parent and child relationship
							System.err.printf("%d %d %d %d%n", prevnode.id, prevnode.id_parent, node.id, node.id_parent);
							throw new BTreeWrongBlockID();
						}
					} else break; //no where to descend in the leaf
				} else break; //found exact match
			}
			exact = (pos < node.keycount && node.keys[pos].compareTo(needkey)==0); //check if found exact, key equal to key we searched for
			if (!exact && !node.isLeaf) throw new BTreeNonExactNonLeaf(); //check, non exact match only possible in leaf
			if (exact) {
				foundkey = node.keys[pos]; //set found key for exact match if found 
			} else {
				foundkey=null;
			}
			
			
		}
	}
	//interfaces, takes key and returns nearest search result instance
	public NearestSearchResult lookup(BTreeObject key) throws BTreeWrongBlockID, BTreeBadMetadata, BTreeNonExactNonLeaf, IOException {
		return new NearestSearchResult(key);
	}
	//looks for nearest key and inserts the key there
	public void insertKey(BTreeObject key) throws BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeWrongBlockID, BTreeBadMetadata, BTreeFullNode, BTreeNonExactNonLeaf, IOException {
		insertToFoundLoc(new NearestSearchResult(key));
	}
	//inserts the needkey from nearest search result 
	public void insertToFoundLoc(NearestSearchResult res) throws BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeWrongBlockID, BTreeBadMetadata, IOException, BTreeFullNode {
		if (res.exact) { //check if search result is exact, dont insert in that case
			return;
		}
        BTreeNode insnode=res.node; //provides insertion point 
        MedianNode med = new MedianNode(res.needkey, null, null); //dummy median node to insert to leaf
        int pos = res.pos; //get insertion position in the current node
        while (insnode.keycount >= maxkeycount) { //while node is full split it exact median and try to insert it to the parent
        	//current node is full
        	     med = insnode.splitNode(med.key, pos, med.right); //split node and obtain a new median with right and left child

        	     if (insnode.id_parent==-1) { //if we split root node then break nothing else to split nothing above root node
        	    	     insnode = null;
        	    	     break;
        	     }
        	     BTreeNode parent = getNode(insnode.id_parent); //otherwise move to the parent of the node we split

        	     if (insnode.id_parent!=parent.id) throw new BTreeWrongBlockID(); //check that id parent is actually parent id of current node
        	     insnode = parent; //set insertion node to parent
        	     pos = insnode.searchkey(med.key); //figure out insertion position for the new median
        }
        if (insnode!=null) { //found the internal node with space
        	    insnode.insertkey(med.key, pos, med.right); //node function to insert the key in the node which has space
        	    if (insnode.id_parent==-1) rootnode = insnode; //in case were inserting in root node update root node reference
               // insnode.save();
        } else {
        	    rootnode = new BTreeNode(nodecount++, -1, false); //if we end up splitting root itself, create new root containing the last median with left and right children
        	    rootnode.keycount=1; //only one key, the last median
        	    rootnode.keys[0] = med.key; //set what the key is
        	    rootnode.children[0] = med.left.id; //set children left and right
        	    rootnode.children[1] = med.right.id; 
        	    med.left.id_parent=rootnode.id; //came without knowing who parent is set new root as parent
        	    med.right.id_parent=rootnode.id;
        	    rootnode.save(); //save all three
        	    med.left.save();
        	    med.right.save();
        }
        
	}
	/*
	 * inner class that represents a node, node holds up to "order" of children node ids
	 * and order - 1 BTreeObject as keys
	 */
	class BTreeNode {
		private long id; //node id
		private long id_parent; //parent id
		private long[] children; //array of children ids
		private BTreeObject[] keys; //array of keys
		private int keycount; //actual number of keys stored in node
		private boolean isLeaf; //leaf indicator
		
		/*
		 * constructor creates a node from id, parent, and leaf indicator. Allocates
		 * storage for keys and children ids
		 */
		public BTreeNode(long id,long id_parent,boolean isLeaf) {
			this.id = id;
			this.id_parent = id_parent;
			this.isLeaf = isLeaf;
			children = new long[order]; //allocates children as order of longs
			for (int i=0; i< children.length; i++) children[i]=-1; //mark as uninitialized
			keys = new BTreeObject[maxkeycount]; //initialize keys
			keycount = 0; //set key count to zero, node is empty
			
		}
		//function searches for nearest possible key inside the node
		//returns the index for the first key larger or equal to sought key
		//binary search
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
		//helper function to validate the key for insertion at the given position
		//also makes sure the right child is valid for non leaf node
		private void checkKeyOrder(BTreeObject key, int pos, BTreeNode rightchild) throws BTreeWrongKeyOrder, BTreeNoInternalNodeChild {			
			if (pos > 0 && key.compareTo(keys[pos-1]) != 1)
				throw new BTreeWrongKeyOrder();
			if (pos < keycount && key.compareTo(keys[pos]) != -1)
				throw new BTreeWrongKeyOrder();
			
			if (rightchild==null & !isLeaf)
				throw new BTreeNoInternalNodeChild();			
		}
		//insert to not full node 
		public void insertkey(BTreeObject key, int pos, BTreeNode rightchild) throws BTreeWrongKeyOrder, BTreeFullNode, BTreeNoInternalNodeChild, IOException {
			if (keycount == maxkeycount)
				throw new BTreeFullNode();
			checkKeyOrder(key, pos, rightchild); //check key order for insert position
			for (int i = keycount; i>pos; i--) { //move keys from insert pos to the right
				keys[i] = keys[i-1]; //free from the right all the way to insert position
			}
			keys[pos] = key; //put key to insert position
			if (!isLeaf) { //move all the right children by one to the right 
				for (int i = keycount; i>pos; i--) {
					children[i+1] = children[i];
				}
				children[pos+1] = rightchild.id; //insert the right child
				rightchild.id_parent = this.id; //update right child's parent id
				rightchild.save(); //save the right child
			}
			keycount++; //advance key count
			save(); //save current node
		}
		//inserts into full node by splitting it on the median and pushing the median to the parent node
		public MedianNode splitNode(BTreeObject key, int pos, BTreeNode rightchild) throws BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeWrongBlockID, BTreeBadMetadata, IOException {
			if (keycount < maxkeycount) //check if node is not full
				throw new BTreeNotFullNode(); 
			checkKeyOrder(key, pos, rightchild); //check if insertion position is correct
			long[] tmpchildren = new long[order+1]; //allocate temp array for children with one extra space
			BTreeObject[] tmpkeys = new BTreeObject[maxkeycount+1]; //allocate temp array for keys with one extra space
			for (int i = 0; i < pos; i++) { //move all the keys and left children before the insert position to temp array
				tmpkeys[i] = keys[i]; 
				tmpchildren[i] = children[i];
				keys[i] = null;
			}
			tmpchildren[pos] = children[pos]; //move the left child of the insert position
			tmpkeys[pos] = key; //insert the key
			if (!isLeaf) { //if not a leaf
				tmpchildren[pos+1] = rightchild.id;	//insert the new right child to the right of insert position
			} 
			for (int i = pos; i < keycount; i++) { //move the rest of the keys and right children
				tmpkeys[i+1] = keys[i];
				tmpchildren[i+2] = children[i+1];
				keys[i] = null;
			}
			
			int mid = tmpkeys.length / 2; //identify median index
			keycount = mid; //old location keep keys and children left to median
			for (int i = 0; i < keycount; i++) { //move the keys and left children before median to their original node
				children[i] = tmpchildren[i];
				keys[i] = tmpkeys[i];
			}
			children[mid] = tmpchildren[mid]; //move the last right child to the original node
			BTreeNode newnode = new BTreeNode(nodecount++, id_parent, isLeaf); //create new node

		
			for (int j=0, i=(mid + 1); i< tmpkeys.length; i++, j++) { //move keys and left children past the median to the new node
				newnode.keys[j] = tmpkeys[i]; //move the key
				if (!isLeaf) {
					BTreeNode tmpchild = getNode(tmpchildren[i]); //go into each child and update the parent
					newnode.children[j] = tmpchild.id;
					tmpchild.id_parent=newnode.id;
					tmpchild.save();
				}
			}
			newnode.keycount = tmpkeys.length - (mid + 1); //update key count for a new node
			if (!isLeaf) {
				BTreeNode tmpchild = getNode(tmpchildren[tmpchildren.length-1]); 
				newnode.children[newnode.keycount] = tmpchild.id; //move and update the last right child
				tmpchild.id_parent=newnode.id;
				tmpchild.save();
			}
			this.save(); //save current node
			newnode.save(); //save split node
			return new MedianNode(tmpkeys[mid], this, newnode); //return median key with left child and right child
		}
		//save function saves to storage or cache if using cache
		void save() throws IOException {
			if (cache==null) saveToStorage();
			else cache.AddToCache(this);
		}
		//physically saves node to the file
		void saveToStorage() throws IOException {
			// Make sure the file is long enough
			long needfilesize = getNodeOffset(id + 1); //allocate the file space for the node
			if (storage.length() < needfilesize) storage.setLength(needfilesize); 
			storage.seek(getNodeOffset(id)); //navigate to node location in file
			byte[] buff = new byte[nodesize]; //allocate the byte buffer to hold the node
			LongBuffer l = ByteBuffer.wrap(buff).asLongBuffer(); //set up as buffer of long
			l.put(id); //put node id
			l.put(id_parent); //put parent id
			l.put((((long)keycount) << 8) | (isLeaf?1:0)); //combine and write 4 byte key count and boolean id into 8 byte
			for (long cref: children) {
				l.put(cref); //write each child id
			}
			for (int i=0; i<keycount; i++) { //for every valid key ask it to write itself to the buffer
				keys[i].toBuffer(l);
			}
			storage.write(buff); //write buffer to file
		}
		
		void loadFromStorage() throws IOException, BTreeWrongBlockID, BTreeBadMetadata {
			storage.seek(getNodeOffset(id)); //navigate to node location file
			byte[] buff = new byte[nodesize]; //allocate the byte buffer to hold the node
			storage.read(buff); //reads buffer
			LongBuffer l = ByteBuffer.wrap(buff).asLongBuffer(); //set up as buffer of long
			long check_id = l.get(); //read data peice by peice
			if (id!=check_id) throw new BTreeWrongBlockID(); //id from file matches the requested node id
			id_parent = l.get(); //read parent
			long tmp = l.get(); //read combined key count and leaf indicator
			keycount = (int)(tmp >> 8); //extract key count
			if (keycount > maxkeycount) throw new BTreeBadMetadata(); //check key count is valid
			isLeaf = ((tmp & 1) == 1); //extract leaf indicator
	
			for (int i=0; i<children.length; i++) { //read children node ids
				children[i]=l.get(); 
			}
			for (int i=0; i<keycount; i++) { //tell keys to read themselves
				keys[i] = new BTreeObject(0); //create empty key
				keys[i].fromBuffer(l); //tell it to read itself
			}
			if (cache!=null) cache.AddToCache(this); //add node to cache if using cache
			
		}
	}
	//obtain the node from cache or physical storage, provide id of node you want
	private BTreeNode getNode(long id) throws IOException, BTreeWrongBlockID, BTreeBadMetadata {
		BTreeNode ret=null; //allocate reference to node
		if (cache!=null) //check if cache is not null try to obtain from cache
		   ret = cache.byNodeID(id); 
		if (ret==null) { //if not using cache or node is not in cache 
			ret = new BTreeNode(id, -1, false); //create empty instance of the node
			ret.loadFromStorage(); //load it from storage
		} 
		return ret;
	}
	
/*
 * constructor initialize a tree with optimal degree or open existing tree
 */
	public BTree(String fname, boolean readonly, boolean init, int cachesize) throws IOException, BTreeBadMetadata, BTreeWrongBlockID {
		if (init) { //initialize new tree
			setupOptimalTree(); //figure out degree
			init_btree(fname); //ready to initialize btree
		} else { //if not asking to initialize just open btree
			open_btree(fname, readonly); 
		}
		setupCache(cachesize); //set up cache
		this.readonly=readonly; //read only will be determined by arguement 
	}
	/*
	 * constructor initialize the tree with specific degree
	 */
	public BTree(String fname, int degree, int cachesize) throws IOException {
		setupTreeFromDegree(degree); //calculate tree parameters based on degree
		init_btree(fname); //initialize btree
		setupCache(cachesize); //set up cache
		readonly=false; //fresh tree 
	}
	//initialize cache with given size if size more than zero
	private void setupCache(int cachesize) {
		if (cachesize > 0) {
			cache = new Cache(cachesize);
		} else { //otherwise null
			cache = null;
		}
	}
	//function to open btree and verify the metadata
	private void open_btree(String fname, boolean readonly) throws IOException, BTreeBadMetadata, BTreeWrongBlockID {
		storage = new RandomAccessFile(fname, readonly?"r":"rw"); //open file with desired mode read or write
		byte[] buf = new byte[metaDatasize]; //allocate buffer to read metaData
		LongBuffer l = ByteBuffer.wrap(buf).asLongBuffer(); //set up buffer as buffer for long
		storage.seek(0); //move to pos zero in file
		storage.read(buf); //read metaData
		degree = (int)l.get(); //extract from metaData piece by piece
		order = (int)l.get(); //reads the data in same order how save metaData writes them
		maxkeycount= (int)l.get(); 
		if (order != 2 * degree || maxkeycount != order -1) //order is double of degree
			throw new BTreeBadMetadata(); //max key count is order - 1
		nodecount = l.get(); //read node count
		nodesize = (int)l.get(); //get nodesize
		if (nodesize < getNodeDataSize(degree)) //nodesize cannot be smaller than the minimum size to store node data
			throw new BTreeBadMetadata(); 
		if (storage.length() != getNodeOffset(nodecount)) //check filesize vs node count
			throw new BTreeWrongFileSize();
		int objsize = (int)l.get(); //read objsize 
		if (objsize != BTreeObject.size) //checking its correct
			throw new BTreeWrongObjectSize();
		long rootid = l.get(); //obtain id of root node
		if (getNodeOffset(1 + rootid) > storage.length()) //to make sure root id represents node in tree
			throw new BTreeWrongRootNode();
		rootnode = getNode(rootid); //read root node from file or cache
			
		if (rootnode.id_parent != -1) //root node should have no parent
			throw new BTreeWrongRootNode();
	}
	//new tree initialization assuming tree, degree, order, node size all set up
	private void init_btree(String fname) throws IOException { 
		storage = new RandomAccessFile(fname + "." + degree, "rw"); //create the new tree file
		storage.setLength(getNodeOffset(1)); //sets the length for metaData and node zero
		nodecount = 1; //fresh tree only has one node
		rootnode = new BTreeNode(0, -1, true); //create empty root node
		rootnode.save(); //save root node
		saveMetaData(); //save MetaData
		
		
	}
	//save tree MetaData
	private void saveMetaData() throws IOException {
		byte[] buf = new byte[metaDatasize]; //create buffer
		LongBuffer l = ByteBuffer.wrap(buf).asLongBuffer(); //set up as buffer of longs
		l.put(degree); //put degree data 
		l.put(order); //put order data
		l.put(maxkeycount); //put maxkeycount data
		l.put(nodecount); //put node count how many nodes in tree 
		l.put(nodesize); //put node size
		l.put(BTreeObject.size); //size of the key object
		l.put(rootnode.id); //id of the root node, where it starts
		storage.seek(0); //navigate to beginning of file
		storage.write(buf); //write buffer to file
		
	}
	//calculates optimal degree order max key count and node size
	private void setupOptimalTree() { 
		this.degree = maxdegree(def_blocksize); //calculate maximum degree to fit one node into block
		this.order = this.degree << 1; //calculate order double of degree
		this.maxkeycount = this.order - 1; //maxkeycount as order - 1
		this.nodesize = ((getNodeDataSize(degree) - 1) / def_blocksize + 1) * def_blocksize; 
	}


	
	
	private void setupTreeFromDegree(int degree) {
		if (degree < 2) throw new IllegalArgumentException(); //check degree
		this.degree = degree; //store degree
		this.order = degree << 1; //calculate order
		this.maxkeycount = this.order - 1; //calculate maxkeycount
		this.nodesize = getNodeDataSize(degree); //do whatever node size for this degree
		
	}
	
	public void shutdown() throws IOException {
		if (!readonly) { //if not open for read only save data
			if (cache!=null) //active cache save everything to disk
			   cache.flush(); 
			saveMetaData(); //save tree MetaData
		}
		storage.close(); //close the file
	}
	
	/*
	 * inner cache class
	 */
	class Cache {
		class NodeTS { //holds node id and time stamp of last access for cleanup queue
			public long node_id; //initialize
			public long lastaccess;
			public NodeTS (long node_id, long ts) { 
				this.node_id = node_id;
				this.lastaccess = ts;
			}
		}
		/*
		 * cache entry for the key
		 * stores the key the node that contains key and position of key in node
		 */
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
		private HashMap<Long, BTreeNode> nodemap; //hashmap to look up node instance by node id 
		private HashMap<Long, Long> nodetsmap; //hashmap for node id and last access time stamp 
		private HashMap<Long, NodeKeyPos> keymap; //hashmap for keys to look up node key position
		private int size; //maximum cache size
		private LinkedList<NodeTS> cleanupqueue; //clean up queue, pairs of node id and time stamp when placed in queue
		/*
		 * constructor initializes all as empty and sets size to desired size
		 */
		public Cache(int size) {
			this.size=size;
			nodemap = new HashMap<Long, BTreeNode>();
			keymap = new HashMap<Long, NodeKeyPos>();
			nodetsmap = new HashMap<Long, Long>();
			cleanupqueue = new LinkedList<NodeTS>();
		}
		//physically writes the cache contents to the disk
		public void flush() throws IOException {
			for (BTreeNode node: nodemap.values()) {
				node.saveToStorage(); //tell each node to save itself
			}
		}
		//look up by node id
		public BTreeNode byNodeID(long node_id) throws IOException {
			BTreeNode ret = nodemap.get(node_id); 
			if (ret!=null)
				AddToCache(ret); //refresh it in cache if found 
			return ret;
		}
		
		public NodeKeyPos byKey(long needkey) {
			return keymap.get(needkey); //lookup class that holds node key and position by key 
		}
		//how you add node to cache
	    public void AddToCache(BTreeNode node) throws IOException {
	    	    nodemap.put(node.id, node); //put node to hashmap by its node id
	    	    long ts = System.nanoTime(); //mark the time stamp 
	    	    nodetsmap.put(node.id, ts); //put the time stamp to the hashmap of time stamps by node id
	    	    cleanupqueue.add(new NodeTS(node.id, ts)); //puts the node id with current time stamp in the clean up queue
	    	    for (int i=0; i< node.keycount; i++) { //put all keys from the node to the key 
	    	    	    keymap.put(node.keys[i].getKey(), new NodeKeyPos(node, node.keys[i], i)); //builds structure for where to find key
	    	    }
	    	    while (nodemap.size() > this.size)  { //make sure we don't hold more nodes than cache size
	    	         NodeTS cand = cleanupqueue.removeFirst(); //take oldest entry from clean up queue
	    	         Long check_ts = nodetsmap.get(cand.node_id); //obtain the current time stamp for the node from the clean up queue
	    	         if (check_ts!=null && cand.lastaccess >= check_ts) { //check if the node was accessed meaning its current time stamp is larger than the one saved in clean up queue
	    	        	     BTreeNode delnode = nodemap.get(cand.node_id); //if not the case, proceed to deleting node
	    	        	     for (int i=0; i< delnode.keycount; i++) { 
	    	        	    	     keymap.remove(delnode.keys[i].getKey()); //delete all keys from keymap that belong to this node
	    	        	     }
	    	        	     nodetsmap.remove(delnode.id); //remove the current time stamp from this node
	    	        	     nodemap.remove(delnode.id); //remove the node itself from cache 
	    	        	     if (!readonly) //if not read only, save the node
	    	        	        delnode.saveToStorage();
	    	         }
	    	         if (DebugPrint.debuglevel>0) { //prints cache statistics
	    	        	 DebugPrint.message(String.format("Cache: nodes %d, keys %d, cleanup queue %d nodes", nodemap.size(), keymap.size(), cleanupqueue.size()));
	    	        	 
	    	         }
	    	    }
	    	        
	    }
		
	}
	/*
	 * helper class for the dump to bind key and right child to store in the tovisitStack
	 */
	class KeyRightChild {
		public BTreeObject key;
		public long right;
		public KeyRightChild(BTreeObject key, long right) {
			this.key=key;
			this.right=right;
		}
	}
	//dump tree in the inorder (LNR)
	//using non recursive algorithm with stack 
	public void dump(int seqlen, String dumpfname) throws DNAWrongSequenceLength, IOException, BTreeWrongBlockID, BTreeBadMetadata {
		LinkedList<KeyRightChild> tovisitStack = new LinkedList<KeyRightChild>(); //stack of left behind nodes
		BufferedWriter buff = new BufferedWriter(new FileWriter(dumpfname)); //output file
		BTreeNode curnode=rootnode; //start from the root node
		while (curnode!=null) { //until there are no more nodes 
			if (curnode.isLeaf) { //check if node is leaf or not
				for (int i=0; i < curnode.keycount; i++) { //output all the keys from left to right 
					BTreeObject o = curnode.keys[i]; 
					buff.write(o.getCounter() + " "+DNASequence.getDNAString(seqlen, o.getKey()));
					buff.newLine();
				} //after you output leaf content check the tovisitStack for left behind branches of tree
				if (tovisitStack.isEmpty()) curnode=null;
				else { //if not empty extract very first entry and traverse 
					KeyRightChild nxt = tovisitStack.removeFirst(); 
					//output the key left behind 
					buff.write(nxt.key.getCounter() + " "+DNASequence.getDNAString(seqlen, nxt.key.getKey()));
					buff.newLine();
					curnode = getNode(nxt.right); //proceed to right child
				}
				
			} else { //if its not a leaf, put all keys with right children into tovisitStack starting from the largest key 
				for (int i = curnode.keycount; i > 0; i--) {
					//push every key with its right child to the stack
					tovisitStack.addFirst(new KeyRightChild(curnode.keys[i-1], curnode.children[i]));
				}
				curnode = getNode(curnode.children[0]); //move on to the very left child
			}
		} 
		buff.close();
	}

	public static void main(String[] args) throws IOException, BTreeBadMetadata, BTreeWrongBlockID, BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeFullNode, BTreeNonExactNonLeaf {
		//the function to test btree with sequence of numbers
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
