package btree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

import bterrors.BTreeWrongBlockID;

public class BTree {
	private int degree;
	private int order;
	private int maxchildren;
	private int nodesize;
	private long nodecount;
	private final static int def_blocksize = 4096;
	private final static int metaDatasize = 4096;
	
	private RandomAccessFile storage;
	private static final int node_overhead = 8*3;
	private static int getNodeDataSize(int testdegree) {
		int testorder = 2 * testdegree;
		return node_overhead + 8 * testorder + BTreeObject.size *(testorder - 1);
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
			keys = new BTreeObject[order - 1];
			keycount = 0;
			
		}
		
		void save() throws IOException {
			// Make sure the file is long enough
			long needfilesize = metaDatasize + (id + 1) * nodesize;
			if (storage.length() < needfilesize) storage.setLength(needfilesize);
			storage.seek(metaDatasize + id * nodesize);
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
			storage.seek(metaDatasize + id * nodesize);
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
	
	
	public BTree(String fname, boolean readonly, boolean init) {
		if (init) {
			setupOptimalTree();
			init_btree(fname);
		} else {
			open_btree(fname, readonly);
		}
	}
	
	public BTree(String fname, int degree) {
		setupTreeFromDegree(degree);
		init_btree(fname);
	}
	
	

	public static void main(String[] args) {
		

	}

}
