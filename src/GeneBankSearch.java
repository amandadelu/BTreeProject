import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import bterrors.BTreeBadMetadata;
import bterrors.BTreeFullNode;
import bterrors.BTreeNoInternalNodeChild;
import bterrors.BTreeNonExactNonLeaf;
import bterrors.BTreeNotFullNode;
import bterrors.BTreeWrongBlockID;
import bterrors.BTreeWrongKeyOrder;
import bterrors.DNASequenceNotFound;
import bterrors.DNAWrongSequenceLength;
import btree.BTree;
import btree.BTreeObject;
import btree.BTree.NearestSearchResult;
import ncbi.DNAInput;
import ncbi.DNASequence;

public class GeneBankSearch {

    public static void usage() {
    	System.out.println("java GeneBankSearch <0/1(no/with Cache)> <btree file> <query file>"
    			+ " <cache size> [<debug level>]");
    }
	public static void main(String[] args) throws IOException, BTreeBadMetadata, BTreeWrongBlockID, DNAWrongSequenceLength, InterruptedException, DNASequenceNotFound, BTreeNonExactNonLeaf, BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeFullNode {
		boolean usecache;
		String btreefname;
		String queryname;
		int cachesize;

		if (args.length< 4 || args.length > 5) {
			usage();
			return;
		}
		try {
			switch(Integer.parseInt(args[0])) {
			case 0: usecache=false; break;
			case 1: usecache=true; break;
			default: throw new IllegalArgumentException();
			}
			btreefname = args[1];
			queryname = args[2];
			cachesize = Integer.parseInt(args[3]);
			if (usecache) {
				  if(cachesize < 1)
					  throw new IllegalArgumentException();
			} else cachesize=0;
			
			if (args.length == 5) {
				DebugPrint.debuglevel = Integer.parseInt(args[4]);
				if (DebugPrint.debuglevel < -1 || DebugPrint.debuglevel > 1) {
					throw new IllegalArgumentException();
				}
			}
			
		} catch(Exception e) {
			usage();
			return;
		}
		
		BTree dnatree=new BTree(btreefname, true, false, cachesize);
		//DNAInput dnaparser = new DNAInput(fname, seqlen);
		int seqlen = -1;
		BufferedReader buff = new BufferedReader(new FileReader(queryname));
		String query = buff.readLine();
		while (query!=null) {
			query = query.trim().toLowerCase();
			if (seqlen==-1)
				seqlen = query.length();
			else if (seqlen !=query.length()) 
				throw new DNAWrongSequenceLength();
			DNASequence seq = new DNASequence(seqlen);
			for (char c: query.toCharArray()) {
				seq.pushCodon(c);
			}
			if (!seq.isComplete()) throw new DNAWrongSequenceLength();
			//System.out.println(tmp + " " + Long.toBinaryString(seq.getDNAKey()));
			NearestSearchResult lkp = dnatree.lookup(new BTreeObject(seq.getDNAKey()));
			if (lkp.exact) {
				System.out.println(query + ": " + lkp.foundkey.getCounter());
			} else {
				//System.out.println(tmp + ": <not found>");
			}
			
			query = buff.readLine();
		}

		buff.close();
		dnatree.shutdown();		
		

	}

}
