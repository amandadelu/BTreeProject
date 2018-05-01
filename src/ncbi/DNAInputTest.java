package ncbi;

import java.io.IOException;
import java.util.Scanner;

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
import btree.BTree.NearestSearchResult;
import btree.BTreeObject;

public class DNAInputTest {

	private static Scanner input;

	public static void main(String[] args) throws DNAWrongSequenceLength, IOException, DNASequenceNotFound, InterruptedException, BTreeBadMetadata, BTreeWrongBlockID, BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeFullNode, BTreeNonExactNonLeaf {
		// TODO Auto-generated method stub
		//file_test();
		read_file_test();
		/*Scanner tmp = new Scanner(System.in);
		byte b = 0;
		while (b != 32) {
			b = tmp.nextByte();
		}*/
		


	}
	
	private static void read_file_test() throws BTreeBadMetadata, BTreeWrongBlockID, IOException, BTreeNonExactNonLeaf {
		BTree dnatree = new BTree("dna0.btree", true, false, 1000);
		NearestSearchResult lkp = dnatree.lookup(new BTreeObject(68719476736L));
		System.out.println(lkp.foundkey.toString());
		dnatree.shutdown();
	}
	
	private static void file_test() throws DNAWrongSequenceLength, IOException, DNASequenceNotFound, InterruptedException, BTreeBadMetadata, BTreeWrongBlockID, BTreeNotFullNode, BTreeWrongKeyOrder, BTreeNoInternalNodeChild, BTreeFullNode, BTreeNonExactNonLeaf {
        input = new Scanner(System.in);
        System.out.print("Please enter a sequence length (1-31):");
        int seqlen = input.nextInt();
        String fmt_seq = "%" + (seqlen << 1) + "s%n";
		DNAInput test = new DNAInput("7263_ref_ASM165402v1_chr2.gbk", seqlen);
		BTree dnatree = new BTree("ddna1.btree", false, true, 500);
		int cnt = 62;
		while (test.hasNext() && cnt-->0) {
			long nxt = test.Next();
			//System.out.print(nxt+" ");
			//dnatree.insertKey(new BTreeObject(nxt));
			NearestSearchResult lkp = dnatree.lookup(new BTreeObject(nxt));
			if (!lkp.exact) {
				dnatree.insertToFoundLoc(lkp);
				lkp = dnatree.lookup(new BTreeObject(nxt));
			}
			
			if (lkp.exact) {
				lkp.foundkey.IncCounter();
				//if (lkp.foundkey.getCounter()>100)
				   System.out.println(lkp.foundkey.toString());
			}
			
			//System.out.printf(fmt_seq, Long.toBinaryString(nxt));
		}
		dnatree.shutdown();
	}
	
	private static void seq_test() throws DNAWrongSequenceLength {
        input = new Scanner(System.in);
        int seqlen = input.nextInt();
        DNASequence s = new DNASequence(seqlen);
        String fmt_seq = "%" + (seqlen << 1) + "s%n";
        char c='0';
        do {
        	   c = input.next().charAt(0);
        	   s.pushCodon(c);
        	   if (s.isComplete())
        	      System.out.printf(fmt_seq, Long.toBinaryString(s.getDNAKey()));
        } while (c!='z');
	}

}
