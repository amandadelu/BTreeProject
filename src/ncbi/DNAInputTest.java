package ncbi;

import java.io.IOException;
import java.util.Scanner;

import bterrors.DNASequenceNotFound;
import bterrors.DNAWrongSequenceLength;

public class DNAInputTest {

	private static Scanner input;

	public static void main(String[] args) throws DNAWrongSequenceLength, IOException, DNASequenceNotFound, InterruptedException {
		// TODO Auto-generated method stub
		file_test();
		
		/*Scanner tmp = new Scanner(System.in);
		byte b = 0;
		while (b != 32) {
			b = tmp.nextByte();
		}*/
		


	}
	
	private static void file_test() throws DNAWrongSequenceLength, IOException, DNASequenceNotFound, InterruptedException {
        input = new Scanner(System.in);
        System.out.print("Please enter a sequence length (1-31):");
        int seqlen = input.nextInt();
        String fmt_seq = "%" + (seqlen << 1) + "s%n";
		DNAInput test = new DNAInput("7263_ref_ASM165402v1_chr2.gbk", seqlen);
		while (test.hasNext()) {
			long nxt = test.Next();
			System.out.printf(fmt_seq, Long.toBinaryString(nxt));
		}
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
