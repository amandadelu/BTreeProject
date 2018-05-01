package ncbi;

import bterrors.DNABadCode;
import bterrors.DNAWrongSequenceLength;

public class DNASequence {
	public static long Let2Bin(char l) throws DNABadCode {
		switch(l) {
		case 'A':
		case 'a': return 0b00;
		case 'T':
		case 't': return 0b11;
		case 'C':
		case 'c': return 0b01;
		case 'G':
		case 'g': return 0b10;
		default: throw new DNABadCode();
		}
	}
	public static char Bin2Let(long b) throws DNABadCode {
		switch((short)b & 0b11) {
		case 0b00: return 'A';
		case 0b11: return 'T';
		case 0b01: return 'C';
		case 0b10: return 'G';
		default: throw new DNABadCode();
		}
	}
	
	private long acc, leftmost; //, bitmask;
	private int seqlen, needlen;
	
	public long getDNAKey() throws DNAWrongSequenceLength {
		if (needlen > 0) throw new DNAWrongSequenceLength();
		return acc;
	}
	public boolean isComplete() {
		return needlen == 0;
	}
	public DNASequence(int seqlen) throws DNAWrongSequenceLength {
		if (seqlen < 1 || seqlen > 31) throw new DNAWrongSequenceLength();
		this.seqlen = seqlen;
		// bitmask is all 1 except for 2 leftmost bits
		// subrtact 1 from seqlen to get # of masked letters
		// mult by 2 (left bit shift by 1) to get the # of bits
		// left shift 1 by that number of bits
		// subtract 1 to obtain the appropriate # of ones 
		//bitmask = (1 << ((seqlen - 1) << 1)) - 1;
		
		//Alternative, (seqlen - 1) * 2, by how many bits to move 
		//to insert the new data
		leftmost = (seqlen - 1) << 1;
		reset();
	}
	
	public void reset() {
		acc = 0;
		needlen = seqlen;
	}
	
	public void pushCodon(char c) throws DNAWrongSequenceLength {
		try {
			//acc = ((acc & bitmask) << 2) | Let2Bin(c);
			
			//Move the accumulater 2 bits right
			//discard the rightmost 2 bits and set the leftmost two bits with new data
			//  // (acc / 4) + (Let2Bin(c) * 2^leftmost)
			acc = (acc >> 2) | (Let2Bin(c) << leftmost);
			switch(needlen) {
			case 0:
				break;
			case -1:
				throw new DNAWrongSequenceLength();
			default:
				needlen -= 1;
				break;
			}
		} catch(DNABadCode e) {
			reset();
		}
	}
}
