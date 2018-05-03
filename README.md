# BTreeProject
CS321 Spring 2018 semester project
**********************************************
* Amanda Delu, Josh Sanders, PJ Cory
* CS321
* BTree Project
**********************************************

-OVERVIEW:

This project is meant to be a culmination of the skills and techniques we learned in this class. The BTree project focuses on reading in a DNA file, parsing the read-in DNA in subsequences with a specified length, and then creating a BTree using the DNA subsequences as keys for the nodes in the tree.
 
-INCLUDED FILES/FOLDERS:

* DNAInput.java - This file reads a given gbk file and compiles a string of the DNA.
* DNASequence.java - This file parses the given DNA string into subsequences of a specified length.
* BTree.java - This file contains the constructor and relevant methods for the BTree we use.
* BTreeObject.java - This object contains the constructor and methods for nodes in the BTree.
* /bterrors - folder containing all custom Exceptions created for this project.
 * README.txt - This file          

-COMPILING AND RUNNING:

$javac GeneBankSearch;    

$ java GeneBankSearch <0/1(no/with Cache)> <btree file> <query file> <cache size> [<debug level>] ;

-PROGRAM DESIGN:

This project is split into three major parts, reading gbk files, parsing into subsequences, and the BTree. Part one, reading the gbk files is handled by DNAInput.java. It scans in the given text file until is finds the “ORIGIN” string and then  proceeds to convert all of the DNA code following it into a string of DNA code. DNASequence.java handles the parsing of the aforementioned DNA Code. DNASequence takes a int as the length of the DNA subsequences it’s supposed to return. DNASequence also contains all conversion methods to turn the binary key representations into a string and vice versa. The third portion of the project dealt with the Btree and BtreeObjects. BTreeObjects handled all of the storage on the disc. Storage on the disc is mainly handled by the cache save function of our program. We hand problems with the storage initially, losing nodes and file paths, but we were eventually able to solve it by having parent locations saved in node metadata.


 
-TESTING:

To test this project we each created a test class to test an individual part of the project. DNAInputTest.java, etc (found src file). This allowed us to immediately test the functionality of the portion of the project that we were each responsible for. Test classes included tests for null pointers, varying lengths of DNA subsequences, constructing and storing BTrees, and tests for all of the different command line arguments that the project is expected to handle. By doing this we were able to ensure that each portion of our project functioned properly both on its own as well as together. Throughout the process there were only a couple instances of crippling bugs, however, they were mostly due just to all of us using different naming conventions initially. Once we all got on the same page we were able to finish the project without issue.

-EXTRA CREDIT:
 
 No extra credit offered/attempted. 
