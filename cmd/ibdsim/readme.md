# ibdsim

This simulates initial block download (IBD) using the utreexo accumulator.
There are 2 things it can do depending on which function is called in simcmd: BuildProofs() (in genproofs.go) or RunIBD() (in ibdsim.go).

In BuildProofs, two things are done asynchronously. First, the proofs are build with writeProofs(). Second, the ttl (time to live) is recording for all txos. This is used to cache transactions to save bandwidth.

In RunIBD, the proofs are read and a pollard, the accumulator, is built. Currently the pollard cannot be saved to disk but that is in the works.

For BuildProofs() proofs for every block is saved to proof.dat file. This file is indexed with proofoffset.dat file. For RunIBD() those two files are used, and the proofs are used to simulate a utreexo node syncing process. Important performance data about the sync process can be obtained & optimized.

example output:
Block 546000 add 854493089 del 804238642 pol nl 50254447 tops 18 he 6718011979 re 608922796 ow 2145511834
 plus 1164.54 total 25214.65

This simulation has reached block height 546000.  854493089 txos have been added, and 804238642 txos have been removed.  The pollard has 50254447 leaves (nl = NumLeaves).  (this should be the difference between adds and removes) There are 18 treetops, as 50254447 in binary has 18 1-bits.  6718011979 hashes have been performed through the sync process (this number is probably off) (he = hashes ever).  608922796 txos have been "remembered" or cached (re = remember).  2145511834 hashes have been sent over the wire (ow = over wire) during the sync simulation (or would have been sent over the wire since this is all on one machine).  With 32 byte hashes, that would be around 68GB of total proof data.
