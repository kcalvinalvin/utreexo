CowForest
===================

Utreexo on disk is formatted as a copy-on-write style structure. I like to call it CowForest.

## Structure

A CowForest consists of the following:

1. TreeBlocks
2. TreeTables
3. Manifest

TreeBlocks are the smallest structure of a CowForest.

TreeTables are multitude of TreeBlocks that are grouped.

Manifest holds all the metadata needed for reading a CowForest.

### TreeBlock
   <beginning of file>
   [treeBlock #1]
   [treeBlock #2]
   ...
   ...
   ...
   [treeBlock #n]
   <end of file>

Each treeBlock is a fixed size utreexo tree with height n that holds a total of
2**(n+1) - 1 nodes. This tree can hold 2**n leaves.
A treeBlock may represent any height of a utreexo forest. For example:

   30
   |-------------------------------\
   28                              29
   |---------------\               |---------------\
   24              25              26              27
   |-------\       |-------\       |-------\       |-------\
   16      17      18      19      20      21      22      23
   |---\   |---\   |---\   |---\   |---\   |---\   |---\   |---\
   00  01  02  03  04  05  06  07  08  09  10  11  12  13  14  15

If treeBlocks are organized into height 3 trees, then this forest would have 3 treeBlocks which are:

   28
   |---------------\
   24              25
   |-------\       |-------\
   16      17      18      19
   |---\   |---\   |---\   |---\
   00  01  02  03  04  05  06  07


   29
   |---------------\
   26              27
   |-------\       |-------\
   20      21      22      23
   |---\   |---\   |---\   |---\
   08  09  10  11  12  13  14  15

   em
   |---------------\
   em              em
   |-------\       |-------\
   em      em      em      em
   |---\   |---\   |---\   |---\
   30  em  em  em  em  em  em  em

A treeBlock is accessible by its position as well.

### TreeTable

TreeBlocks are grouped into TreeTables and accessed as such. There are some rules for TreeTables.

1. They must only have the same height TreeBlock

TreeTables are held on disk before being written with a FIFO (first-in-first-out) caching policy.
This is the most efficient way of caching with Bitcoin as a single UTXO will never be
accessed twice.

And as stated in section 5.7 of the Utreexo paper, a UTXO is going to
be less and less likely to be spent as it gets older. Bélády's anomaly unfortunately
doesn't apply in this case. f

When written, each TreeTable is essentially immutable. They are never overwritten.

If a TreeTable is to be modified, a new file is written to disk to represent that TreeTable.
In the Manifest, the old TreeTable file is marked as stale. Stale TreeTables are
garbage collected every so often.

### Manifest

Manifest holds all neccessary metadata for a utreexo forest. This includes
