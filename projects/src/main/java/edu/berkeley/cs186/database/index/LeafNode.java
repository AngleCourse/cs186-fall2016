package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * A B+ tree leaf node. A leaf node header contains the page number of the
 * parent node (or -1 if no parent exists), the page number of the previous leaf
 * node (or -1 if no previous leaf exists), and the page number of the next leaf
 * node (or -1 if no next leaf exists). A leaf node contains LeafEntry's.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class LeafNode extends BPlusNode {

  public LeafNode(BPlusTree tree) {
    super(tree, true);
    getPage().writeByte(0, (byte) 1);
    setPrevLeaf(-1);
    setParent(-1);
    setNextLeaf(-1);
  }
  
  public LeafNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, true);
    if (getPage().readByte(0) != (byte) 1) {
      throw new BPlusTreeException("Page is not Leaf Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return true;
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
      LeafNode leaf = this, lastLeaf = null;

      //System.out.println("Start searching...");
      int index = 0;
      lastLeaf = leaf;
      while(lastLeaf.getNextLeaf() > -1){
          index++;
          lastLeaf = (LeafNode)BPlusNode.getBPlusNode(
                      getTree(), lastLeaf.getNextLeaf());
      }
      //System.out.println("Currently we are " + index +
      //    " away from the end, FindFirst is: " + Boolean.toString(findFirst));

      int slot = findLastKey(key);
      if(!findFirst){
        //System.out.println("Starting scan for last key: " + key.toString());
        if(slot == this.numEntries-1){
            //keep searching
             while(leaf.getNextLeaf()>-1){
                 lastLeaf = leaf;
                 leaf = (LeafNode) BPlusNode.getBPlusNode(getTree(), leaf.getNextLeaf());
                 slot = leaf.findLastKey(key);
                 if(slot < 0){
                     return lastLeaf;
                 }else if(slot >= 0 && slot < this.numEntries){
                     return leaf;
                 }
             }
        }
      }

      //System.out.println("After searching");
      index = 0;
      lastLeaf = leaf;
      while(lastLeaf.getNextLeaf() > -1){
          index++;
          lastLeaf = (LeafNode)BPlusNode.getBPlusNode(
                      getTree(), lastLeaf.getNextLeaf());
      }
      //System.out.println("Currently we are " + index +
      //    " away from the end");

      //Nerver found or found only on this leaf
      return leaf;
  }
  /**
   * Find the last position of the specified key on
   * this leaf node.
   */
  private int findLastKey(DataType key){
    byte[] bitMap = this.getBitMap();
    int found = -1;
    for (int i = 0; i < this.numEntries; i++) {
      int byteOffset = i/8;
      int bitOffset = 7 - (i % 8);
      byte mask = (byte) (1 << bitOffset);
      
      byte value = (byte) (bitMap[byteOffset] & mask);
      
      if (value != 0) {
          BEntry entry = readEntry(i);
          if(entry.getKey().compareTo(key) == 0){
              found = i;
          }else if(found > -1){
              //Found before
              return found;
          }
      }
    }
    return found;
  }

  /**
   * Splits this node and copies up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full leaf node of 2d entries will be split
   * into a left node with d entries and a right node with d entries, with the
   * leftmost key of the right node copied up.
   */
  @Override
  public void splitNode() {
      if(!hasSpace()){
          LeafNode leaf = new LeafNode(getTree());
          Iterator<BEntry> iterator = getAllValidEntries().iterator();
          ArrayList<BEntry> entries = new ArrayList<BEntry>();
          ArrayList<BEntry> new_entries = new ArrayList<BEntry>();
          for(int index = 0; index < this.numEntries; index++){
              if(index < this.numEntries/2){
                  //Put it in the original node
                  entries.add(iterator.next());
              }else{
                  new_entries.add(iterator.next());
              }
          }
          overwriteBNodeEntries(entries);
          leaf.overwriteBNodeEntries(new_entries);
          InnerEntry entry = new InnerEntry(new_entries.get(0).getKey(), leaf.getPageNum());

          leaf.setPrevLeaf(getPageNum());
          leaf.setNextLeaf(getNextLeaf());
          
          setNextLeaf(leaf.getPageNum());

          //Find a place to hold entry in the parent node
          if(getParent() > -1){
              BPlusNode parent = BPlusNode.getBPlusNode(getTree(), getParent());
              parent.insertBEntry(entry);
              leaf.setParent(getParent());
          }else{
              //root node
              InnerNode parent = new InnerNode(getTree());
              parent.setParent(-1);
              parent.setFirstChild(getPageNum());
              getTree().updateRoot(parent.getPageNum());
              parent.insertBEntry(entry);

              setParent(parent.getPageNum());
              leaf.setParent(parent.getPageNum());
          }
          
      }
  }
  
  public int getPrevLeaf() {
    return getPage().readInt(5);
  }

  public int getNextLeaf() {
    return getPage().readInt(9);
  }
  
  public void setPrevLeaf(int val) {
    getPage().writeInt(5, val);
  }

  public void setNextLeaf(int val) {
    getPage().writeInt(9, val);
  }

  /**
   * Creates an iterator of RecordID's for all entries in this node.
   *
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scan() {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      rids.add(le.getRecordID());
    }

    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's whose keys are greater than or equal to
   * the given start value key.
   *
   * @param startValue the start value key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanFrom(DataType startValue) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (startValue.compareTo(le.getKey()) < 1) { 
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }

  /**
   * Creates an iterator of RecordID's that correspond to the given key.
   *
   * @param key the search key
   * @return an iterator of RecordID's
   */
  public Iterator<RecordID> scanForKey(DataType key) {
    List<BEntry> validEntries = getAllValidEntries();
    List<RecordID> rids = new ArrayList<RecordID>();

    for (BEntry le : validEntries) {
      if (key.compareTo(le.getKey()) == 0) { 
        rids.add(le.getRecordID());
      }
    }
    return rids.iterator();
  }
}
