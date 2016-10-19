package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.datatypes.DataType;
import edu.berkeley.cs186.database.io.Page;

import java.util.List;
import java.util.Iterator;


/**
 * A B+ tree inner node. An inner node header contains the page number of the
 * parent node (or -1 if no parent exists), and the page number of the first
 * child node (or -1 if no child exists). An inner node contains InnerEntry's.
 * Note that an inner node can have duplicate keys if a key spans multiple leaf
 * pages.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class InnerNode extends BPlusNode {

  public InnerNode(BPlusTree tree) {
    super(tree, false);
    getPage().writeByte(0, (byte) 0);
    setFirstChild(-1);
    setParent(-1);
  }
  
  public InnerNode(BPlusTree tree, int pageNum) {
    super(tree, pageNum, false);
    if (getPage().readByte(0) != (byte) 0) {
      throw new BPlusTreeException("Page is not Inner Node!");
    }
  }

  @Override
  public boolean isLeaf() {
    return false;
  }

  public int getFirstChild() {
    return getPage().readInt(5);
  }
  
  public void setFirstChild(int val) {
    getPage().writeInt(5, val);
  }

  /**
   * See BPlusNode#locateLeaf documentation.
   */
  @Override
  public LeafNode locateLeaf(DataType key, boolean findFirst) {
    //Recursively find until we reach a leaf node.
    BPlusNode node = null;
    Iterator<BEntry> iterator = getAllValidEntries().iterator();
    BEntry entry = null, lastEntry = null;
    int page_num = 0;

    while(iterator.hasNext()){
        entry = iterator.next();
        if(entry.getKey().compareTo(key) > 0){
            if(lastEntry != null){
                page_num = lastEntry.getPageNum();
            }else{
                page_num = this.getFirstChild();
            }
            break;
        }
        lastEntry = entry;
    }

    if(page_num > -1){
        Page page = getTree().allocator.fetchPage(page_num);
        if(page.readByte(0) == 0){
            //Inner Node
            return (new InnerNode(getTree(), page_num)).locateLeaf(key,
                    findFirst);
        }else{
            //Leaf Node
            return (new LeafNode(getTree(), page_num)).locateLeaf(key,
                    findFirst);
        }
    }else{
        return null;
    }
  }

  /**
   * Splits this node and pushes up the middle key. Note that we split this node
   * immediately after it becomes full rather than when trying to insert an
   * entry into a full node. Thus a full inner node of 2d entries will be split
   * into a left node with d entries and a right node with d-1 entries, with the
   * middle key pushed up.
   */
  @Override
  public void splitNode() {
      //Test whether this node is full or not.
      if(!hasSpace()){
          //Split the node
          InnerNode node = new InnerNode(getTree());
          byte[] bitMap = getBitMap();
          BEntry popEntry = null;
          for(int index = this.numEntries/2; index < this.numEntries; index++){
              int byteOffset = index / 8;
              int bitOffset = 7 - (index % 8);
              byte mask = (byte)(1 << bitOffset);
              bitMap[byteOffset] = (byte)(bitMap[byteOffset] | mask);
              if(index == this.numEntries/2){
                  //Pop entry
                  popEntry = readEntry(index); 
              }else{
                  //Transfer it to new node.
                  node.writeEntry(index - (this.numEntries/2 +1), 
                          readEntry(index));
              }
          }
          setBitMap(bitMap);
          BEntry entry = new InnerEntry(popEntry.getKey(),
                  node.getPageNum());
          node.setFirstChild(popEntry.getPageNum());
          if(isRoot()){
              //Create a new root
              InnerNode root = new InnerNode(getTree());
              root.writeEntry(0, entry);
              root.setParent(-1);
              root.setFirstChild(getPageNum());
              node.setParent(root.getPageNum());
              this.getTree().updateRoot(root.getPageNum());
          }else{
              //Just split current node
              node.setParent(getParent());
              node.setFirstChild(popEntry.getPageNum());
              //Find a place for the entry
              InnerNode parent = new InnerNode(getTree(), getPageNum());
              parent.insertBEntry(entry);
          }
      }
  }
}
