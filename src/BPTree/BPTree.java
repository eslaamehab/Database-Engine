package src.BPTree;

import src.DBGeneralEngine.TreeIndex;
import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.OverflowRef;
import src.Ref.Ref;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

//        import java.util.ArrayList;
//        import java.util.LinkedList;
//        import java.util.Queue;
//
//        import General.GeneralReference;
//        import General.OverflowReference;
//        import General.Ref;


public class BPTree<T extends Comparable<T>> implements Serializable, TreeIndex<T> {

/**
 *
 */
private final int order;
private BPTreeNode<T> root;
private int nextId;


    /*      Constructor     */
/**
 * Creates an empty B+ tree
 * @param order the maximum number of keys in the nodes of the tree
 * @throws DBAppException
 */
public BPTree(int order)throws DBAppException
        {
        this.order=order;
        root=new BPTreeLeafNode<T>(this.order);
        root.setRoot(true);
        }


        /*      Getters & Setters     */
        public int getNextId() {
                return nextId;
        }

        public void setNextId(int nextId) {
                this.nextId = nextId;
        }


public void updateRef(T key, Ref ref, Ref newRef) throws IOException, DBAppException {
    delete(key, ref);
    insert(key,newRef);
}
        public void updateRef(String oldPage,String newPage,T key)throws DBAppException{

                BPTreeLeafNode bpTreeLeafNode = searchForUpdateRef(key);
                bpTreeLeafNode.updateRef(oldPage, newPage, key);
                bpTreeLeafNode.serializeNode();
        }

/**
 * Inserts the specified key associated with the given record in the B+ tree
 * @param key the key to be inserted
 * @param ref the reference of the record associated with the key
 * @throws DBAppException
 */
public void insert(T key,Ref ref)throws DBAppException
        {
        PushUp<T> pushUp = root.insert(key,ref,null,-1);

        if(pushUp!=null)
        {
        BPTreeInnerNode<T> newRoot = new BPTreeInnerNode<T>(order);
        newRoot.insertLeftAt(0,pushUp.key,root);
        newRoot.setChild(1,pushUp.newNode);
        root.setRoot(false);
        root.serializeNode();
        root = newRoot;
        root.setRoot(true);
        }
        }

/**
 * Looks up for the record that is associated with the specified key
 *
 * @param key the key to find its record
 * @return the reference of the record associated with this key
 * @throws DBAppException
 */
public GeneralRef search(T key)throws DBAppException
        {
        return root.search(key);
        }

/**
 * Delete a key and its associated record from the tree.
 *
 * @param key the key to be deleted
 * @param ref
 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
 * @throws DBAppException
 */
public boolean delete(T key, Ref ref)throws DBAppException
        {
        boolean isDeleted = root.delete(key,null,-1);
        // go down and find the new root in case the old root is deleted
        while(root instanceof BPTreeInnerNode&&!root.isRoot())
        root = ((BPTreeInnerNode<T>)root).getFirstChild();
        return isDeleted;
        }

//
/**
 * Delete 1 Ref(either Ref/or single Ref inside an overflow page)
 * only not the key with its pointer
 *
 * @param key the key to be deleted
 * @return a boolean to indicate whether the key is successfully deleted or it
 * was not in the tree
 * @throws DBAppException
 */
public boolean delete(T key, String Page_name)throws DBAppException{
        boolean done=root.delete(key,null,-1,Page_name);
        // go down and find the new root in case the old root is deleted
        while(root instanceof BPTreeInnerNode&&!root.isRoot())
        root=((BPTreeInnerNode<T>)root).getFirstChild();
        return done;
        }

/**
 * Returns a string representation of the B+ tree.
 */
public String toString()
        {
        StringBuilder stringBuilder = new StringBuilder();
        BPTreeLeafNode.pagesToPrint = new ArrayList<OverflowRef>();
        Queue<BPTreeNode<T>> currentQueue = new LinkedList<BPTreeNode<T>>();
        Queue<BPTreeNode<T>> nextQueue = new LinkedList<BPTreeNode<T>>();
        currentQueue.add(root);

        while(!currentQueue.isEmpty())
        {

        next = new LinkedList<BPTreeNode<T>>();
        while(!currentQueue.isEmpty())
        {

        BPTreeNode<T> curentNode = currentQueue.remove();
        stringBuilder.append(curentNode);

        if(curentNode instanceof BPTreeLeafNode)
        {
            stringBuilder.append("->");
        }
        else
        {
        stringBuilder.append("{");
        BPTreeInnerNode<T> parent=(BPTreeInnerNode<T>)curNode;

        for(int i=0;i<=parent.numberOfKeys;++i)
        {

        stringBuilder.append(parent.getChild(i).index).append(",");
        nextQueue.add(parent.getChild(i));
        }

        stringBuilder.append("}");
        }

        }
        stringBuilder.append("\n");
        currentQueue = nextQueue;
        }

        ArrayList<OverflowRef> printRefs = BPTreeLeafNode.pagesToPrint;
        stringBuilder.append("\n Overflow refs are: ");

        for(int i=0;i<printRefs.size();i++)
        {
        stringBuilder.append("ref number: ").append(i + 1).append(" is : ");
        stringBuilder.append(printRefs.get(i).toString());
        }

        return stringBuilder.toString();
        }

public Ref searchForInsertion(T key, int tableLength) throws DBAppException{
        return root.searchForInsertion(key,tableLength);
        }

public BPTreeLeafNode getLeftmostLeaf()throws DBAppException{

        BPTreeNode<T> currentNode = root;
        while(!(currentNode instanceof BPTreeLeafNode)){
        BPTreeInnerNode<T> innerNode = (BPTreeInnerNode) currentNode;
        currentNode = innerNode.getFirstChild();
        }

        return(BPTreeLeafNode)currentNode;
        }
public ArrayList<GeneralRef> searchMTE(T key)throws DBAppException{
        return root.searchMTE(key);
        }
public ArrayList<GeneralRef> searchMT(T key)throws DBAppException{
        return root.searchMT(key);
        }

        }
