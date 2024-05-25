package src.BPTree;

import src.DBGeneralEngine.TreeIndex;
import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.OverflowRef;
import src.Ref.Ref;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;


public class BPTree<T extends Comparable<T>> implements Serializable, TreeIndex<T> {

    /**
     * Attributes
     */
    private final int order;
    private BPTreeNode<T> root;
    private int nextId;


    /**
     * Constructor
     * Creates an empty B+ tree
     *
     * @param order the maximum number of keys in the nodes of the tree
     */
    public BPTree(int order) throws DBAppException {
        this.order = order;
        root = new BPTreeLeafNode<T>(this.order);
        root.setRoot(true);
    }


    /**
     * Getters & Setters
     *
     */
    public int getNextId() {
        return nextId;
    }

    public void setNextId(int nextId) {
        this.nextId = nextId;
    }

    public int getOrder() {
        return order;
    }

    public BPTreeNode<T> getRoot() {
        return root;
    }

    public void setRoot(BPTreeNode<T> root) {
        this.root = root;
    }


    public void updateRef(T key, Ref newRef) throws IOException, DBAppException {
        delete(key);
        insert(key, newRef);
    }

    public void updateRef(String oldPage, String newPage, T key) throws DBAppException {

        BPTreeLeafNode bpTreeLeafNode = searchForUpdateRef(key);
        bpTreeLeafNode.updateRef(oldPage, newPage, key);
        bpTreeLeafNode.serializeNode();
    }

    public BPTreeLeafNode searchForUpdateRef(T key) throws DBAppException {
        return root.searchForUpdateRef(key);
    }


    /**
     * Inserts the key associated with the given record in the B+ tree
     *
     * @param key the key to be inserted
     * @param ref the reference of the record associated with the key
     */
    public void insert(T key, Ref ref) throws DBAppException {
        PushUpBPTree<T> pushUp = root.insert(key, ref, null, -1);

        if (pushUp != null) {
            BPTreeInnerNode<T> newRoot = new BPTreeInnerNode<T>(order);
            newRoot.insertLeftAt(0, pushUp.key, root);
            newRoot.setChild(1, pushUp.newNode);
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
     */
    public GeneralRef search(T key) throws DBAppException {
        return root.search(key);
    }


    /**
     * Delete a key and its associated record from the tree.
     *
     * @param key the key to be deleted
     * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
     */
    @Override
    public boolean delete(T key) throws DBAppException {
        boolean isDeleted = root.delete(key, null, -1);

        while (root instanceof BPTreeInnerNode && !root.isRoot()) {
            root = ((BPTreeInnerNode<T>) root).getFirstChild();
        }
        return isDeleted;
    }


    /**
     * Delete 1 Ref(either Ref/or single Ref inside an overflow page)
     * only not the key with its pointer
     *
     * @param key the key to be deleted
     * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
     */
    public boolean delete(T key, String PageName) throws DBAppException {
        boolean done = root.delete(key, null, -1, PageName);

        while ((root instanceof BPTreeInnerNode) && !root.isRoot()) {
            root = ((BPTreeInnerNode<T>) root).getFirstChild();
        }
        return done;
    }


    /**
     * Returns a string representation of the B+ tree.
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        BPTreeLeafNode.pagesToPrint = new ArrayList<>();

        Queue<BPTreeNode<T>> currentQueue = new LinkedList<>();
        Queue<BPTreeNode<T>> nextQueue;

        currentQueue.add(root);

        while (!currentQueue.isEmpty()) {

            nextQueue = new LinkedList<>();
            while (!currentQueue.isEmpty()) {

                BPTreeNode<T> currentNode = currentQueue.remove();
                stringBuilder.append(currentNode);

                if (currentNode instanceof BPTreeLeafNode) {
                    stringBuilder.append("->");
                } else {
                    stringBuilder.append("{");
                    BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) currentNode;

                    for (int i = 0; i <= parent.getNumberOfKeys(); ++i) {

                        try {
                            stringBuilder.append(parent.getChild(i).getIndex()).append(",");
                        } catch (DBAppException e) {
                            throw new RuntimeException(e);
                        }
                        try {
                            nextQueue.add(parent.getChild(i));
                        } catch (DBAppException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    stringBuilder.append("}");
                }

            }
            stringBuilder.append("\n");
            currentQueue = nextQueue;
        }

        ArrayList<OverflowRef> overflowRefs = BPTreeLeafNode.pagesToPrint;
        stringBuilder.append("\n Overflow refs are: ");

        for (int i = 0; i < overflowRefs.size(); i++) {
            stringBuilder.append("Ref number: ").append(i + 1).append(" is : ");
            stringBuilder.append(overflowRefs.get(i).toString());
        }

        return stringBuilder.toString();
    }


    public Ref searchForInsertion(T key, int tableLength) throws DBAppException {
        return root.searchForInsertion(key, tableLength);
    }

    public BPTreeLeafNode getLeftmostLeaf() throws DBAppException {

        BPTreeNode currentNode = root;
        while (!(currentNode instanceof BPTreeLeafNode)) {
            BPTreeInnerNode innerNode = (BPTreeInnerNode) currentNode;
            currentNode = innerNode.getFirstChild();
        }

        return (BPTreeLeafNode) currentNode;
    }

    public ArrayList<GeneralRef> searchMTE(T key) throws DBAppException {
        return root.searchMTE(key);
    }

    public ArrayList<GeneralRef> searchMT(T key) throws DBAppException {
        return root.searchMT(key);
    }

}
