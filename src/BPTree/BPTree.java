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


/**
 * BPTree class implements a B+ Tree structure.
 *
 * @param <T> The type of elements maintained by this tree, which must be comparable.
 */
public class BPTree<T extends Comparable<T>> implements Serializable, TreeIndex<T> {


    /**
     * Attributes
     *
     * order -> The order of the B+ Tree, determining the maximum number of children each node can have.
     * root -> The root node of the B+ Tree.
     * nextId -> An identifier for the next node or leaf to be created, used to maintain unique IDs.
     */
    private final int order;
    private BPTreeNode<T> root;
    private int nextId;


    /**
     * Constructor
     *
     * Initializes an empty B+ Tree with the specified order.
     * Sets the root to a new B+ Tree leaf node and marks it as the root.
     *
     * @param order The order of the B+ Tree, determining the maximum number of children each node can have.
     * @throws DBAppException If an error occurs during the initialization of the B+ Tree.
     */
    public BPTree(int order) throws DBAppException {
        this.order = order;
        // Initialize the root as a new B+ Tree leaf node with the specified order
        root = new BPTreeLeafNode<T>(this.order);
        // Mark the root node as the root
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


    /**
     * Retrieves the leftmost leaf node in the B+ tree.
     *
     * @return The leftmost leaf node.
     * @throws DBAppException if an error occurs during the retrieval process.
     */
    public BPTreeLeafNode getLeftmostLeaf() throws DBAppException {
        BPTreeNode currentNode = root;
        while (!(currentNode instanceof BPTreeLeafNode)) {
            BPTreeInnerNode innerNode = (BPTreeInnerNode) currentNode;
            currentNode = innerNode.getFirstChild();
        }
        return (BPTreeLeafNode) currentNode;
    }

    /**
     * Updates the reference associated with a given key.
     * First, deletes the existing reference for the key, then inserts the new reference.
     *
     * @param key The key whose reference needs to be updated.
     * @param newRef The new reference to associate with the key.
     */
    public void updateRef(T key, Ref newRef) throws IOException, DBAppException {
        delete(key);
        insert(key, newRef);
    }

    /**
     * Updates the reference associated with a given key by replacing the old page reference with a new page reference.
     *
     * @param oldPage The old page reference to be replaced.
     * @param newPage The new page reference to associate with the key.
     * @param key The key whose reference needs to be updated.
     * @throws DBAppException If an error occurs during the update process.
     */
    public void updateRef(String oldPage, String newPage, T key) throws DBAppException {

        BPTreeLeafNode bpTreeLeafNode = searchForUpdateRef(key);
        bpTreeLeafNode.updateRef(oldPage, newPage, key);
        bpTreeLeafNode.serializeNode();
    }

    /**
     * Searches for the B+ tree leaf node that contains the reference to be updated for the given key.
     *
     * @param key The key whose reference needs to be updated.
     * @return The BPTreeLeafNode that contains the reference for the given key.
     * @throws DBAppException If an error occurs during the search process.
     */
    public BPTreeLeafNode searchForUpdateRef(T key) throws DBAppException {
        return root.searchForUpdateRef(key);
    }

    /**
     * Inserts a key and its associated reference into the B+ Tree.
     * If the root node splits, a new root node is created.
     *
     * @param key The key to be inserted.
     * @param ref The reference associated with the key.
     * @throws DBAppException If an error occurs during the insertion process.
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
     * Searches for the given key in the B+ Tree.
     *
     * @param key The key to search for.
     * @return The reference associated with the given key, or null if the key is not found.
     * @throws DBAppException If an error occurs during the search process.
     */
    public GeneralRef search(T key) throws DBAppException {
        return root.search(key);
    }

    /**
     * Searches for the minimum to the equal key (MTE) references associated with the specified key.
     *
     * @param key The key to search for.
     * @return An ArrayList containing the MTE references associated with the key.
     * @throws DBAppException if an error occurs during the search process.
     */
    public ArrayList<GeneralRef> searchMTE(T key) throws DBAppException {
        return root.searchMTE(key);
    }

    /**
     * Searches for the minimum reference associated with a key that is greater than or equal to the specified key.
     *
     * @param key The key to search for.
     * @return An ArrayList of GeneralRef objects representing the minimum references found.
     * @throws DBAppException if an error occurs during the search process.
     */
    public ArrayList<GeneralRef> searchMT(T key) throws DBAppException {
        return root.searchMT(key);
    }

    /**
     * Deletes the entry with the given key from the B+ Tree.
     *
     * @param key The key to be deleted.
     * @return true if the entry was successfully deleted, false otherwise.
     * @throws DBAppException If an error occurs during the deletion process.
     */
    @Override
    public boolean delete(T key) throws DBAppException {
        boolean isDeleted = root.delete(key, null, -1);

        // If the root node is an inner node and is not the root of the tree, traverse to the first child to update the root.
        while (root instanceof BPTreeInnerNode && !root.isRoot()) {
            root = ((BPTreeInnerNode<T>) root).getFirstChild();
        }
        return isDeleted;
    }

    /**
     * Deletes the entry with the given key from the B+ Tree and updates the corresponding PageName.
     *
     * @param key   The key to be deleted.
     * @param PageName The name of the page where the entry should be updated.
     * @return true if the entry was successfully deleted and the PageName was updated, false otherwise.
     * @throws DBAppException If an error occurs during the deletion process.
     */
    public boolean delete(T key, String PageName) throws DBAppException {
        boolean done = root.delete(key, null, -1, PageName);

        // If the root node is an inner node and is not the root of the tree, traverse to the first child to update the root.
        while ((root instanceof BPTreeInnerNode) && !root.isRoot()) {
            root = ((BPTreeInnerNode<T>) root).getFirstChild();
        }
        return done;
    }

    /**
     * Searches for the appropriate location to insert the given key and returns the corresponding reference.
     *
     * @param key          The key to be inserted.
     * @param tableLength  The length of the table.
     * @return             The reference corresponding to the key.
     * @throws DBAppException if an error occurs during the search operation.
     */
    public Ref searchForInsertion(T key, int tableLength) throws DBAppException {
        return root.searchForInsertion(key, tableLength);
    }


    /**
     * This method is primarily for debugging and tree visualization.
     *
     * @return A string representation of the B+ Tree including its nodes and overflow references..
     */
    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder();

        // Initialize a list to hold overflow references from leaf nodes
        BPTreeLeafNode.pagesToPrint = new ArrayList<>();

        Queue<BPTreeNode<T>> currentQueue = new LinkedList<>();
        Queue<BPTreeNode<T>> nextQueue;

        currentQueue.add(root);

        // Breadth-first traversal
        while (!currentQueue.isEmpty()) {
            // Initialize the next queue for the next level of traversal
            nextQueue = new LinkedList<>();

            while (!currentQueue.isEmpty()) {
                BPTreeNode<T> currentNode = currentQueue.remove();
                stringBuilder.append(currentNode);

                // If the current node is a leaf node, indicate the end of the leaf node
                if (currentNode instanceof BPTreeLeafNode) {
                    stringBuilder.append("->");
                }
                else {
                    // If the current node is an inner node, indicate its children
                    stringBuilder.append("{");
                    BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) currentNode;

                    // Process each child of the inner node
                    for (int i = 0; i <= parent.getNumberOfKeys(); ++i) {
                        try {
                            // Append the index of the child node
                            stringBuilder.append(parent.getChild(i).getIndex()).append(",");
                            // Add the child node to the next queue for traversal
                            nextQueue.add(parent.getChild(i));
                        } catch (DBAppException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    stringBuilder.append("}");
                }
            }

            // New line to separate levels in the tree and move to the next level of traversal
            stringBuilder.append("\n");
            currentQueue = nextQueue;
        }

        // Append overflow references to the string representation
        ArrayList<OverflowRef> overflowRefs = BPTreeLeafNode.pagesToPrint;
        stringBuilder.append("\n Overflow refs are: ");
        for (int i = 0; i < overflowRefs.size(); i++) {
            stringBuilder.append("Ref number: ").append(i + 1).append(" is : ");
            stringBuilder.append(overflowRefs.get(i).toString());
        }

        return stringBuilder.toString();
    }

}
