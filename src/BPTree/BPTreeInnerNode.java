package src.BPTree;


import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.Ref;

import java.io.Serializable;
import java.util.ArrayList;


/**
 * BPTreeInnerNode class represents an inner node in a B+ Tree.
 *
 * @param <T> The type of elements maintained by this tree, which must be comparable.
 */
public class BPTreeInnerNode<T extends Comparable<T>> extends BPTreeNode<T> implements Serializable {

    /**
     * Attributes
     * <p>
     * children ->  An array of leafs to hold the children of the inner node
     */
    private final String[] children;


    /**
     * Constructor
     * <p>
     * Creates an Inner Node for the B+ Tree with a given order.
     * Initializes the keys and children arrays.
     *
     * @param n The order of the B+ Tree, representing the maximum number of keys in the inner node.
     * @throws DBAppException If an error occurs during the initialization.
     */
    public BPTreeInnerNode(int n) throws DBAppException {
        super(n);
        setKeys(new Comparable[n]);
        children = new String[n + 1];
    }


    /**
     * Getters & Setters
     */
    public String[] getChildren() {
        return children;
    }

    /**
     * Gets the child node at the given index.
     *
     * @param index The index of the child node to be retrieved.
     * @return Either the deserialized child node at the given index, or null if the index is out of bounds.
     * @throws DBAppException If there's an issue deserializing the child node.
     */
    public BPTreeNode<T> getChild(int index) throws DBAppException {
        return children[index] == null ? null : deserializeNode(children[index]);
    }

    /**
     * Sets the child node at the specified index in this inner node.
     *
     * @param index The index where the child node should be set.
     * @param child The child node to set at the specified index.
     */
    public void setChild(int index, BPTreeNode<T> child) {
        children[index] = (child == null) ? null : child.getNodeName();
    }

    /**
     * Gets and deserializes the first child node of this inner node.
     *
     * @return The first child node of this inner node.
     * @throws DBAppException If there's an issue deserializing the first child node.
     */
    public BPTreeNode<T> getFirstChild() throws DBAppException {
        return deserializeNode(children[0]);
    }

    /**
     * Gets and deserializes the last child node of this inner node
     *
     * @return last child node of this inner node.
     * @throws DBAppException If there's an issue deserializing the last child node.
     */
    public BPTreeNode<T> getLastChild() throws DBAppException {
        return deserializeNode(children[getNumberOfKeys()]);
    }


    /**
     * Returns the minimum number of keys that a node must contain.
     *
     * @return The minimum number of keys for the node.
     */
    public int minKeys() {
        return this.isRoot() ? 1 : (getOrder() + 2) / 2 - 1;
    }


    /**
     * Inserts a key and its corresponding reference into the B+ Tree.
     * Handles splitting of nodes and updating parent nodes as necessary.
     *
     * @param key    The key to insert into the B+ Tree.
     * @param ref    The reference associated with the key.
     * @param parent The parent node of the current node.
     * @param ptr    The position of the current node in the parent's children.
     * @return A PushUpBPTree object containing the key and new node to be pushed up to the parent node,
     * or null if no push-up is needed.
     * @throws DBAppException If an error occurs during insertion.
     */
    public PushUpBPTree<T> insert(T key, Ref ref, BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        int index = findIndex(key);
        BPTreeNode<T> bpTreeNode = deserializeNode(children[index]);

        // Recursively insert the key and reference into the appropriate child node
        PushUpBPTree<T> pushUp = bpTreeNode.insert(key, ref, this, index);

        if (pushUp == null) {
            bpTreeNode.serializeNode();
            return null;
        }

        // If the current node is full, split it and push up the new key and node to the parent
        if (this.isFull()) {
            BPTreeInnerNode<T> newBpTreeNode = this.split(pushUp);
            Comparable<T> newKey = newBpTreeNode.getFirstKey();
            newBpTreeNode.deleteAt(0, 0);
            newBpTreeNode.serializeNode();
            bpTreeNode.serializeNode();

            return new PushUpBPTree<T>(newBpTreeNode, newKey);
        } else {
            // Insert the key and new node into the current node
            index = 0;
            while (index < getNumberOfKeys() && getKey(index).compareTo(key) < 0)
                ++index;
            this.insertRightAt(index, pushUp.key, pushUp.newNode);
            bpTreeNode.serializeNode();

            return null;
        }
    }


    /**
     * Inserts a key at the given index in the node.
     *
     * @param index The index at which to insert the key.
     * @param key   The key to insert.
     * @throws DBAppException If an error occurs during the insertion process.
     */
    private void insertAt(int index, Comparable<T> key) throws DBAppException {
        // Shift keys and children to the right from the end of the array to the index position
        for (int i = getNumberOfKeys(); i > index; --i) {
            this.setKey(i, this.getKey(i - 1));         // Shift the key at position i-1 to position i
            this.setChild(i + 1, this.getChild(i));     // Shift the child at position i to position i+1
        }
        this.setKey(index, key);
        setNumberOfKeys(getNumberOfKeys() + 1);
    }


    /**
     * Inserts a key and a left child node at the given index.
     *
     * @param index     The index at which to insert the key and left child.
     * @param key       The key to insert.
     * @param leftChild The left child node to insert.
     * @throws DBAppException If an error occurs during the insertion process.
     */
    public void insertLeftAt(int index, Comparable<T> key, BPTreeNode<T> leftChild) throws DBAppException {
        insertAt(index, key);
        // Shift the current child at index to the right
        this.setChild(index + 1, this.getChild(index));
        this.setChild(index, leftChild);
    }


    /**
     * Inserts a key and a right child node at the specified index.
     *
     * @param index      The index at which to insert the key and right child.
     * @param key        The key to insert.
     * @param rightChild The right child node to insert.
     * @throws DBAppException If an error occurs during the insertion process.
     */
    public void insertRightAt(int index, Comparable<T> key, BPTreeNode<T> rightChild) throws DBAppException {
        insertAt(index, key);
        this.setChild(index + 1, rightChild);
    }


    /**
     * Splits the current node into two nodes when it exceeds its maximum capacity.
     * Moves the keys and children appropriately to maintain the B+ Tree structure.
     *
     * @param pushup The key and new node to be pushed up to the parent node.
     * @return The newly created node after splitting.
     * @throws DBAppException If there's an error during the split operation.
     */
    public BPTreeInnerNode<T> split(PushUpBPTree<T> pushup) throws DBAppException {

        int keyIndex = this.findIndex((T) pushup.key);
        int midIndex = getNumberOfKeys() / 2 - 1;

        if (keyIndex > midIndex) {
            ++midIndex;
        }

        int totalKeys = getNumberOfKeys() + 1;

        // Create a new node for splitting then move keys and children to it
        BPTreeInnerNode<T> newNode = new BPTreeInnerNode<T>(getOrder());
        for (int i = midIndex; i < totalKeys - 1; ++i) {
            newNode.insertRightAt(i - midIndex, this.getKey(i), this.getChild(i + 1));

            setNumberOfKeys(getNumberOfKeys() - 1);
        }
        newNode.setChild(0, this.getChild(midIndex));

        // Insert new key and new node
        if (keyIndex < totalKeys / 2) {
            this.insertRightAt(keyIndex, pushup.key, pushup.newNode);
        } else {
            newNode.insertRightAt(keyIndex - midIndex, pushup.key, pushup.newNode);
        }

        return newNode;
    }


    /**
     * Finds the index where the given key should be inserted in the node.
     * If the key is greater than all keys in the node, returns the number of keys.
     *
     * @param key The key to find the insertion index for.
     * @return The index where the key should be inserted.
     */
    public int findIndex(T key) {
        for (int i = 0; i < getNumberOfKeys(); ++i) {
            int compareKeys = getKey(i).compareTo(key);
            if (compareKeys > 0)
                return i;  // Return the index where key should be inserted
        }
        return getNumberOfKeys();  // If key is greater than all keys, insert at the end
    }


    /**
     * Deletes the specified key from the B+ Tree, updating references and ensuring the tree remains balanced.
     *
     * @param key    The key to be deleted.
     * @param parent The parent node of the current node.
     * @param ptr    The pointer indicating the current node's position in the parent's children array.
     * @return true if the key was successfully deleted, false otherwise.
     * @throws DBAppException If an error occurs during the deletion process.
     */
    public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        boolean done = false;

        // Traverse through the keys to find the node containing the key
        for (int i = 0; !done && i < getNumberOfKeys(); ++i) {
            if (getKeys()[i].compareTo(key) > 0) {
                BPTreeNode<T> b = deserializeNode(children[i]);
                done = b.delete(key, this, i);
                b.serializeNode();
            }
        }

        // If not found in the above loop, check the last child
        if (!done) {
            BPTreeNode<T> bpTreeNode = deserializeNode(children[getNumberOfKeys()]);
            done = bpTreeNode.delete(key, this, getNumberOfKeys());
            bpTreeNode.serializeNode();
        }

        // If the number of keys falls below the minimum, handle underflow
        if (getNumberOfKeys() < this.minKeys()) {
            if (isRoot()) {
                BPTreeNode<T> firstChildNode = this.getFirstChild();
                firstChildNode.setRoot(true);
                firstChildNode.serializeNode();
                this.setRoot(false);
                return done;
            }

            // Attempt to borrow a key from a sibling node
            if (borrow(parent, ptr)) {
                return done;
            }

            // If borrowing fails, merge with a sibling node
            merge(parent, ptr);
        }

        return done;
    }


    /**
     * Deletes the specified key from the B+ Tree, updating references and ensuring the tree remains balanced.
     *
     * @param key      The key to be deleted.
     * @param parent   The parent node of the current node.
     * @param ptr      The pointer indicating the current node's position in the parent's children array.
     * @param pageName The name of the page associated with the key.
     * @return true if the key was successfully deleted, false otherwise.
     * @throws DBAppException If an error occurs during the deletion process.
     */
    public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr, String pageName) throws DBAppException {
        boolean done = false;

        // Traverse through the keys to find the node containing the key
        for (int i = 0; !done && i < getNumberOfKeys(); ++i) {
            if (getKeys()[i].compareTo(key) > 0) {
                BPTreeNode<T> bpTreeNode = deserializeNode(children[i]);
                done = bpTreeNode.delete(key, this, i, pageName);
                bpTreeNode.serializeNode();
            }
        }

        // If not found in the above loop, check the last child
        if (!done) {
            BPTreeNode<T> bpTreeNode = deserializeNode(children[getNumberOfKeys()]);
            done = bpTreeNode.delete(key, this, getNumberOfKeys(), pageName);
            bpTreeNode.serializeNode();
        }

        // If the number of keys falls below the minimum, handle underflow
        if (getNumberOfKeys() < this.minKeys()) {
            if (this.isRoot()) {
                BPTreeNode<T> firstChildNode = this.getFirstChild();
                firstChildNode.setRoot(true);
                firstChildNode.serializeNode();
                this.setRoot(false);
                return done;
            }
            if (borrow(parent, ptr)) {
                return done;
            }
            merge(parent, ptr);
        }
        return done;
    }


    /**
     * Deletes the key and child reference at the given index.
     * Shifts the subsequent keys and children to fill the gap.
     *
     * @param keyIndex The index of the key to delete.
     * @param ptr      Indicates whether to shift the children pointers (1) or not (0).
     */
    public void deleteAt(int keyIndex, int ptr) {

        // Shift keys and children to the left to fill the gap created by the deleted key and child
        for (int i = keyIndex; i < getNumberOfKeys() - 1; ++i) {
            getKeys()[i] = getKeys()[i + 1];
            children[i + ptr] = children[i + ptr + 1];
        }

        if (ptr == 0) {
            children[getNumberOfKeys() - 1] = children[getNumberOfKeys()];
        }
        setNumberOfKeys(getNumberOfKeys() - 1);
    }


    /**
     * Deletes the child reference at the given index.
     *
     * @param index The index of the child reference to delete.
     */
    public void deleteAt(int index) {
        deleteAt(index, 1);
    }


    /**
     * Searches for the given key in the B+ Tree and returns the associated reference.
     *
     * @param key The key to search for.
     * @return The GeneralRef object associated with the given key, or null if the key is not found.
     * @throws DBAppException If an error occurs during the search process.
     */
    @Override
    public GeneralRef search(T key) throws DBAppException {
        BPTreeNode<T> bpTreeNode = deserializeNode(children[findIndex(key)]);
        return bpTreeNode.search(key);
    }


    /**
     * Searches for the appropriate location to insert the given key and returns the corresponding reference.
     *
     * @param key         The key to search for.
     * @param tableLength The length of the table.
     * @return The reference corresponding to the key.
     * @throws DBAppException If an error occurs during the search process.
     */
    public Ref searchForInsertion(T key, int tableLength) throws DBAppException {
        BPTreeNode<T> bpTreeNode = deserializeNode(children[findIndex(key)]);
        return bpTreeNode.searchForInsertion(key, tableLength);
    }


    /**
     * Searches for all references with keys greater than or equal to the given key.
     *
     * @param key The key to search for.
     * @return An ArrayList of GeneralRef objects representing the references found.
     * @throws DBAppException If an error occurs during the search process.
     */
    public ArrayList<GeneralRef> searchMTE(T key) throws DBAppException {
        BPTreeNode<T> bpTreeNode = deserializeNode(children[findIndex(key)]);
        return bpTreeNode.searchMTE(key);
    }


    /**
     * Searches for all references with keys greater than the given key.
     *
     * @param key The key to search for.
     * @return An ArrayList of GeneralRef objects representing the references found.
     * @throws DBAppException If an error occurs during the search process.
     */
    public ArrayList<GeneralRef> searchMT(T key) throws DBAppException {
        BPTreeNode<T> b = deserializeNode(children[findIndex(key)]);
        return b.searchMT(key);
    }


    /**
     * Searches for the leaf node that should be updated for the given key.
     *
     * @param key The key to search for.
     * @return The BPTreeLeafNode that contains the reference to be updated.
     * @throws DBAppException If an error occurs during the search process.
     */
    public BPTreeLeafNode searchForUpdateRef(T key) throws DBAppException {
        BPTreeNode<T> b = deserializeNode(children[findIndex(key)]);
        return b.searchForUpdateRef(key);
    }


    /**
     * Attempts to borrow a key from a sibling node to maintain the minimum number of keys.
     * If borrowing from the left sibling is possible, it will borrow from the left;
     * otherwise, it will attempt to borrow from the right sibling.
     *
     * @param parent The parent node of the current node containing the keys and children.
     * @param ptr    The pointer indicating the current node's position in the parent's children array.
     * @return true if a key was successfully borrowed, false otherwise.
     * @throws DBAppException If an error occurs during the borrowing process.
     */
    public boolean borrow(BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        // Check left sibling
        if (ptr > 0) {
            BPTreeInnerNode<T> leftSibling = (BPTreeInnerNode<T>) parent.getChild(ptr - 1);
            if (leftSibling.getNumberOfKeys() > leftSibling.minKeys()) {
                BPTreeNode<T> leftSiblingLastChild = leftSibling.getLastChild();
                this.insertLeftAt(0, parent.getKey(ptr - 1), leftSiblingLastChild);
                leftSiblingLastChild.serializeNode();
                parent.deleteAt(ptr - 1);
                parent.insertRightAt(ptr - 1, leftSibling.getLastKey(), this);
                leftSibling.deleteAt(leftSibling.getNumberOfKeys() - 1);
                leftSibling.serializeNode();
                return true;
            }
        }

        // Check right sibling
        if (ptr < parent.getNumberOfKeys()) {
            BPTreeInnerNode<T> rightSibling = (BPTreeInnerNode<T>) parent.getChild(ptr + 1);
            if (rightSibling.getNumberOfKeys() > rightSibling.minKeys()) {
                BPTreeNode<T> rightSiblingFirstChild = rightSibling.getFirstChild();
                this.insertRightAt(this.getNumberOfKeys(), parent.getKey(ptr), rightSiblingFirstChild);
                rightSiblingFirstChild.serializeNode();
                parent.deleteAt(ptr);
                parent.insertRightAt(ptr, rightSibling.getFirstKey(), rightSibling);
                rightSibling.deleteAt(0, 0);
                rightSibling.serializeNode();
                return true;
            }
        }
        return false;
    }


    /**
     * Merges the current node with its sibling node.
     * If the pointer is greater than 0, merges with the left sibling;
     * otherwise, merges with the right sibling.
     *
     * @param parent The parent node containing the keys and children.
     * @param ptr    The pointer indicating the current node's position in the parent's children array.
     * @throws DBAppException If an error occurs during the merge process.
     */
    public void merge(BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        if (ptr > 0) {
            // Merge with left sibling
            BPTreeInnerNode<T> leftSibling = (BPTreeInnerNode<T>) parent.getChild(ptr - 1);
            leftSibling.merge(parent.getKey(ptr - 1), this);
            parent.deleteAt(ptr - 1);
            leftSibling.serializeNode();
        } else {
            // Merge with right sibling
            BPTreeInnerNode<T> rightSibling = (BPTreeInnerNode<T>) parent.getChild(ptr + 1);
            this.merge(parent.getKey(ptr), rightSibling);
            parent.deleteAt(ptr);
            rightSibling.serializeNode();
        }
    }


    /**
     * Merges the current node with the given foreign node by inserting the parent key and all keys and children from the foreign node.
     *
     * @param parentKey   The key from the parent node to be inserted.
     * @param foreignNode The foreign node to be merged into the current node.
     * @throws DBAppException If an error occurs during the merge process.
     */
    public void merge(Comparable<T> parentKey, BPTreeInnerNode<T> foreignNode) throws DBAppException {
        // Insert the parent key and the first child of the foreign node into the current node
        this.insertRightAt(getNumberOfKeys(), parentKey, foreignNode.getFirstChild());

        // Insert all keys and children from the foreign node into the current node
        for (int i = 0; i < foreignNode.getNumberOfKeys(); ++i) {
            this.insertRightAt(getNumberOfKeys(), foreignNode.getKey(i), foreignNode.getChild(i + 1));
        }
    }

}

