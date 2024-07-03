package src.RTree;

import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.Ref;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * The RTreeInnerNode class represents an internal node in the R-Tree.
 * It is responsible for managing the child nodes of the R-Tree.
 * Inner nodes store the minimum bounding rectangles (MBRs) of their child nodes and pointers to those child nodes.
 * They are used to guide the search process through the R-Tree hierarchy.
 *
 * @param <CustomPolygon> the type of custom polygon objects stored in the R-Tree
 */
public class RTreeInnerNode<CustomPolygon extends Comparable<CustomPolygon>> extends RTreeNode<CustomPolygon> implements Serializable {

    /**
     * Attributes
     * <p>
     * children     -> An array of child node indices, which acts as a lookup table to access them.
     */
    private final String[] children;


    /**
     * Constructor
     * Creates a new RTreeInnerNode with the specified maximum number of keys.
     *
     * @param maxKeys the maximum number of keys (and child nodes) that can be stored in this inner node
     * @throws DBAppException if there is an issue creating the inner node
     */
    @SuppressWarnings("unchecked")
    public RTreeInnerNode(int maxKeys) throws DBAppException {
        super(maxKeys);
        setKeys(new Comparable[maxKeys]);
        children = new String[maxKeys + 1];
    }


    /**
     * Getters & Setters
     * <p>
     * <p>
     * Gets an array of child node indices stored in the RTreeInnerNode
     *
     * @return array of child node indices
     */
    public String[] getChildren() {
        return children;
    }

    /**
     * Gets the child node at the given index
     *
     * @param index the index of the child node to retrieve
     * @return child node at the given index, or null if not found
     * @throws DBAppException if there is an issue deserializing the child node
     */
    public RTreeNode<CustomPolygon> getChild(int index) throws DBAppException {
        return (children[index] == null) ? null : deserializeNode(children[index]);
    }

    /**
     * Retrieves the first child node of this RTreeInnerNode.
     *
     * @return the first child node
     * @throws DBAppException if there is an issue deserializing the child node
     */
    public RTreeNode<CustomPolygon> getFirstChild() throws DBAppException {
        return deserializeNode(children[0]);
    }

    /**
     * Retrieves the last child node of this RTreeInnerNode.
     *
     * @return the last child node
     * @throws DBAppException if there is an issue deserializing the child node
     */
    public RTreeNode<CustomPolygon> getLastChild() throws DBAppException {
        return deserializeNode(children[getNumberOfKeys()]);
    }

    /**
     * Sets the child node at the given index.
     *
     * @param index the index of the child node to set
     * @param child the child node to set, or null to remove the child
     */
    public void setChild(int index, RTreeNode<CustomPolygon> child) {
        children[index] = (child == null) ? null : child.getNodeName();
    }

    /**
     * Retrieves the minimum number of keys (child nodes) the RTreeInnerNode can have.
     *
     * @return the minimum number of keys for this RTreeInnerNode
     */
    public int getMinKeys() {
        return this.isRoot() ? 1 : (getOrder() + 2) / 2 - 1;
    }


    /**
     * Finds ( using binary search ) the index where the given key should be inserted in this RTreeInnerNode.
     *
     * @param key the key to find the index for
     * @return the index where the given key should be inserted
     */
    public int findIndex(CustomPolygon key) {
        for (int i = 0; i < getNumberOfKeys(); ++i) {
            int cmp = getKey(i).compareTo(key);
            if (cmp > 0)
                return i;
        }
        // If no key is greater than the given key, return the index after the last key
        return getNumberOfKeys();
    }


    /**
     * Inserts a new key and reference into the R-Tree node.
     *
     * @param key    key to be inserted
     * @param ref    reference where this inserted key is located
     * @param parent parent of this inserted node
     * @param ptr    index of pointer in the parent node pointing to this node
     * @return a PushUpRTree object containing the new key and node to be pushed up to the parent,
     * or null if the insertion was successful without needing to push up
     * @throws DBAppException if there is an issue deserializing or serializing the child nodes
     */
    public PushUpRTree<CustomPolygon> insert(CustomPolygon key, Ref ref, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        int index = findIndex(key);

        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[index]);
        PushUpRTree<CustomPolygon> pushUpRTree = rTreeNode.insert(key, ref, this, index);

        if (pushUpRTree == null) {
            rTreeNode.serializeNode();
            return null;
        }

        if (this.isFull()) {
            RTreeInnerNode<CustomPolygon> newNode = this.split(pushUpRTree);
            Comparable<CustomPolygon> newKey = newNode.getFirstKey();
            newNode.deleteAt(0, 0);
            newNode.serializeNode();
            rTreeNode.serializeNode();
            return new PushUpRTree<CustomPolygon>(newNode, newKey);
        } else {
            index = 0;
            while (index < getNumberOfKeys() && getKey(index).compareTo(key) < 0)
                ++index;
            this.insertRightAt(index, pushUpRTree.key, pushUpRTree.newNode);
            rTreeNode.serializeNode();
            return null;
        }

    }


    /**
     * Inserts the given key at the specified index in this RTreeInnerNode.
     * <p>
     * This method shifts all keys and child pointers after the given index to
     * make room for the new key. It then sets the new key at the specified
     * index and increments the number of keys in the node.
     *
     * @param index the index at which to insert the key
     * @param key   the key to be inserted
     * @throws DBAppException if an error occurs during the insertion
     */
    private void insertAt(int index, Comparable<CustomPolygon> key) throws DBAppException {
        // Shift all keys and child pointers after the given index to make room
        for (int i = getNumberOfKeys(); i > index; --i) {
            this.setKey(i, this.getKey(i - 1));
            this.setChild(i + 1, this.getChild(i));
        }

        // Set the new key at the specified index
        this.setKey(index, key);

        // Increment the number of keys in the node
        setNumberOfKeys(getNumberOfKeys() + 1);
    }


    /**
     * Inserts the given key at the given index and adjusts the left child pointer.
     * Where it sets the child pointer to the left of the inserted key to the given left child.
     *
     * @param index     the index where the key is inserted
     * @param key       the key to be inserted in that index
     * @param leftChild the child node to be set as the left child of the inserted key
     * @throws DBAppException if an error occurs during the insertion
     */
    public void insertLeftAt(int index, Comparable<CustomPolygon> key, RTreeNode<CustomPolygon> leftChild) throws DBAppException {
        insertAt(index, key);
        this.setChild(index + 1, this.getChild(index));
        this.setChild(index, leftChild);
    }


    /**
     * Inserts the given key at the given index and adjusts the right child pointer.
     * Where it sets the child pointer to the right of the inserted key to the given right child.
     *
     * @param index      the index where the key is inserted
     * @param key        the key to be inserted in that index
     * @param rightChild the child node to be set as the right child of the inserted key
     * @throws DBAppException if an error occurs during the insertion
     */
    public void insertRightAt(int index, Comparable<CustomPolygon> key, RTreeNode<CustomPolygon> rightChild) throws DBAppException {
        insertAt(index, key);
        this.setChild(index + 1, rightChild);
    }


    /**
     * Searches for the record reference of the given key.
     *
     * @param key the key to search for
     * @return the record reference for the given key, if found
     * @throws DBAppException if an error occurs during the search
     */
    @Override
    public GeneralRef search(CustomPolygon key) throws DBAppException {
        // Find the index of the child node that may contain the given key
        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[findIndex(key)]);

        // Search for the record reference in the child node
        return rTreeNode.search(key);
    }


    /**
     * Searches for all record references that intersect the given key.
     * "MTE" in the method name stands for "Match Totally or Envelope".
     * Indicating searching for record references that either
     * Exactly match the given key or are enveloped by the given key.
     *
     * @param key the key to search for
     * @return list of all record references that intersect the given key
     * @throws DBAppException if an error occurs during the search
     */
    public ArrayList<GeneralRef> searchMTE(CustomPolygon key) throws DBAppException {
        // Find the index of the child node that may contain the given key
        RTreeNode<CustomPolygon> b = deserializeNode(children[findIndex(key)]);

        // Search for all matching record references in the child node
        return b.searchMTE(key);
    }


    /**
     * Searches for all record references that match the given key.
     * "MT" in the method name stands for "Match Totally"
     * Indicating searching for exact matches of the given key.
     *
     * @param key the key to search for
     * @return list of all record references that match the given key
     * @throws DBAppException if an error occurs during the search
     */
    public ArrayList<GeneralRef> searchMT(CustomPolygon key) throws DBAppException {
        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[findIndex(key)]);
        return rTreeNode.searchMT(key);
    }


    /**
     * Searches for the leaf node that contains the record reference for the given key.
     *
     * @param key the key to search for
     * @return the leaf node that contains the record reference for the given key
     * @throws DBAppException if an error occurs during the search
     */
    public RTreeLeafNode searchForUpdateRef(CustomPolygon key) throws DBAppException {
        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[findIndex(key)]);
        return rTreeNode.searchForUpdateRef(key);
    }


    /**
     * Searches for the appropriate leaf node to insert the given key.
     *
     * @param key         the key to search for
     * @param tableLength the length of the table
     * @return the leaf node to insert the given key
     * @throws DBAppException if an error occurs during the search
     */
    public Ref searchForInsertion(CustomPolygon key, int tableLength) throws DBAppException {
        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[findIndex(key)]);
        return rTreeNode.searchForInsertion(key, tableLength);
    }


    /**
     * Splits the inner node by moving half of the keys and child pointers to a new node.
     * <p>
     * <p>
     * This method is needed when an inner node becomes full ( number of keys reach the maximum order of the R-Tree ).
     * It creates a new inner node and redistributes the keys and child pointers between the original node and the new node.
     *
     * @param pushUpRTree object containing both the key and the new child node to be pushed up to the parent node
     * @return the new inner node created after the split
     * @throws DBAppException if an error occurs during the split operation
     */
    @SuppressWarnings("unchecked")
    public RTreeInnerNode<CustomPolygon> split(PushUpRTree<CustomPolygon> pushUpRTree) throws DBAppException {
        int keyIndex = this.findIndex((CustomPolygon) pushUpRTree.key);
        int midIndex = getNumberOfKeys() / 2 - 1;

        // Split nodes
        if (keyIndex > midIndex)
            ++midIndex;

        int totalKeys = getNumberOfKeys() + 1;

        RTreeInnerNode<CustomPolygon> newNode = new RTreeInnerNode<>(getOrder());
        for (int i = midIndex; i < totalKeys - 1; ++i) {
            newNode.insertRightAt(i - midIndex, this.getKey(i), this.getChild(i + 1));
            setNumberOfKeys(getNumberOfKeys() - 1);
        }
        newNode.setChild(0, this.getChild(midIndex));

        // Add new key
        if (keyIndex < totalKeys / 2)
            this.insertRightAt(keyIndex, pushUpRTree.key, pushUpRTree.newNode);
        else
            newNode.insertRightAt(keyIndex - midIndex, pushUpRTree.key, pushUpRTree.newNode);

        return newNode;
    }


    /**
     * Borrows a key and child pointer either from left or right sibling node.
     * <p>
     * <p>
     * This method is needed to balance the number of keys and child pointers between the current node and its left or right sibling.
     * If one of the siblings has more keys than the minimum required,
     * It will borrow a key and child pointer from that sibling and update the parent node accordingly.
     *
     * @param parent the parent node of the current node
     * @param ptr    the index of the pointer in the parent node that points to the current node
     * @return true if a key and child pointer were successfully borrowed from either sibling, false otherwise
     * @throws DBAppException if an error occurs during the borrowing operation
     */
    public boolean borrow(RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        //check left sibling
        if (ptr > 0) {
            RTreeInnerNode<CustomPolygon> leftSibling = (RTreeInnerNode<CustomPolygon>) parent.getChild(ptr - 1);
            if (leftSibling.getNumberOfKeys() > leftSibling.getMinKeys()) {
                RTreeNode<CustomPolygon> leftSiblingLastChild = leftSibling.getLastChild();
                this.insertLeftAt(0, parent.getKey(ptr - 1), leftSiblingLastChild);
                leftSiblingLastChild.serializeNode();
                parent.deleteAt(ptr - 1);
                parent.insertRightAt(ptr - 1, leftSibling.getLastKey(), this);
                leftSibling.deleteAt(leftSibling.getNumberOfKeys() - 1);
                leftSibling.serializeNode();
                return true;
            }
        }

        //check right sibling
        if (ptr < parent.getNumberOfKeys()) {
            RTreeInnerNode<CustomPolygon> rightSibling = (RTreeInnerNode<CustomPolygon>) parent.getChild(ptr + 1);
            if (rightSibling.getNumberOfKeys() > rightSibling.getMinKeys()) {
                RTreeNode<CustomPolygon> rightSiblingFirstChild = rightSibling.getFirstChild();
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
     * Merges the current node with either the left or right sibling node.
     * <p>
     * <p>
     * This method is needed when the current node has fewer keys than the minimum required.
     * It will either merge the current node with the left sibling or the right sibling,
     * Depending on which one has the fewest keys.
     *
     * @param parent the parent node of the current node
     * @param ptr    the index of the pointer in the parent node that points to the current node
     * @throws DBAppException if an error occurs during the merging operation
     */
    public void merge(RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        if (ptr > 0) {
            //merge with left
            RTreeInnerNode<CustomPolygon> leftSibling = (RTreeInnerNode<CustomPolygon>) parent.getChild(ptr - 1);
            leftSibling.merge(parent.getKey(ptr - 1), this);
            parent.deleteAt(ptr - 1);
            leftSibling.serializeNode();
        } else {
            //merge with right
            RTreeInnerNode<CustomPolygon> rightSibling = (RTreeInnerNode<CustomPolygon>) parent.getChild(ptr + 1);
            this.merge(parent.getKey(ptr), rightSibling);
            parent.deleteAt(ptr);
            rightSibling.serializeNode();
        }
    }


    /**
     * Merges the current node with the given foreign node using a parent key.
     * <p>
     * This method is needed when the current node has fewer keys than the minimum required.
     *
     * @param parentKey   the key pulled from the parent node
     * @param foreignNode the node to be merged with the current node
     * @throws DBAppException if an error occurs during the merging operation
     */
    public void merge(Comparable<CustomPolygon> parentKey, RTreeInnerNode<CustomPolygon> foreignNode) throws DBAppException {
        this.insertRightAt(getNumberOfKeys(), parentKey, foreignNode.getFirstChild());
        for (int i = 0; i < foreignNode.getNumberOfKeys(); ++i)
            this.insertRightAt(getNumberOfKeys(), foreignNode.getKey(i), foreignNode.getChild(i + 1));
    }


    /**
     * Deletes the given key from the R-Tree node.
     *
     * @param key    The 'CustomPolygon' key to be deleted.
     * @param parent The parent 'RTreeInnerNode' of the current node.
     * @param ptr    The index of the current node within the parent node.
     * @return True if the key was successfully deleted, false otherwise.
     * @throws DBAppException If an exception occurs during the deletion process.
     */
    public boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        boolean done = false;

        // Recursively traverse the tree to find the node containing the key
        for (int i = 0; !done && i < getNumberOfKeys(); ++i)
            if (getKeys()[i].compareTo(key) > 0) {
                RTreeNode<CustomPolygon> b = deserializeNode(children[i]);
                done = b.delete(key, this, i);
                b.serializeNode();
            }

        // If the key was not found in the previous loop, search in the last child node
        if (!done) {
            RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[getNumberOfKeys()]);
            done = rTreeNode.delete(key, this, getNumberOfKeys());
            rTreeNode.serializeNode();
        }

        if (getNumberOfKeys() < this.getMinKeys()) {
            if (isRoot()) {
                this.getFirstChild().setRoot(true);
                getFirstChild().serializeNode();
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
     * Deletes the given key from the R-Tree node from a specific page's storage for the deletion.
     * This is needed when deleting a value from a different page's storage, not just the current page.
     *
     * @param key      The 'CustomPolygon' key to be deleted.
     * @param parent   The parent 'RTreeInnerNode' of the current node.
     * @param ptr      The index of the current node within the parent node.
     * @param pageName The name of the page's storage used for deletion associated with the R-Tree node.
     * @return True if the key was successfully deleted, false otherwise.
     * @throws DBAppException If an exception occurs during the deletion process.
     */
    public boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr, String pageName) throws DBAppException {
        boolean done = false;

        for (int i = 0; !done && i < getNumberOfKeys(); ++i)
            if (getKeys()[i].compareTo(key) > 0) {
                RTreeNode<CustomPolygon> b = deserializeNode(children[i]);
                done = b.delete(key, this, i, pageName);
                b.serializeNode();
            }

        if (!done) {
            RTreeNode<CustomPolygon> b = deserializeNode(children[getNumberOfKeys()]);
            done = b.delete(key, this, getNumberOfKeys(), pageName);
            b.serializeNode();
        }

        if (getNumberOfKeys() < this.getMinKeys()) {
            if (this.isRoot()) {
                RTreeNode<CustomPolygon> nd = this.getFirstChild();
                nd.setRoot(true);
                nd.serializeNode();
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
     * Deletes the key at the given index and either the left or right pointer
     *
     * @param keyIndex the index whose key will be deleted
     * @param childPtr either 0 for deleting the left pointer or 1 for deleting the right pointer
     */
    public void deleteAt(int keyIndex, int childPtr) {

        // Shift the remaining keys and pointers to the left, effectively deleting the key at the given index
        for (int i = keyIndex; i < getNumberOfKeys() - 1; ++i) {
            setKeys(new Comparable[]{getKeys()[i + 1]});
            children[i + childPtr] = children[i + childPtr + 1];
        }

        // If the child pointer to be deleted is the left pointer, move the rightmost pointer to the left
        if (childPtr == 0)
            children[getNumberOfKeys() - 1] = children[getNumberOfKeys()];
        setNumberOfKeys(getNumberOfKeys() - 1);
    }


    /**
     * Deletes the key at the given index.
     *
     * @param index The index of the key to be deleted.
     */
    public void deleteAt(int index) {

        // Call the 'deleteAt' method with the child pointer set to 1 (right pointer)
        deleteAt(index, 1);
    }

}
