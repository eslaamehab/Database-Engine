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
     *
     * @attribute children is an array of leafs to hold the children of the inner node
     */
    private final String[] children;


    /**
     * Constructor
     * <p>
     * Creates an Inner Node for the B+ Tree with a specified order.
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
     * get child at specified index
     *
     * @return Node child at specified index
     */
    public BPTreeNode<T> getChild(int index) throws DBAppException {
        return children[index] == null ? null : deserializeNode(children[index]);
    }

    /**
     * create child at specified index
     */
    public void setChild(int index, BPTreeNode<T> child) {
        children[index] = (child == null) ? null : child.getNodeName();
    }

    /**
     * get the first child of this node.
     *
     * @return first child node.
     */
    public BPTreeNode<T> getFirstChild() throws DBAppException {
        return deserializeNode(children[0]);
    }

    /**
     * get the last child of this node
     *
     * @return last child node.
     */
    public BPTreeNode<T> getLastChild() throws DBAppException {
        return deserializeNode(children[getNumberOfKeys()]);
    }


    /**
     * @return the minimum keys values in InnerNode
     */
    public int minKeys() {
        return this.isRoot() ? 1 : (getOrder() + 2) / 2 - 1;
    }


    /**
     * insert given key in the corresponding index.
     *
     * @param key    key to be inserted
     * @param ref    reference where inserted key is located
     * @param parent parent of that inserted node
     * @param ptr    index of pointer in the parent node pointing to the current node
     * @return value to be pushed up to the parent.
     */
    public PushUpBPTree<T> insert(T key, Ref ref, BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        int index = findIndex(key);
        BPTreeNode<T> bpTreeNode = deserializeNode(children[index]);

        PushUpBPTree<T> pushUp = bpTreeNode.insert(key, ref, this, index);

        if (pushUp == null) {
            bpTreeNode.serializeNode();
            return null;
        }

        if (this.isFull()) {
            BPTreeInnerNode<T> newBpTreeNode = this.split(pushUp);
            Comparable<T> newKey = newBpTreeNode.getFirstKey();
            newBpTreeNode.deleteAt(0, 0);
            newBpTreeNode.serializeNode();
            bpTreeNode.serializeNode();

            return new PushUpBPTree<T>(newBpTreeNode, newKey);
        } else {
            index = 0;
            while (index < getNumberOfKeys() && getKey(index).compareTo(key) < 0)
                ++index;
            this.insertRightAt(index, pushUp.key, pushUp.newNode);
            bpTreeNode.serializeNode();

            return null;
        }
    }


    /**
     * split the inner node and adjust values and pointers.
     *
     * @param pushup key to be pushed up to the parent in case of splitting.
     * @return Inner node after splitting
     */

    public BPTreeInnerNode<T> split(PushUpBPTree<T> pushup) throws DBAppException {

        int keyIndex = this.findIndex((T) pushup.key);
        int midIndex = getNumberOfKeys() / 2 - 1;

        // split nodes
        if (keyIndex > midIndex) {
            ++midIndex;
        }

        int totalKeys = getNumberOfKeys() + 1;

        // move keys to new node
        BPTreeInnerNode<T> newNode = new BPTreeInnerNode<T>(getOrder());
        for (int i = midIndex; i < totalKeys - 1; ++i) {
            newNode.insertRightAt(i - midIndex, this.getKey(i), this.getChild(i + 1));

            setNumberOfKeys(getNumberOfKeys() - 1);
        }
        newNode.setChild(0, this.getChild(midIndex));

        // insert new key
        if (keyIndex < totalKeys / 2) {
            this.insertRightAt(keyIndex, pushup.key, pushup.newNode);
        } else {
            newNode.insertRightAt(keyIndex - midIndex, pushup.key, pushup.newNode);
        }

        return newNode;
    }


    /**
     * find the correct place index of specified key in that node.
     *
     * @param key to be looked for
     * @return index of that given key
     */
    public int findIndex(T key) {
        for (int i = 0; i < getNumberOfKeys(); ++i) {
            int compareKeys = getKey(i).compareTo(key);
            if (compareKeys > 0)
                return i;
        }
        return getNumberOfKeys();
    }


    /**
     * insert at given index a given key
     *
     * @param index where it inserts the key
     * @param key   to be inserted at index
     */
    private void insertAt(int index, Comparable<T> key) throws DBAppException {
        for (int i = getNumberOfKeys(); i > index; --i) {
            this.setKey(i, this.getKey(i - 1));
            this.setChild(i + 1, this.getChild(i));
        }
        this.setKey(index, key);
        setNumberOfKeys(getNumberOfKeys() + 1);
    }


    /**
     * insert key and adjust left pointer with given child.
     *
     * @param index     where key is inserted
     * @param key       key to be inserted in that index
     * @param leftChild child which this node points to with pointer at left of that index
     */
    public void insertLeftAt(int index, Comparable<T> key, BPTreeNode<T> leftChild) throws DBAppException {
        insertAt(index, key);
        this.setChild(index + 1, this.getChild(index));
        this.setChild(index, leftChild);
    }


    /**
     * insert key and adjust right pointer with given child.
     *
     * @param index      where key is inserted
     * @param key        key to be inserted in that index
     * @param rightChild child which this node points to with pointer at right of that index
     */
    public void insertRightAt(int index, Comparable<T> key, BPTreeNode<T> rightChild) throws DBAppException {
        insertAt(index, key);
        this.setChild(index + 1, rightChild);
    }


    /**
     * delete key and return true or false to acknowledge
     */
    public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        boolean done = false;

        for (int i = 0; !done && i < getNumberOfKeys(); ++i)
            if (getKeys()[i].compareTo(key) > 0) {
                BPTreeNode<T> b = deserializeNode(children[i]);
                done = b.delete(key, this, i);
                b.serializeNode();
            }

        if (!done) {
            BPTreeNode<T> bpTreeNode = deserializeNode(children[getNumberOfKeys()]);
            done = bpTreeNode.delete(key, this, getNumberOfKeys());
            bpTreeNode.serializeNode();
        }

        if (getNumberOfKeys() < this.minKeys()) {
            if (isRoot()) {
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


    public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr, String pageName) throws DBAppException {
        boolean done = false;

        for (int i = 0; !done && i < getNumberOfKeys(); ++i)
            if (getKeys()[i].compareTo(key) > 0) {
                BPTreeNode<T> bpTreeNode = deserializeNode(children[i]);
                done = bpTreeNode.delete(key, this, i, pageName);
                bpTreeNode.serializeNode();
            }

        if (!done) {
            BPTreeNode<T> bpTreeNode = deserializeNode(children[getNumberOfKeys()]);
            done = bpTreeNode.delete(key, this, getNumberOfKeys(), pageName);
            bpTreeNode.serializeNode();
        }

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
     * borrow from the right sibling or left sibling in case of overflow.
     *
     * @param parent of the current node
     * @param ptr    index of pointer in the parent node pointing to the current node
     * @return true or false to acknowledge successful borrow
     */
    public boolean borrow(BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        //check left sibling
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

        //check right sibling
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
     * try to merge with left or right sibling in case of overflow
     *
     * @param parent of the current node
     * @param ptr    index of pointer in the parent node pointing to the current node
     */
    public void merge(BPTreeInnerNode<T> parent, int ptr) throws DBAppException {
        if (ptr > 0) {
            //merge with left sibling
            BPTreeInnerNode<T> leftSibling = (BPTreeInnerNode<T>) parent.getChild(ptr - 1);
            leftSibling.merge(parent.getKey(ptr - 1), this);
            parent.deleteAt(ptr - 1);
            leftSibling.serializeNode();
        } else {
            //merge with right sibling
            BPTreeInnerNode<T> rightSibling = (BPTreeInnerNode<T>) parent.getChild(ptr + 1);
            this.merge(parent.getKey(ptr), rightSibling);
            parent.deleteAt(ptr);
            rightSibling.serializeNode();
        }
    }


    /**
     * merge the current node with the passed node and pulling the passed key from the parent to be inserted with the merged node
     *
     * @param parentKey   the pulled key from the parent to be inserted in the merged node
     * @param foreignNode the node to be merged with the current node
     */
    public void merge(Comparable<T> parentKey, BPTreeInnerNode<T> foreignNode) throws DBAppException {
        this.insertRightAt(getNumberOfKeys(), parentKey, foreignNode.getFirstChild());
        for (int i = 0; i < foreignNode.getNumberOfKeys(); ++i)
            this.insertRightAt(getNumberOfKeys(), foreignNode.getKey(i), foreignNode.getChild(i + 1));
    }


    /**
     * delete the key at the specified index with the option to delete the right or left pointer
     *
     * @param keyIndex the index whose key will be deleted
     * @param ptr      0 for deleting the left pointer and 1 for deleting the right pointer
     */
    public void deleteAt(int keyIndex, int ptr) {
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
     * searches for the record reference of the specified key
     */
    @Override
    public GeneralRef search(T key) throws DBAppException {
        BPTreeNode<T> bpTreeNode = deserializeNode(children[findIndex(key)]);
        return bpTreeNode.search(key);
    }


    public Ref searchForInsertion(T key, int tableLength) throws DBAppException {
        BPTreeNode<T> bpTreeNode = deserializeNode(children[findIndex(key)]);
        return bpTreeNode.searchForInsertion(key, tableLength);
    }


    /**
     * delete the key at the given index
     */
    public void deleteAt(int index) {
        deleteAt(index, 1);
    }


    public ArrayList<GeneralRef> searchMTE(T key) throws DBAppException {
        BPTreeNode<T> bpTreeNode = deserializeNode(children[findIndex(key)]);
        return bpTreeNode.searchMTE(key);
    }


    public ArrayList<GeneralRef> searchMT(T key) throws DBAppException {
        BPTreeNode<T> b = deserializeNode(children[findIndex(key)]);
        return b.searchMT(key);
    }


    public BPTreeLeafNode searchForUpdateRef(T key) throws DBAppException {
        BPTreeNode<T> b = deserializeNode(children[findIndex(key)]);
        return b.searchForUpdateRef(key);
    }

}

