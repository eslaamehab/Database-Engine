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
public class RTreeInnerNode<CustomPolygon extends Comparable<CustomPolygon>> extends RTreeNode<CustomPolygon>  implements Serializable {

    /**
     * Attributes
     *
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
     */
    public String[] getChildren() {
        return children;
    }

    /**
     * Gets a child with specific index
     * @return Node which is child at specified index
     */
    public RTreeNode<CustomPolygon> getChild(int index) throws DBAppException {
        return (children[index] == null) ? null : deserializeNode(children[index]);
    }

    /**
     * Gets the first child of this node.
     * @return first child node
     */
    public RTreeNode<CustomPolygon> getFirstChild() throws DBAppException {
        return deserializeNode(children[0]);
    }

    /**
     * Gets the last child of this node
     * @return last child node
     */
    public RTreeNode<CustomPolygon> getLastChild() throws DBAppException {
        return deserializeNode(children[getNumberOfKeys()]);
    }

    /**
     * Sets a child at specific index
     */
    public void setChild(int index, RTreeNode<CustomPolygon> child) {
        children[index] = (child == null) ? null : child.getNodeName();
    }



    /**
     * @return the minimum keys values in InnerNode
     */
    public int getMinKeys() {
        return this.isRoot() ? 1 : (getOrder() + 2) / 2 - 1;
    }


    /**
     * Inserts key at index
     * @param key    key to be inserted
     * @param ref    reference which that inserted key is located
     * @param parent parent of that inserted node
     * @param ptr    index of pointer in the parent node pointing to the current node
     * @return value to be pushed up to the parent
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
     * Splits the inner node
     * @param pushUpRTree key to be pushed up to the parent in case of splitting
     * @return Inner node after splitting
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
            setNumberOfKeys(getNumberOfKeys()-1);
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
     * Finds the index of the given
     * @param key to be looked for
     * @return index of that given key
     */
    public int findIndex(CustomPolygon key) {
        for (int i = 0; i < getNumberOfKeys(); ++i) {
            int cmp = getKey(i).compareTo(key);
            if (cmp > 0)
                return i;
        }
        return getNumberOfKeys();
    }

    // below searches here


    /**
     * Inserts at the given index the given key
     * @param index where it inserts the key
     * @param key   to be inserted at index
     */
    private void insertAt(int index, Comparable<CustomPolygon> key) throws DBAppException {
        for (int i = getNumberOfKeys(); i > index; --i) {
            this.setKey(i, this.getKey(i - 1));
            this.setChild(i + 1, this.getChild(i));
        }
        this.setKey(index, key);
        setNumberOfKeys(getNumberOfKeys()+1);
    }


    /**
     * Inserts the given key and adjust left pointer to the given left child
     * @param index     where key is inserted
     * @param key       to be inserted in that index
     * @param leftChild child which this node points to with pointer at left of that index
     */
    public void insertLeftAt(int index, Comparable<CustomPolygon> key, RTreeNode<CustomPolygon> leftChild) throws DBAppException {
        insertAt(index, key);
        this.setChild(index + 1, this.getChild(index));
        this.setChild(index, leftChild);
    }


    /**
     * Inserts the given key and adjust right pointer to the given right child
     * @param index      where key is inserted
     * @param key        to be inserted in that index
     * @param rightChild child which this node points to with pointer at right of that index
     */
    public void insertRightAt(int index, Comparable<CustomPolygon> key, RTreeNode<CustomPolygon> rightChild) throws DBAppException {
        insertAt(index, key);
        this.setChild(index + 1, rightChild);
    }


    /**
     * Deletes the given key
     * @return true if it is deleted or false otherwise
     */
    public boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        boolean done = false;

        for (int i = 0; !done && i < getNumberOfKeys(); ++i)
            if (getKeys()[i].compareTo(key) > 0) {
                RTreeNode<CustomPolygon> b = deserializeNode(children[i]);
                done = b.delete(key, this, i);
                b.serializeNode();
            }

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
     * Borrows from Right or left sibling
     * @param parent of the current node
     * @param ptr    index of pointer in the parent node pointing to the current node
     * @return true if it could borrow form either sibling or false otherwise
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
     * Merges with either left or right sibling
     * @param parent of the current node
     * @param ptr    index of pointer in the parent node pointing to the current node
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
     * Merges the given foreign node with the current node with parent key
     * @param parentKey  the pulled key from the parent
     * @param foreignNode the node to be merged with the current node
     */
    public void merge(Comparable<CustomPolygon> parentKey, RTreeInnerNode<CustomPolygon> foreignNode) throws DBAppException {
        this.insertRightAt(getNumberOfKeys(), parentKey, foreignNode.getFirstChild());
        for (int i = 0; i < foreignNode.getNumberOfKeys(); ++i)
            this.insertRightAt(getNumberOfKeys(), foreignNode.getKey(i), foreignNode.getChild(i + 1));
    }


    /**
     * Deletes the key at the given index and either the left or right pointer
     *
     * @param keyIndex the index whose key will be deleted
     * @param childPtr either 0 for deleting the left pointer or 1 for deleting the right pointer
     */
    public void deleteAt(int keyIndex, int childPtr) {
        for (int i = keyIndex; i < getNumberOfKeys() - 1; ++i) {
            setKeys(new Comparable[]{getKeys()[i + 1]});
            children[i + childPtr] = children[i + childPtr + 1];
        }
        if (childPtr == 0)
            children[getNumberOfKeys() - 1] = children[getNumberOfKeys()];
        setNumberOfKeys(getNumberOfKeys()-1);
    }


    /**
     * Searches for the record reference of the given key
     */
    @Override
    public GeneralRef search(CustomPolygon key) throws DBAppException {
        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[findIndex(key)]);

        return rTreeNode.search(key);
    }

    public ArrayList<GeneralRef> searchMTE(CustomPolygon key) throws DBAppException {
        RTreeNode<CustomPolygon> b = deserializeNode(children[findIndex(key)]);
        return b.searchMTE(key);
    }


    public ArrayList<GeneralRef> searchMT(CustomPolygon key) throws DBAppException {
        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[findIndex(key)]);
        return rTreeNode.searchMT(key);
    }


    public RTreeLeafNode searchForUpdateRef(CustomPolygon key) throws DBAppException {
        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[findIndex(key)]);
        return rTreeNode.searchForUpdateRef(key);
    }

    public Ref searchForInsertion(CustomPolygon key, int tableLength) throws DBAppException {
        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[findIndex(key)]);
        return rTreeNode.searchForInsertion(key, tableLength);
    }


    /**
     * Deletes the key at the given index
     */
    public void deleteAt(int index) {
        deleteAt(index, 1);
    }

}
