package src.RPTree;

import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;
import src.Ref.Ref;

import java.io.Serializable;
import java.util.ArrayList;

public class RTreeInnerNode<CustomPolygon extends Comparable<CustomPolygon>> extends RTreeNode<CustomPolygon>  implements Serializable {

    /**
     * attributes
     */
    private final String[] children;


    /**
     * create RTreeNode given order.
     *
     * @param n the maximum number of keys in the nodes of the tree
     * @throws DBAppException
     */
    @SuppressWarnings("unchecked")
    public RTreeInnerNode(int n) throws DBAppException {
        super(n);
        keys = new Comparable[n];
        children = new String[n + 1];
    }


    /**
     * get child with specified index
     *
     * @return Node which is child at specified index
     * @throws DBAppException
     */
    public RTreeNode<CustomPolygon> getChild(int index) throws DBAppException {
        return (children[index] == null) ? null : deserializeNode(children[index]);
    }


    /**
     * creating child at specified index
     */
    public void setChild(int index, RTreeNode<CustomPolygon> child) {
        children[index] = (child == null) ? null : child.nodeName;
    }


    /**
     * get the first child of this node.
     *
     * @return first child node.
     * @throws DBAppException
     */
    public RTreeNode<CustomPolygon> getFirstChild() throws DBAppException {
        return deserializeNode(children[0]);
    }


    /**
     * get the last child of this node
     *
     * @return last child node.
     * @throws DBAppException
     */
    public RTreeNode<CustomPolygon> getLastChild() throws DBAppException {
        return deserializeNode(children[numberOfKeys]);
    }


    /**
     * @return the minimum keys values in InnerNode
     */
    public int minKeys() {
        return this.isRoot() ? 1 : (order + 2) / 2 - 1;
    }


    /**
     * insert given key in the corresponding index.
     *
     * @param key    key to be inserted
     * @param ref    reference which that inserted key is located
     * @param parent parent of that inserted node
     * @param ptr    index of pointer in the parent node pointing to the current node
     * @return value to be pushed up to the parent.
     * @throws DBAppException
     */
    public PushUp<CustomPolygon> insert(CustomPolygon key, Ref ref, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        int index = findIndex(key);

        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[index]);
        PushUp<CustomPolygon> pushUp = rTreeNode.insert(key, ref, this, index);

        if (pushUp == null) {
            rTreeNode.serializeNode();
            return null;
        }

        if (this.isFull()) {
            RTreeInnerNode<CustomPolygon> newNode = this.split(pushUp);
            Comparable<CustomPolygon> newKey = newNode.getFirstKey();
            newNode.deleteAt(0, 0);
            newNode.serializeNode();
            rTreeNode.serializeNode();
            return new PushUp<CustomPolygon>(newNode, newKey);
        } else {
            index = 0;
            while (index < numberOfKeys && getKey(index).compareTo(key) < 0)
                ++index;
            this.insertRightAt(index, pushUp.key, pushUp.newNode);
            rTreeNode.serializeNode();
            return null;
        }

    }


    /**
     * split the inner node and adjust values and pointers.
     *
     * @param pushup key to be pushed up to the parent in case of splitting.
     * @return Inner node after splitting
     * @throws DBAppException
     */
    @SuppressWarnings("unchecked")
    public RTreeInnerNode<CustomPolygon> split(PushUp<CustomPolygon> pushup) throws DBAppException {
        int keyIndex = this.findIndex((CustomPolygon) pushup.key);
        int midIndex = numberOfKeys / 2 - 1;

        // Split nodes
        if (keyIndex > midIndex)
            ++midIndex;

        int totalKeys = numberOfKeys + 1;

        RTreeInnerNode<CustomPolygon> newNode = new RTreeInnerNode<>(order);
        for (int i = midIndex; i < totalKeys - 1; ++i) {
            newNode.insertRightAt(i - midIndex, this.getKey(i), this.getChild(i + 1));
            numberOfKeys--;
        }
        newNode.setChild(0, this.getChild(midIndex));

        // Add new key
        if (keyIndex < totalKeys / 2)
            this.insertRightAt(keyIndex, pushup.key, pushup.newNode);
        else
            newNode.insertRightAt(keyIndex - midIndex, pushup.key, pushup.newNode);

        return newNode;
    }


    /**
     * find the correct place index of specified key in that node.
     *
     * @param key to be looked for
     * @return index of that given key
     */
    public int findIndex(CustomPolygon key) {
        for (int i = 0; i < numberOfKeys; ++i) {
            int cmp = getKey(i).compareTo(key);
            if (cmp > 0)
                return i;
        }
        return numberOfKeys;
    }


    /**
     * insert at given index a given key
     *
     * @param index where it inserts the key
     * @param key   to be inserted at index
     * @throws DBAppException
     */
    private void insertAt(int index, Comparable<CustomPolygon> key) throws DBAppException {
        for (int i = numberOfKeys; i > index; --i) {
            this.setKey(i, this.getKey(i - 1));
            this.setChild(i + 1, this.getChild(i));
        }
        this.setKey(index, key);
        numberOfKeys++;
    }


    /**
     * insert key and adjust left pointer with given child.
     *
     * @param index     where key is inserted
     * @param key       to be inserted in that index
     * @param leftChild child which this node points to with pointer at left of that index
     * @throws DBAppException
     */
    public void insertLeftAt(int index, Comparable<CustomPolygon> key, RTreeNode<CustomPolygon> leftChild) throws DBAppException {
        insertAt(index, key);
        this.setChild(index + 1, this.getChild(index));
        this.setChild(index, leftChild);
    }


    /**
     * insert key and adjust right pointer with given child.
     *
     * @param index      where key is inserted
     * @param key        to be inserted in that index
     * @param rightChild child which this node points to with pointer at right of that index
     * @throws DBAppException
     */
    public void insertRightAt(int index, Comparable<CustomPolygon> key, RTreeNode<CustomPolygon> rightChild) throws DBAppException {
        insertAt(index, key);
        this.setChild(index + 1, rightChild);
    }


    /**
     * delete key and return true or false if it is deleted or not
     *
     * @throws DBAppException
     */
    public boolean delete(CustomPolygon key, RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        boolean done = false;

        for (int i = 0; !done && i < numberOfKeys; ++i)
            if (keys[i].compareTo(key) > 0) {
                RTreeNode<CustomPolygon> b = deserializeNode(children[i]);
                done = b.delete(key, this, i);
                b.serializeNode();
            }

        if (!done) {
            RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[numberOfKeys]);
            done = rTreeNode.delete(key, this, numberOfKeys);
            rTreeNode.serializeNode();
        }

        if (numberOfKeys < this.minKeys()) {
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

        for (int i = 0; !done && i < numberOfKeys; ++i)
            if (keys[i].compareTo(key) > 0) {
                RTreeNode<CustomPolygon> b = deserializeNode(children[i]);
                done = b.delete(key, this, i, pageName);
                b.serializeNode();
            }

        if (!done) {
            RTreeNode<CustomPolygon> b = deserializeNode(children[numberOfKeys]);
            done = b.delete(key, this, numberOfKeys, pageName);
            b.serializeNode();
        }

        if (numberOfKeys < this.minKeys()) {
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
     * borrow from the right sibling or left sibling in case of overflow.
     *
     * @param parent of the current node
     * @param ptr    index of pointer in the parent node pointing to the current node
     * @return true or false if it can borrow form right sibling or left sibling or it can not
     * @throws DBAppException
     */
    public boolean borrow(RTreeInnerNode<CustomPolygon> parent, int ptr) throws DBAppException {
        //check left sibling
        if (ptr > 0) {
            RTreeInnerNode<CustomPolygon> leftSibling = (RTreeInnerNode<CustomPolygon>) parent.getChild(ptr - 1);
            if (leftSibling.numberOfKeys > leftSibling.minKeys()) {
                RTreeNode<CustomPolygon> leftSiblingLastChild = leftSibling.getLastChild();
                this.insertLeftAt(0, parent.getKey(ptr - 1), leftSiblingLastChild);
                leftSiblingLastChild.serializeNode();
                parent.deleteAt(ptr - 1);
                parent.insertRightAt(ptr - 1, leftSibling.getLastKey(), this);
                leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
                leftSibling.serializeNode();
                return true;
            }
        }

        //check right sibling
        if (ptr < parent.numberOfKeys) {
            RTreeInnerNode<CustomPolygon> rightSibling = (RTreeInnerNode<CustomPolygon>) parent.getChild(ptr + 1);
            if (rightSibling.numberOfKeys > rightSibling.minKeys()) {
                RTreeNode<CustomPolygon> rightSiblingFirstChild = rightSibling.getFirstChild();
                this.insertRightAt(this.numberOfKeys, parent.getKey(ptr), rightSiblingFirstChild);
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
     * @throws DBAppException
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
     * merge the current node with the passed node and pulling the passed key from the parent
     * to be inserted with the merged node
     *
     * @param parentKey   the pulled key from the parent to be inserted in the merged node
     * @param foreignNode the node to be merged with the current node
     * @throws DBAppException
     */
    public void merge(Comparable<CustomPolygon> parentKey, RTreeInnerNode<CustomPolygon> foreignNode) throws DBAppException {
        this.insertRightAt(numberOfKeys, parentKey, foreignNode.getFirstChild());
        for (int i = 0; i < foreignNode.numberOfKeys; ++i)
            this.insertRightAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getChild(i + 1));
    }


    /**
     * delete the key at the specified index with the option to delete the right or left pointer
     *
     * @param keyIndex the index whose key will be deleted
     * @param childPtr 0 for deleting the left pointer and 1 for deleting the right pointer
     */
    // 0 for left, 1 for right
    public void deleteAt(int keyIndex, int childPtr) {
        for (int i = keyIndex; i < numberOfKeys - 1; ++i) {
            keys[i] = keys[i + 1];
            children[i + childPtr] = children[i + childPtr + 1];
        }
        if (childPtr == 0)
            children[numberOfKeys - 1] = children[numberOfKeys];
        numberOfKeys--;
    }


    /**
     * searches for the record reference of the specified key
     *
     * @throws DBAppException
     */
    @Override
    public GeneralRef search(CustomPolygon key) throws DBAppException {
        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[findIndex(key)]);

        return rTreeNode.search(key);
    }


    public Ref searchForInsertion(CustomPolygon key, int tableLength) throws DBAppException {
        RTreeNode<CustomPolygon> rTreeNode = deserializeNode(children[findIndex(key)]);
        return rTreeNode.searchForInsertion(key, tableLength);
    }


    /**
     * delete the key at the given index and deleting its right child
     */
    public void deleteAt(int index) {
        deleteAt(index, 1);
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
}
