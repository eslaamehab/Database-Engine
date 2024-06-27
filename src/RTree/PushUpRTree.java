package src.RTree;

/**
 * PushUp class is used to push up keys to inner nodes when splitting an R-Tree
 *
 * @param <CustomPolygon> the type of the custom polygon objects stored in the R-Tree, which must implement the `Comparable` interface.
 */
public class PushUpRTree<CustomPolygon extends Comparable<CustomPolygon>> {

    /**
     * Attributes
     * <p>
     * newNode ->   an `RTreeNode` object containing the new node to be pushed up.
     * key ->       a `Comparable` object representing the key to be pushed up.
     */
    RTreeNode<CustomPolygon> newNode;
    Comparable<CustomPolygon> key;



    /**
     * Constructor
     * Initializes the `newNode` and `key` attributes with the provided values.
     *
     * @param newNode the new node to be pushed up.
     * @param key the key to be pushed up.
     */
    public PushUpRTree(RTreeNode<CustomPolygon> newNode, Comparable<CustomPolygon> key)
    {
        this.newNode = newNode;
        this.key = key;
    }


    /**
     * Getters & Setters
     */
    public RTreeNode<CustomPolygon> getNewNode() {
        return newNode;
    }

    public void setNewNode(RTreeNode<CustomPolygon> newNode) {
        this.newNode = newNode;
    }

    public Comparable<CustomPolygon> getKey() {
        return key;
    }

    public void setKey(Comparable<CustomPolygon> key) {
        this.key = key;
    }

}
