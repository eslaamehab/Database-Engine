package src.RTree;

/**
 * PushUp class is used to push up keys to inner nodes when splitting
 */
public class PushUpRTree<CustomPolygon extends Comparable<CustomPolygon>> {

    /**
     * Attributes
     */
    RTreeNode<CustomPolygon> newNode;
    Comparable<CustomPolygon> key;


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


    /**
     * Constructor
     */
    public PushUpRTree(RTreeNode<CustomPolygon> newNode, Comparable<CustomPolygon> key)
    {
        this.newNode = newNode;
        this.key = key;
    }
}
