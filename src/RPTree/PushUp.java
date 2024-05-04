package src.RPTree;

public class PushUp<CustomPolygon extends Comparable<CustomPolygon>> {

    /**
     * This class is used for push keys up to the inner nodes in case
     * of splitting at a lower level
     */
    RTreeNode<CustomPolygon> newNode;
    Comparable<CustomPolygon> key;

    public PushUp(RTreeNode<CustomPolygon> newNode, Comparable<CustomPolygon> key)
    {
        this.newNode = newNode;
        this.key = key;
    }
}
