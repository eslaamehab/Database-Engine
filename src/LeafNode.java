package src;

public interface LeafNode<T extends Comparable<T>> {

    public LeafNode getNext() throws DBAppException;
    public int getNumberOfKeys();
    public Comparable<T> getKey(int index);
    public GeneralReference getRecord(int index);

    public LeafNode<T> searchForUpdateRef(T key);
    public void updateRef(String oldPage,String newPage,T key) throws DBAppException;
}
