package src.APTree;

import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;

public interface APTreeLeafNode<T extends Comparable<T>> {

    String getNextNodeName() throws DBAppException;
    int getNumberOfKeys();
    Comparable<T> getKey(int index);
    GeneralRef getRecord(int index);


    // safe delete below couple methods?
    APTreeLeafNode<T> searchForUpdateRef(T key);
    void updateRef(String oldPage, String newPage, T key) throws DBAppException;
}
