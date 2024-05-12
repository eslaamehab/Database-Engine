package src.DBGeneralEngine;

import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;

public interface LeafNode<T extends Comparable<T>> {

    String getNextNodeName() throws DBAppException;
    int getNumberOfKeys();
    Comparable<T> getKey(int index);
    GeneralRef getRecord(int index);

    void updateRef(String oldPage, String newPage, T key) throws DBAppException;
}
