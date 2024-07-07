package src.DBGeneralEngine;

import src.Ref.GeneralRef;

/**
 * The LeafNode interface defines the contract for leaf nodes in an R-tree data structure.
 * Leaf nodes are the bottom level nodes in the R-tree and contain the actual data records.
 * Each leaf node stores a set of keys and their associated data records.
 *
 * @param <T> the type of the keys stored in the leaf node, which must implement the Comparable interface
 */
public interface LeafNode<T extends Comparable<T>> {


    /**
     * Navigates between leaf nodes and returns the name of the next leaf node in the R-tree
     *
     * @return the name of the next leaf node, or null if there is no next node
     * @throws DBAppException if there is an error accessing the next node
     */
    String getNextNodeName() throws DBAppException;


    /**
     * Returns the number of keys (and associated records) stored in the leaf node.
     *
     * @return the number of keys in the leaf node
     */
    int getNumberOfKeys();


    /**
     * Returns the key at the given index in the leaf node.
     *
     * @param index the index of the key to retrieve
     * @return the key at the specified index
     */
    Comparable<T> getKey(int index);


    /**
     * Returns the data record associated with the key at the given index in the leaf node.
     *
     * @param index the index of the record to retrieve
     * @return the data record associated with the key at the given index
     */
    GeneralRef getRecord(int index);


    /**
     * Updates the reference to a data record in the leaf node.
     * Used when a data record is moved to a new page.
     *
     * @param oldPage the name of the old page where the data record was stored
     * @param newPage the name of the new page where the data record is now stored
     * @param key     the key associated with the data record
     * @throws DBAppException if there is an error updating the reference
     */
    void updateRef(String oldPage, String newPage, T key) throws DBAppException;

}
