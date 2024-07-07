package src.DBGeneralEngine;

import src.Ref.GeneralRef;
import src.Ref.Ref;

import java.util.ArrayList;


/**
 * The TreeIndex interface provides methods for inserting, deleting, and searching for data records based on their keys.
 * The interface also supports updating the reference to a data record when it is moved to a new page.
 *
 * @param <T> the type of the keys stored in the index, which must implement the Comparable interface
 */
public interface TreeIndex<T extends Comparable> {


    /**
     * Inserts a new data record into the index, associating it with the given key.
     *
     * @param key             the key to associate with the data record
     * @param recordReference the reference to the data record to be inserted
     * @throws DBAppException if there is an error during the insertion process
     */
    void insert(T key, Ref recordReference) throws DBAppException;


    /**
     * Searches the index for the data record associated with the given key.
     * @param key the key to search for
     * @return the data record associated with the given key, or null if the key is not found
     * @throws DBAppException if there is an error during the search process
     */
    GeneralRef search(T key) throws DBAppException;


    /**
     * Searches the index for all data records that match the given key or are greater than or equal to the given key.
     *
     * @param key the key to search for
     * @return a list of all data records that match the given key or are greater than or equal to the given key
     * @throws DBAppException if there is an error during the search process
     */
    ArrayList<GeneralRef> searchMTE(T key) throws DBAppException;


    /**
     * Searches the index for all data records that match the given key.
     *
     * @param key the key to search for
     * @return a list of all data records that match the given key
     * @throws DBAppException if there is an error during the search process
     */
    ArrayList<GeneralRef> searchMT(T key) throws DBAppException;


    /**
     * Searches the index for the appropriate leaf node to insert a new data record with the given key.
     *
     * @param key         the key of the new data record to be inserted
     * @param tableLength the current length of the table
     * @return the reference to the leaf node where the new data record should be inserted
     * @throws DBAppException if there is an error during the search process
     */
    Ref searchForInsertion(T key, int tableLength) throws DBAppException;


    /**
     * @return the leftmost leaf node in the index
     * @throws DBAppException if there is an error retrieving the leftmost leaf node
     */
    LeafNode getLeftmostLeaf() throws DBAppException;


    /**
     * Updates the reference to a data record in the index when a data record is moved to a new page.
     *
     * @param oldPage the name of the old page where the data record was stored
     * @param newPage the name of the new page where the data record is now stored
     * @param key     the key associated with the data record
     * @throws DBAppException if there is an error updating the reference
     */
    void updateRef(String oldPage, String newPage, T key) throws DBAppException;


    /**
     * Deletes the data record associated with the given key from the index.
     *
     * @param key the key of the data record to be deleted.
     * @return true if the deletion was successful, false otherwise
     * @throws DBAppException if there is an error during the deletion process
     */
    boolean delete(T key) throws DBAppException;


    /**
     * Deletes the data record associated with the given key from the index, starting the search from the given page.
     *
     * @param key      the key of the data record to be deleted
     * @param PageName the name of the page to start the search from
     * @return true if the deletion was successful, false otherwise
     * @throws DBAppException if there is an error during the deletion process
     */
    boolean delete(T key, String PageName) throws DBAppException;

}
