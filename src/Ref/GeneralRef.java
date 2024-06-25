package src.Ref;

import src.DBGeneralEngine.DBAppException;

import java.util.ArrayList;

/**
 * This abstract class serves as the base class for managing references in the database system.
 * <p>
 * It handles the management of references, including handling overflow conditions and updating reference information.
 */
public abstract class GeneralRef {

    /**
     * Checks if the reference has reached an overflow condition.
     *
     * @return `true` if the reference has reached an overflow condition or `false` otherwise.
     */
    public abstract boolean isOverflow();


    /**
     * Updates the reference information when the associated page in the database has changed.
     *
     * @param oldPage The previous page associated with the reference.
     * @param newPage The new page associated with the reference.
     * @throws DBAppException If an error occurs during the update process.
     */
    public abstract void updateRef(String oldPage, String newPage) throws DBAppException;


    /**
     * Retrieves an ArrayList of all the Ref objects associated with the current GeneralRef instance.
     * If the GeneralRef is
     * A single Ref object      -> returns an ArrayList containing only that Ref object.
     * An OverflowRef object    -> recursively gather all the Ref objects from the OverflowRef and return them in the ArrayList.
     *
     * @return An ArrayList of all the Ref objects associated with the current GeneralRef instance.
     * @throws DBAppException If any database-related exception occurs during the retrieval of the references.
     */
    public ArrayList<Ref> getAllRef() throws DBAppException
    {

        ArrayList<Ref> allRef = new ArrayList<>();
        if(this instanceof Ref) {
            allRef.add((Ref)this);
        }
        else {
            OverflowRef overflowRef = (OverflowRef) this;
            allRef.addAll(overflowRef.getAllRef());
        }
        return allRef;
    }

}
