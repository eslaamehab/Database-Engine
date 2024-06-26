package src.Ref;

import src.DBGeneralEngine.DBAppException;
import src.DBGeneralEngine.OverflowPage;

import java.io.*;
import java.util.ArrayList;

/**
 * This class is used to represent a reference that may have overflow conditions.
 * It includes methods to handle these overflow conditions and maintain integrity of the stored data.
 * It inherits the basic functionality of GeneralRef.
 * It implements 'Serializable' interface to allow instances of `OverflowRef` to be easily serialized and deserialized
 * In order to enable persistence and transfer of the object's state.
 */
public class OverflowRef extends GeneralRef implements Serializable
{

    /**
     * Attributes
     *
     * firstPageName -> The name of the first page in the overflow reference chain.
     */
    private String firstPageName;


    /**
     * Getters & Setters
     */
    public String getFirstPageName()
    {
        return firstPageName;
    }

    public void setFirstPageName(String firstPageName)
    {
        this.firstPageName = firstPageName;
    }

    /**
     * Deserializes the first OverflowPage object from the file system.
     *
     * @return The deserialized OverflowPage object.
     * @throws DBAppException If an error occurs during the deserialization process.
     */
    public OverflowPage getFirstPage() throws DBAppException
    {
        return deserializeOverflowPage(firstPageName);
    }

    /**
     * Sets the first OverflowPage object and serializes it to the file system.
     *
     * @param firstPage The new first OverflowPage object.
     * @throws DBAppException If an error occurs during the serialization process.
     */
    public void setFirstPage(OverflowPage firstPage) throws DBAppException
    {
        this.firstPageName= firstPage.getPageName();
        firstPage.serialize();
    }

    /**
     * Retrieves an ArrayList of all the Ref objects associated with the OverflowRef instance.
     *
     * @return An ArrayList of all the Ref objects.
     * @throws DBAppException If an error occurs during the deserialization of the OverflowPage.
     */
    public ArrayList<Ref> getAllRef() throws DBAppException
    {
        return deserializeOverflowPage(firstPageName).getAllRefs();
    }

    /**
     * Retrieves the last Ref object in the OverflowRef chain.
     *
     * @return The last Ref object.
     * @throws DBAppException If an error occurs during the deserialization of the OverflowPage.
     */
    public Ref getLastRef() throws DBAppException {
        OverflowPage overflowPage = deserializeOverflowPage(firstPageName);
        Ref lastRef = overflowPage.getLastRef();
        overflowPage.serialize();
        return lastRef;
    }

    /**
     * Retrieves the total size of the Ref objects stored in the OverflowRef chain.
     *
     * @return The total size of the Ref objects.
     * @throws DBAppException If an error occurs during the deserialization of the OverflowPage.
     */
    public int getTotalSize() throws DBAppException
    {
        OverflowPage overflowPage = deserializeOverflowPage(firstPageName);
        return overflowPage.getTotalSize();
    }

    /**
     * Deserializes an OverflowPage object from the file system.
     *
     * @param firstPageName The name of the OverflowPage to deserialize.
     * @return The deserialized OverflowPage object.
     * @throws DBAppException If an error occurs during the deserialization process.
     */
    public OverflowPage deserializeOverflowPage(String firstPageName) throws DBAppException {

        try {
            FileInputStream fileInputStream = new FileInputStream("data: "+ firstPageName + ".class");
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            OverflowPage overflowPage = (OverflowPage) objectInputStream.readObject();
            objectInputStream.close();
            fileInputStream.close();
            return overflowPage;
        }
        catch(IOException e) {
            throw new DBAppException("IO Exception in page: "+firstPageName );
        }
        catch(ClassNotFoundException e) {
            throw new DBAppException("Class Not Found Exception in: "+ firstPageName);
        }
    }

    /**
     * Inserts a Ref object into the OverflowRef chain.
     *
     * @param ref The Ref object to be inserted.
     * @throws DBAppException If an error occurs during the serialization of the OverflowPage.
     */
    public void insert(Ref ref) throws DBAppException
    {
        OverflowPage overflowPage = deserializeOverflowPage(firstPageName);
        overflowPage.addRecord(ref);
        overflowPage.serialize();
    }

    /**
     * Deletes a Ref object from the OverflowRef chain.
     *
     * @param pageName The name of the page containing the Ref object to be deleted.
     * @throws DBAppException If an error occurs during the deserialization or serialization of the OverflowPage.
     */
    public void deleteRef(String pageName) throws DBAppException
    {

        OverflowPage overflowPage = deserializeOverflowPage(firstPageName);
        overflowPage.deleteRecord(pageName);
        if(overflowPage.getRefs().isEmpty())
        {
            File file = new File("data: "+ firstPageName + ".class");
            file.delete();
            firstPageName = overflowPage.getNext();
            deserializeOverflowPage(firstPageName);
        }
        else {
            overflowPage.serialize();
        }
    }

    /**
     * Checks if the OverflowRef has reached an overflow condition.
     *
     * @return Always returns `true` since this is an OverflowRef.
     */
    public boolean isOverflow() {
        return true;
    }

    /**
     * Updates the reference information when the associated page in the database has changed.
     *
     * @param oldPage The previous page associated with the reference.
     * @param newPage The new page associated with the reference.
     * @throws DBAppException If an error occurs during the serialization of the OverflowPage.
     */
    @Override
    public void updateRef(String oldPage, String newPage) throws DBAppException {
        OverflowPage overflowPage = deserializeOverflowPage(firstPageName);
        overflowPage.updateRef(oldPage, newPage);
        overflowPage.serialize();
    }

    /**
     * Mainly for debugging and visualization
     *
     * @return A string representation of the OverflowRef object.
     */
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();

        try
        {
            stringBuilder.append(deserializeOverflowPage(this.firstPageName));
        }
        catch(DBAppException e){
            System.out.println("Error deserializing first page");
        }
        return stringBuilder.toString();
    }

}

