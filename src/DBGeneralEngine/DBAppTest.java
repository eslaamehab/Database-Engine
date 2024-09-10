package src.DBGeneralEngine;

import java.io.*;
import java.util.Vector;

import static java.util.Objects.hash;


/**
 * DBAppTest class representing a test implementation for a database application.
 * This class manages a collection of inserts associated with a specific table.
 */
public class DBAppTest implements Serializable {

    /**
     * Attributes
     *
     * inserts -> Stores the inserted objects for the table
     * nextID -> Static variable to keep track of the next available ID
     * id -> Unique identifier for this instance
     * tableName -> Name of the table associated with this instance
     */
    private final Vector<Object> inserts;
    private static long nextId = 0L;
    private final long id;
    private final String tableName;


    /**
     * Constructor
     * Initializes the DBAppTest with a specified table name and prepares to manage inserts.
     *
     * @param tableName the name of the table for this instance
     * @throws IOException if an error occurs while initializing the next ID
     */
    public DBAppTest(String tableName) throws IOException {
        this.tableName = tableName;
        inserts = new Vector<>();
        nextId = getNextId();
        this.id = nextId++;
        saveNextId();
    }


    /**
     * Getters & Setters


     * Returns the Vector of inserted objects.
     *
     * @return a Vector containing the inserted objects
     */
    public Vector<Object> getInserts() {
        return inserts;
    }


    /**
     * Sets the next available ID for instances of DBAppTest.
     *
     * @param nextId the new next ID to be set
     */
    public static void setNextId(long nextId) {
        DBAppTest.nextId = nextId;
    }


    /**
     * Returns the name of the table associated with this instance.
     *
     * @return the name of the table
     */
    public String getTableName() {
        return tableName;
    }


    /**
     * Retrieves the next available ID from storage.
     *
     * @return the next available ID
     */
    public long getNextId() {
        long id = 0;
        try {
//            Add the path below
            File file = new File("Data: " + tableName + " Next ID");
            if (!file.exists())
                return 0;

//            Add the path below
            FileInputStream fileInputStream = new FileInputStream("Data/ " + tableName + " Next ID");
            ObjectInputStream in = new ObjectInputStream(fileInputStream);
            id = (long) in.readObject();

            in.close();
            fileInputStream.close();
        } catch (IOException ex) {
            System.out.println("IOException");
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException");
            ex.printStackTrace();
        }
        return id;
    }


    /**
     * Returns the unique identifier for this instance.
     *
     * @return the ID of this instance
     */
    public long getId() {
        return this.id;
    }


    /**
     * Retrieves the object at the specified index in the inserts Vector.
     *
     * @param id the index of the object to retrieve
     * @return the object at the specified index
     */
    public Object get(int id) {
        return inserts.get(id);
    }


    /**
     * Saves the next available ID to storage.
     *
     * @throws IOException if an error occurs while saving the ID
     */
    public void saveNextId() throws IOException {

//        Add the path below
        File file = new File("Data: " + tableName + " Next ID");
        if (!file.exists())
            file.delete();
        file.createNewFile();
        ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(file));
        stream.writeObject(nextId);
        stream.close();
    }


    /**
     * Checks if the specified ID matches this instance's ID.
     *
     * @param id the ID to check against
     * @return true if the IDs match, false otherwise
     */
    public boolean hasId(long id) {
        return this.id == id;
    }


    /**
     * Adds an object at the specified index in the inserts Vector.
     *
     * @param id  the index at which to insert the object
     * @param obj the object to be added
     */
    public void add(int id, Object obj) {
        inserts.insertElementAt(obj, id);

    }


    /**
     * Updates the object at the specified index in the inserts Vector.
     *
     * @param id  the index of the object to update
     * @param obj the new object to replace the existing one
     */
    public void update(int id, Object obj) {
        inserts.setElementAt(obj, id);
    }


    /**
     * Retrieves all values from the inserts Vector.
     *
     * @return a Vector containing all inserted objects
     */
    public Vector<Object> getValues() {
        return inserts;
    }


    /**
     * Adds an object to the end of the inserts Vector.
     *
     * @param obj the object to be added
     */
    public void add(Object obj) {
        inserts.add(obj);
    }


    /**
     * Returns a string representation of the DBAppTest instance.
     * This includes all inserted objects and the instance's ID.
     *
     * @return a string representation of the instance
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Object obj : inserts)
            stringBuilder.append(obj.toString()).append(", ");
        stringBuilder.append("inserting id").append(id);
        return stringBuilder.toString();
    }


    /**
     * Computes a hash code for this instance based on its attributes.
     *
     * @return the hash code for this instance
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((inserts == null) ? 0 : inserts.hashCode()) + hash(getId());
        return result;
    }


    /**
     * Checks equality between this instance and another object.
     *
     * @param obj the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof DBAppTest testAPTree))
            return false;
        System.out.println(getId() == testAPTree.getId());
        return getId() == testAPTree.getId() && inserts.equals(testAPTree.inserts);
    }


    /**
     * Main method for testing the DBAppTest class.
     * Dummy entries are instantiated here for testing purposes.
     */
    public static void main(String[] args) throws IOException {


    }
}

