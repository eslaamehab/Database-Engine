package src.DBGeneralEngine;

import java.io.*;
import java.util.Vector;

import static java.util.Objects.hash;

public class DBAppTest implements Serializable {

    /**
     * Attributes
     */
    private final Vector<Object> inserts;
    private static long nextId = 0L;
    private final long id;
    private final String tableName;


    /**
     * Constructor
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
     */
    public Vector<Object> getInserts() {
        return inserts;
    }

    public static void setNextId(long nextId) {
        DBAppTest.nextId = nextId;
    }

    public String getTableName() {
        return tableName;
    }

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

    public long getId() {
        return this.id;
    }

    public Object get(int id) {
        return inserts.get(id);
    }


    /**
     * Methods
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


    public boolean hasId(long id) {
        return this.id == id;
    }


    public void add(int id, Object obj) {
        inserts.insertElementAt(obj, id);

    }

    public void update(int id, Object obj) {
        inserts.setElementAt(obj, id);
    }

    public Vector<Object> getValues() {
        return inserts;
    }

    public void add(Object obj) {
        inserts.add(obj);
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Object obj : inserts)
            stringBuilder.append(obj.toString()).append(", ");
        stringBuilder.append("inserting id").append(id);
        return stringBuilder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((inserts == null) ? 0 : inserts.hashCode()) + hash(getId());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof DBAppTest testAPTree))
            return false;
        System.out.println(getId() == testAPTree.getId());
        return getId() == testAPTree.getId() && inserts.equals(testAPTree.inserts);
    }

    public static void main(String[] args) throws IOException {

        // instantiate dummmy entries here

    }
}

