package src.APTree;

import java.io.*;
import java.util.Vector;

import static java.util.Objects.hash;

public class APTreeTest implements Serializable {

    private Vector<Object> inserts;
    private static long nextId = 0l;
    private long id;
    private final String tableName;

    public void saveNextId() throws IOException {
        File file = new File("Data: " + tableName+" Next ID");
        if (!file.exists())
            file.delete();
        file.createNewFile();
        ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(file));
        stream.writeObject(nextId);
        stream.close();
    }

    public long getNextId() {
        long x = 0;
        try {

            File f = new File("Data: " + tableName+" Next ID");
            if (!f.exists())
                return 0;
            FileInputStream file = new FileInputStream("Add path here/" + tableName+" Next ID");
            ObjectInputStream in = new ObjectInputStream(file);
             x = (long) in.readObject();

            in.close();
            file.close();
        }

        catch (IOException ex) {
            System.out.println("IOException");
            // ex.printStackTrace();
        }

        catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException");
        }
        return x;
    }

    public APTreeTest(String tableName) throws IOException {
        this.tableName = tableName;
        inserts = new Vector<Object>();
        nextId = getNextId();
        this.id = nextId++;
        saveNextId();
    }

    public boolean hasId(long id) {
        return this.id == id;
    }

    public long getId() {
        return this.id;
    }

    public Object get(int id) {
        return inserts.get(id);
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
        String str = "";
        for (Object obj : inserts)
            str += obj.toString() + ", ";
        str += " insert id " + id;
        return str;
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
        if (!(obj instanceof APTreeTest))
            return false;
        APTreeTest testAPTree = (APTreeTest) obj;
         System.out.println(getId() == testAPTree.getId());
        return getId() == testAPTree.getId() && inserts.equals(testAPTree.inserts);
    }

    public static void main(String[] args) throws IOException {

        // instantiate entries here
    }
}

