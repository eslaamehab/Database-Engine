package src;

import java.io.*;
import java.util.Collections;
import java.util.Vector;

public class Page implements Serializable {

    Vector v = new Vector(); //vector of hashtables

    Vector<Tuple> records;
    String tableName;
    int pageNumber;

    public void serialize(Page p, String address) throws IOException {
        try {
            FileOutputStream fileOut = new FileOutputStream(address);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();
            //System.out.printf("Serialized data is saved in /tmp/employee.ser");
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public void deserialize(Page p, String address){
        try {
            FileInputStream fileIn = new FileInputStream(address);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            p = (Page) in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
            return;
        } catch (ClassNotFoundException c) {
            System.out.println("Employee class not found");
            c.printStackTrace();
            return;
        }
    }

    public Page(String tableName, int pageNumber){
        records = new Vector<>();
        this.tableName = tableName;
        this.pageNumber = pageNumber;
    }
    public Vector<Tuple> getRecords(){
        return records;
    }

    public int getSize(){
        return records.size();
    }
    public void printPage() {
        System.out.println(pageNumber);
        for(Tuple t:records) {
            System.out.println(t);
        }
        System.out.println("----------------------------------");

    }

    public Object[] insert(Tuple newRecord) {
        int low=0;
        int hi=records.size() - 1;
        int mid=-1;
        int pos=-1;
        while(low<=hi) {
            mid= low+hi>>1;
            if(records.get(mid).compareTo(newRecord) >= 0){
                hi=mid-1;
                pos=mid;
            }
            else {
                low=mid+1;
            }
        }
        if (pos==-1)
            pos = getSize();

        records.add(pos, newRecord);
        Object[] res = {pos, (getSize() == DBApp.MaximumRowsCountinPage + 1? records.remove(DBApp.MaximumRowsCountinPage) : null)};
        return res;
    }

    public Tuple getLast(){
        return records.isEmpty()?null:records.lastElement();
    }

    public Tuple getFirst(){
        return records.firstElement();
    }

}