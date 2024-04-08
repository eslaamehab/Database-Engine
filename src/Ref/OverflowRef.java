package src.Ref;

import src.DBGeneralEngine.DBAppException;
import src.APTree.OverflowPage;

import java.io.*;
import java.util.ArrayList;

public class OverflowRef extends GeneralRef implements Serializable
{

    private String firstPageName;

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
            throw new DBAppException("IO Exception in page: "+firstPageName+".class");
        }
        catch(ClassNotFoundException e) {
            throw new DBAppException("Class Not Found Exception");
        }
    }


    public String getFirstPageName() {
        return firstPageName;
    }

    public void setFirstPageName(String firstPageName) {
        this.firstPageName = firstPageName;
    }

    public OverflowPage getFirstPage() throws DBAppException {
        OverflowPage firstPage = deserializeOverflowPage(firstPageName);
        return firstPage;
    }

    public void setFirstPage(OverflowPage firstPage) throws DBAppException {
        this.firstPageName= firstPage.getPageName();
        firstPage.serialize();
    }


    public void insert(Ref ref) throws DBAppException{
        OverflowPage overflowPage = deserializeOverflowPage(firstPageName);
        overflowPage.addRecord(ref);
        overflowPage.serialize();
    }
    public void deleteRef(String pageName) throws DBAppException{

        OverflowPage overflowPage = deserializeOverflowPage(firstPageName);
        overflowPage.deleteRecord(pageName);
        if(overflowPage.getRefs().isEmpty())
        {
            File file = new File("data: "+ firstPageName + ".class");
            file.delete();
            firstPageName = overflowPage.getNext();
            overflowPage = deserializeOverflowPage(firstPageName);
        }
        else {
            overflowPage.serialize();
        }
    }

    public int getTotalSize() throws DBAppException
    {
        OverflowPage overflowPage = deserializeOverflowPage(firstPageName);
        return overflowPage.getTotalSize();
    }


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
    public boolean isOverflow() {
        return true;
    }
    public boolean isRecord() {
        return false;
    }
    @Override
    public void updateRef(String oldPage, String newPage) throws DBAppException {
        OverflowPage overflowPage = deserializeOverflowPage(firstPageName);
        overflowPage.updateRef(oldPage, newPage);
        overflowPage.serialize();
    }

    public ArrayList<Ref> getAllRef() throws DBAppException
    {
        return deserializeOverflowPage(firstPageName).getAllRefs();
    }
    public Ref getLastRef() throws DBAppException {
        OverflowPage overflowPage = deserializeOverflowPage(firstPageName);
        Ref lastRef = overflowPage.getLastRef();
        overflowPage.serialize();
        return lastRef;
    }

}

