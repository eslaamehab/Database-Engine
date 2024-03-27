package src.Ref;

import src.DBGeneralEngine.DBAppException;
import src.APTree.OverflowPage;

import java.io.*;
//import java.sql.Ref;
import java.util.ArrayList;

public class OverflowRef extends GeneralRef implements Serializable
{
    public void setFirstPageName(String firstPageName) {
        this.firstPageName = firstPageName;
    }

    private String firstPageName;
    //done (ta2riban) insert , delete and update pass the key and page

    public String getFirstPageName() {
        return firstPageName;
    }
    public OverflowPage getFirstPage() throws DBAppException {
        OverflowPage firstPage=deserializeOverflowPage(firstPageName);
        return firstPage;
    }

    public OverflowPage deserializeOverflowPage(String firstPageName2) throws DBAppException {

        try {
//			System.out.println("IO||||	 deserialize:overflow Page:"+firstPageName2);
            FileInputStream fileIn = new FileInputStream("data/"+ firstPageName2 + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            OverflowPage OFP =   (OverflowPage) in.readObject();
            in.close();
            fileIn.close();
            return OFP;
        }
        catch(IOException e) {
            throw new DBAppException("IO Exception while loading an overflow page from the disk"+"\tdata/"+firstPageName2+".class");
        }
        catch(ClassNotFoundException e) {
            throw new DBAppException("Class Not Found Exception");
        }
    }

    public void setFirstPage(OverflowPage firstPage) throws DBAppException {
        firstPageName= firstPage.getPageName();
        firstPage.serialize();
    }


    public void insert(Ref recordRef) throws DBAppException{
        OverflowPage firstPage=deserializeOverflowPage(firstPageName);
        firstPage.addRecord(recordRef);
        firstPage.serialize();
    }
    public void deleteRef(String page_name) throws DBAppException{
        deleteRefORIGINAL(page_name);
//		deleteRefNEW(page_name);
    }

    public void deleteRefORIGINAL(String page_name) throws DBAppException
    {
        OverflowPage firstPage =deserializeOverflowPage(firstPageName);
        firstPage.deleteRecord(page_name);
        if(firstPage.getRefs().isEmpty())
        {
            // TODO delete overflow page with name (firstPageName)
            File f = new File("data/"+firstPageName+".class");
//			System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file "+firstPageName);
            f.delete();
            firstPageName = firstPage.getNext();
            firstPage=deserializeOverflowPage(firstPageName); //TODO:why? shouldn't return here; this next page hasn't been edited or anything
        }
        //shouldn't this be in an else part ?
        firstPage.serialize();
    }

    public void deleteRefNEW(String page_name) throws DBAppException
    {
        OverflowPage first_ovp=deserializeOverflowPage(firstPageName);
        first_ovp.deleteRecord(page_name);
        if(first_ovp.getRefs().isEmpty())
        {
            // TODO delete overflow page with name (firstPageName)
//			File f = new File("data/"+firstPageName+".class");
//			System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file "+firstPageName);
//			f.delete();
//			firstPageName = first_ovp.getNext();
//			first_ovp=deserializeOverflowPage(firstPageName); //TODO:why? shouldn't return here; this next page hasn't been edited or anything
            String second_ovp_name = first_ovp.getNext();
            OverflowPage second_ovp = deserializeOverflowPage(second_ovp_name);
            {//delete file
                File f = new File("data/"+second_ovp_name+".class");
//				System.out.println("/////||||\\\\\\\\\\\\\\\\\\deleting file "+second_ovp_name);
                f.delete();
            }
            second_ovp.setPageName(firstPageName);
            first_ovp = second_ovp;
        }
        //shouldn't this be in an else part ?
        first_ovp.serialize();
    }

    public int getTotalSize() throws DBAppException
    {
        OverflowPage firstPage=deserializeOverflowPage(firstPageName);
        return firstPage.getTotalSize();
    }


    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        try
        {
            sb.append(deserializeOverflowPage(this.firstPageName));
        }
        catch(DBAppException e){
            System.out.println("WRONG first page name");
        }
        return sb.toString();
    }
    public boolean isOverflow() {
        return true;
    }
    public boolean isRecord() {
        return false;
    }
    @Override
    public void updateRef(String oldPage, String newPage) throws DBAppException {
        OverflowPage firstPage=deserializeOverflowPage(firstPageName);
        firstPage.updateRef(oldPage, newPage);
        firstPage.serialize();
    }

    public ArrayList<Ref> getAllRef() throws DBAppException
    {
        return deserializeOverflowPage(firstPageName).getAllRefs();
    }
    public Ref getLastRef() throws DBAppException {
        OverflowPage firstPage=deserializeOverflowPage(firstPageName);
        Ref ref=firstPage.getLastRef();
        firstPage.serialize();
        return ref;
    }

}

