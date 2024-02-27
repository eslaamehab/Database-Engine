package src;

import java.sql.Ref;
import java.util.ArrayList;

public interface TreeIndex<T extends Comparable> {
    public GeneralReference search(T key) throws DBAppException;
    public Ref searchForInsertion(T key, int tableLength) throws DBAppException ;
    public boolean delete(T key) throws DBAppException;
    public boolean delete(T key, String Page_name) throws DBAppException;
    public void insert(T key, Ref recordReference) throws DBAppException;
    public LeafNode getLeftmostLeaf() throws DBAppException ;
    public ArrayList<GeneralReference> searchMTE(T key) throws DBAppException;
    public ArrayList<GeneralReference> searchMT(T key) throws DBAppException;
    public void updateRef(String oldPage,String newPage,T key) throws DBAppException;
}
