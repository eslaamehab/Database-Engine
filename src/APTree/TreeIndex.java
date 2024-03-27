package src.APTree;

import src.DBGeneralEngine.DBAppException;
import src.Ref.GeneralRef;

import java.sql.Ref;
import java.util.ArrayList;

public interface TreeIndex<T extends Comparable> {
    public GeneralRef search(T key) throws DBAppException;
    public Ref searchForInsertion(T key, int tableLength) throws DBAppException ;
    public boolean delete(T key) throws DBAppException;
    public boolean delete(T key, String PageName) throws DBAppException;
    public void insert(T key, Ref recordReference) throws DBAppException;
    public LeafNode getLeftmostLeaf() throws DBAppException ;
    public ArrayList<GeneralRef> searchMTE(T key) throws DBAppException;
    public ArrayList<GeneralRef> searchMT(T key) throws DBAppException;
    public void updateRef(String oldPage,String newPage,T key) throws DBAppException;
}
