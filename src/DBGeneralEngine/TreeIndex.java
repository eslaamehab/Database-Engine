package src.DBGeneralEngine;

import src.Ref.GeneralRef;
import src.Ref.Ref;

import java.util.ArrayList;

public interface TreeIndex<T extends Comparable> {

    boolean delete(T key) throws DBAppException;

    boolean delete(T key, String PageName) throws DBAppException;

    void insert(T key, Ref recordReference) throws DBAppException;

    void updateRef(String oldPage, String newPage, T key) throws DBAppException;

    GeneralRef search(T key) throws DBAppException;

    Ref searchForInsertion(T key, int tableLength) throws DBAppException;

    LeafNode getLeftmostLeaf() throws DBAppException;

    ArrayList<GeneralRef> searchMTE(T key) throws DBAppException;

    ArrayList<GeneralRef> searchMT(T key) throws DBAppException;

}
