package src.Ref;

import src.DBGeneralEngine.DBAppException;

import java.util.ArrayList;

public abstract class GeneralRef {

    /**
     * Abstract Methods
     */
    public abstract boolean isOverflow();

    public abstract void updateRef(String oldPage, String newPage) throws DBAppException;

    public ArrayList<Ref> getAllRef() throws DBAppException
    {

        ArrayList<Ref> allRef = new ArrayList<>();
        if(this instanceof Ref) {
            allRef.add((Ref)this);
        }
        else {
            OverflowRef overflowRef = (OverflowRef) this;
            allRef.addAll(overflowRef.getAllRef());
        }
        return allRef;
    }

}
