package src;

import java.sql.Ref;
import java.util.ArrayList;

public abstract class GeneralReference {

    public abstract boolean isOverflow();
    public abstract boolean isRecord();
    public abstract void updateReference(String oldPage, String newPage) throws DBAppException;

    public ArrayList<Ref> getALLReference() throws DBAppException
    {
        ArrayList<Ref> results = new ArrayList<Ref>();
        if(this instanceof Ref)
        {
            results.add((Ref)this);
        }
        else
        {
            OverflowReference overflowReference = (OverflowReference) this;
            results.addAll(overflowReference.ALLgetRef());
        }

        return results;

    }

}
