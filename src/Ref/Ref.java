package src.Ref;

import java.io.Serializable;

public class Ref extends GeneralRef implements Serializable
{

    private String pageNo;

    public Ref(String pageNo)
    {
        this.pageNo = pageNo;
    }


    public String getPage()
    {
        return pageNo;
    }

    public void setPage(String pageNo) {
        this.pageNo=pageNo;
    }


    public boolean equals(Object obj) {
        Ref x= (Ref) obj;
        String firstNo = x.pageNo;
        Ref y= this;
        String secondNo = y.pageNo;
        return firstNo.equals(secondNo);
    }

    public int hashCode() {
        char[] charArray = pageNo.toCharArray();
        String res = "";
        for (int i=charArray.length-1; i>=0; i--) {
            char c = charArray[i];
            int cInt = c-'0';
            if ( cInt<0 || cInt>9 ) break;
            res = cInt + res;
        }
        return Integer.parseInt(res);
    }

    public void updateRef(String oldPage, String newPage) {
        pageNo = newPage;
    }

    @Override
    public boolean isOverflow() {
        return false;
    }

    @Override
    public boolean isRecord() {
        return false;
    }
}

