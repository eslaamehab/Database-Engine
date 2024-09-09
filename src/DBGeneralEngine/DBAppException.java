package src.DBGeneralEngine;


/**
 * DBAppException class extends the Exception class to provide custom exceptions for the database application.
 * This exception can be thrown to indicate specific errors that occur within the database operations.
 */
public class DBAppException extends Exception {


    /**
     * Default constructor for DBAppException.
     * Initializes a new instance of the exception without a detail message.
     */
    public DBAppException() {
        super();
    }


    /**
     * Constructor for DBAppException with a specified detail message.
     * This message can provide additional context about the error.
     *
     * @param arg0 the detail message to be associated with the exception
     */
    public DBAppException(String arg0) {
        super(arg0);
    }

}
