package fr.jetoile.sample.exception;

/**
 * User: khanh
 * To change this template use File | Settings | File Templates.
 */
public class NotFoundServiceException extends Exception {
    public NotFoundServiceException(String message) {
        super(message);
    }
}
