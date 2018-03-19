package org.fanlychie.jibatch.exception;

/**
 * Created by fanlychie on 2018/3/19.
 */
public class RuntimeCastException extends RuntimeException {

    private Throwable cause;

    public RuntimeCastException(Throwable throwable) {
        this.cause = throwable;
    }

    @Override
    public synchronized Throwable getCause() {
        return cause;
    }

}