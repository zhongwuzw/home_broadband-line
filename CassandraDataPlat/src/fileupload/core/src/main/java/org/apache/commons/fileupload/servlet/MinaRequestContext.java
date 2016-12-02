package org.apache.commons.fileupload.servlet;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import org.apache.commons.fileupload.RequestContext;
import org.apache.mina.example.httpserver.codec.HttpRequestMessage;

/**
 * <p>Provides access to the request information needed for a request made to
 * an HTTP servlet.</p>
 *
 * @author <a href="mailto:martinc@apache.org">Martin Cooper</a>
 *
 * @since FileUpload 1.1
 *
 * @version $Id: ServletRequestContext.java 479262 2006-11-26 03:09:24Z niallp $
 */
public class MinaRequestContext implements RequestContext {

    // ----------------------------------------------------- Instance Variables

    /**
     * The request for which the context is being provided.
     */
    private HttpRequestMessage request;


    // ----------------------------------------------------------- Constructors

    /**
     * Construct a context for this request.
     *
     * @param request The request to which this context applies.
     */
    public MinaRequestContext(HttpRequestMessage request) {
        this.request = request;
    }


    // --------------------------------------------------------- Public Methods

    /**
     * Retrieve the character encoding for the request.
     *
     * @return The character encoding for the request.
     */
    public String getCharacterEncoding() {
        return request.getCharacterEncoding();
    }

    /**
     * Retrieve the content type of the request.
     *
     * @return The content type of the request.
     */
    public String getContentType() {
        return request.getHeader("Content-Type")[0];
    }

    /**
     * Retrieve the content length of the request.
     *
     * @return The content length of the request.
     */
    public int getContentLength() {
        return Integer.parseInt(request.getHeader("Content-Length")[0]);
    }

    /**
     * Retrieve the input stream for the request.
     *
     * @return The input stream for the request.
     *
     * @throws IOException if a problem occurs.
     */
    public InputStream getInputStream() throws IOException {
    	InputStream is = new ByteArrayInputStream(request.getBodybytes());
        return is;
    }

    /**
     * Returns a string representation of this object.
     *
     * @return a string representation of this object.
     */
    public String toString() {
        return "ContentLength="
            + this.getContentLength()
            + ", ContentType="
            + this.getContentType();
    }
}
