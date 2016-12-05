package com.chinamobile.httpserver.response;

import java.io.IOException;

import org.apache.mina.example.httpserver.codec.HttpResponseMessage;

public interface CmriUploadResponse {
	/**
     * @author cmri
     */
	HttpResponseMessage execute(Object message) throws IOException;
}
