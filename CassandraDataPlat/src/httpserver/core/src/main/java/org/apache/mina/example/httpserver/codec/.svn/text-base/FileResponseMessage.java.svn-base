 /*
    *  Licensed to the Apache Software Foundation (ASF) under one
    *  or more contributor license agreements.  See the NOTICE file
    *  distributed with this work for additional information
    *  regarding copyright ownership.  The ASF licenses this file
    *  to you under the Apache License, Version 2.0 (the
    *  "License"); you may not use this file except in compliance
    *  with the License.  You may obtain a copy of the License at
    *
   *    http://www.apache.org/licenses/LICENSE-2.0
   *
   *  Unless required by applicable law or agreed to in writing,
   *  software distributed under the License is distributed on an
   *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   *  KIND, either express or implied.  See the License for the
   *  specific language governing permissions and limitations
   *  under the License.
   *
   */
  package org.apache.mina.example.httpserver.codec;
  
  import java.io.ByteArrayOutputStream;
import java.io.File;
  import java.io.IOException;
  import java.text.SimpleDateFormat;
  import java.util.Date;
  import java.util.HashMap;
  import java.util.Map;
  
import org.apache.mina.core.buffer.IoBuffer;
  
  /**
   * A HTTP response message.
   *
   * @author The Apache MINA Project (dev@mina.apache.org)
   * @version $Rev: 581234 $, $Date: 2007-10-02 22:39:48 +0900 (?, 02 10? 2007) $
   */
  public class FileResponseMessage extends File{
      public FileResponseMessage(String pathname) {
		super(pathname);
		// TODO Auto-generated constructor stub
	}

	/** HTTP response codes */
      public static final int HTTP_STATUS_SUCCESS = 200;
  
      public static final int HTTP_STATUS_NOT_FOUND = 404;
  
      /** Map<String, String> */
      private final Map<String, String> headers = new HashMap<String, String>();
  
      /** Storage for body of HTTP response. */
      private final ByteArrayOutputStream body = new ByteArrayOutputStream(1024);
  
      private File bodyFile = null;
      
      private int responseCode = HTTP_STATUS_SUCCESS;

  
      public Map<String, String> getHeaders() {
          return headers;
      }
  
      public void setContentType(String contentType) {
          headers.put("Content-Type", contentType);
      }
  
      public void setResponseCode(int responseCode) {
          this.responseCode = responseCode;
      }
  
      public int getResponseCode() {
          return this.responseCode;
      }
  
      public void appendBody(byte[] b) {
          try {
              body.write(b);
          } catch (IOException ex) {
              ex.printStackTrace();
          }
      }
  
      public void appendBody(String s) {
          try {
        	  /**
        	   * @author LiuGang
        	   * This point is the response message encoding config point.
        	   * To enable the scalability of mina http server, it is to be needed to take care of the encoding charset.
        	   */
              body.write(s.getBytes("utf-8"));
          } catch (IOException ex) {
              ex.printStackTrace();
          }
      }
  
      public IoBuffer getBody() {
          return IoBuffer.wrap(body.toByteArray());
      }
  
      public int getBodyLength() {
         return body.size();
     }

	public File getBodyFile() {
		return bodyFile;
	}
	
	public void setBodyFile(File bodyFile) {
		this.bodyFile = bodyFile;
	}
      
 }