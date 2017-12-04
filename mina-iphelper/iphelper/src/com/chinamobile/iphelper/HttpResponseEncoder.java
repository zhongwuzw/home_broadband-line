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
  package com.chinamobile.iphelper;
  
  import java.nio.charset.CharacterCodingException;
  import java.nio.charset.Charset;
  import java.nio.charset.CharsetEncoder;
  import java.util.Map.Entry;
  
  import org.apache.mina.core.buffer.IoBuffer;
  import org.apache.mina.core.session.IoSession;
  import org.apache.mina.filter.codec.ProtocolEncoderOutput;
  import org.apache.mina.filter.codec.demux.MessageEncoder;
  
  /**
   * A {@link MessageEncoder} that encodes {@link HttpResponseMessage}.
   *
   * @author The Apache MINA Project (dev@mina.apache.org)
   * @version $Rev: 590006 $, $Date: 2007-10-30 18:44:02 +0900 (?, 30 10? 2007) $
   */
  public class HttpResponseEncoder implements MessageEncoder<HttpResponseMessage> {
  
      private static final byte[] CRLF = new byte[] { 0x0D, 0x0A };
  
      public HttpResponseEncoder() {
      }
  
      public void encode(IoSession session, HttpResponseMessage message,
              ProtocolEncoderOutput out) throws Exception {
          IoBuffer buf = IoBuffer.allocate(256);
          // Enable auto-expand for easier encoding
          buf.setAutoExpand(true);
  
          try {
              // output all headers except the content length
//              CharsetEncoder encoder = Charset.defaultCharset().newEncoder();
        	  CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
              buf.putString("HTTP/1.1 ", encoder);
              buf.putString(String.valueOf(message.getResponseCode()), encoder);
              switch (message.getResponseCode()) {
              case HttpResponseMessage.HTTP_STATUS_SUCCESS:
                  buf.putString(" OK", encoder);
                  break;
              case HttpResponseMessage.HTTP_STATUS_NOT_FOUND:
                  buf.putString(" Not Found", encoder);
                  break;
              }
              buf.put(CRLF);
              for (Entry<String, String> entry: message.getHeaders().entrySet()) {
                  buf.putString(entry.getKey(), encoder);
                  buf.putString(": ", encoder);
                  buf.putString(entry.getValue(), encoder);
                  buf.put(CRLF);
              }
              // now the content length is the body length
              buf.putString("Content-Length: ", encoder);
              buf.putString(String.valueOf(message.getBodyLength()), encoder);
              buf.put(CRLF);
              buf.put(CRLF);
              // add body
              buf.put(message.getBody());
          } catch (CharacterCodingException ex) {
              ex.printStackTrace();
          }
  
          buf.flip();
          out.write(buf);
      }
  }