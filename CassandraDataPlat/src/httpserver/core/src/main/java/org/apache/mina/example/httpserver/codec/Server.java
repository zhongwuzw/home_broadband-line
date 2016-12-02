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
  
  import java.net.InetSocketAddress;
  
import org.apache.log4j.Logger;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
  
  /**
   * (<b>Entry point</b>) HTTP server
   *
   * @author The Apache MINA Project (dev@mina.apache.org)
   * @version $Rev: 600461 $, $Date: 2007-12-03 18:55:52 +0900 (?, 03 12? 2007) $
   */ 
  public class Server {
      /** Default HTTP port */
      private static final int DEFAULT_PORT = 9595;
	  static Logger logger = Logger.getLogger(Server.class);

      /** Tile server revision number */
      public static final String VERSION_STRING = "$Revision: 600461 $ $Date: 2007-12-03 18:55:52 +0900 (?, 03 12? 2007) $";
  
      public Server() {
    	  this(new String[0]);    	  
      }
      public Server(String[] args) {
    	  int port = DEFAULT_PORT;
    	  
          for (int i = 0; i < args.length; i++) {
              if (args[i].equals("-port")) {
                  port = Integer.parseInt(args[i + 1]);
              }
          }
  
          try {
              // Create an acceptor
              NioSocketAcceptor acceptor = new NioSocketAcceptor();
  
              // Create a service configuration
              acceptor.getFilterChain().addLast(
                      "protocolFilter",
                      new ProtocolCodecFilter(
                              new HttpServerProtocolCodecFactory()));
              acceptor.getFilterChain().addLast("logger", new LoggingFilter());
              acceptor.setHandler(new ServerHandler());
              acceptor.bind(new InetSocketAddress(port));
              acceptor.getSessionConfig().setReadBufferSize(204800);
              acceptor.getSessionConfig().setMaxReadBufferSize(2048000);
              logger.info("MINA HTTP Server now listening on port " + port);
          } catch (Exception ex) {
              ex.printStackTrace();
          }
      }
  }