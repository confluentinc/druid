/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.security.pac4j;

import org.apache.druid.java.util.common.logger.Logger;
import org.pac4j.core.config.Config;
import org.pac4j.core.engine.DefaultCallbackLogic;
import org.pac4j.core.engine.DefaultSecurityLogic;
import org.pac4j.core.exception.http.HttpAction;
import org.pac4j.jee.context.JEEContext;
import org.pac4j.jee.http.adapter.JEEHttpActionAdapter;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class Pac4jFilter implements Filter
{
  private static final Logger log = new Logger(Pac4jFilter.class);

  private final Config pac4jConfig;
  private final Pac4jSessionStore sessionStore;
  private final String callbackPath;
  private final String authorizerName;

  public Pac4jFilter(
      Config pac4jConfig,
      String callbackPath,
      String authorizerName,
      String cookieName
  )
  {
    this.pac4jConfig = pac4jConfig;
    this.callbackPath = callbackPath;
    this.authorizerName = authorizerName;
    this.sessionStore = new Pac4jSessionStore(cookieName);
  }

  @Override
  public void init(FilterConfig filterConfig)
  {
    // nothing to do
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
      throws IOException, ServletException
  {
    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;

    JEEContext context = new JEEContext(request, response);

    if (request.getRequestURI().equals(callbackPath)) {
      DefaultCallbackLogic callbackLogic = new DefaultCallbackLogic();
      callbackLogic.perform(
          context,
          sessionStore,
          pac4jConfig,
          JEEHttpActionAdapter.INSTANCE,
          null,
          null,
          null
      );
    } else {
      DefaultSecurityLogic securityLogic = new DefaultSecurityLogic();
      try {
        securityLogic.perform(
            context,
            sessionStore,
            pac4jConfig,
            (ctx, session, profiles, parameters) -> {
              try {
                filterChain.doFilter(servletRequest, servletResponse);
              }
              catch (IOException | ServletException e) {
                throw new RuntimeException(e);
              }
              return null;
            },
            JEEHttpActionAdapter.INSTANCE,
            null,
            authorizerName,
            null
        );
      }
      catch (HttpAction e) {
        JEEHttpActionAdapter.INSTANCE.adapt(e, context);
      }
    }
  }

  @Override
  public void destroy()
  {
    // nothing to do
  }
}
