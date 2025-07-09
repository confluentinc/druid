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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.util.Pac4jConstants;
import org.pac4j.jee.context.JEEContext;
import org.pac4j.jee.context.session.JEESessionStore;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Optional;

public class Pac4jSessionStore implements SessionStore
{
  private static final Logger LOGGER = new Logger(Pac4jSessionStore.class);
  private final JEESessionStore delegate = JEESessionStore.INSTANCE;
  private final String cookieName;

  public Pac4jSessionStore(String cookieName)
  {
    this.cookieName = cookieName;
  }

  @Override
  public Optional<String> getSessionId(WebContext context, boolean createSession)
  {
    return delegate.getSessionId(context, createSession);
  }

  @Override
  public Optional<Object> get(WebContext context, String key)
  {
    return delegate.get(context, key);
  }

  @Override
  public void set(WebContext context, String key, Object value)
  {
    Object profile = value;
    Cookie cookie;

    if (value == null) {
      cookie = new Cookie(key, "");
      cookie.setMaxAge(0);
    } else {
      if (Pac4jConstants.USER_PROFILES.equals(key)) {
        profile = serializeProfile(value);
      }
      String serializedProfile = Base64.getEncoder().encodeToString((byte[]) profile);
      cookie = new Cookie(key, serializedProfile);
      cookie.setMaxAge(-1);
    }

    cookie.setHttpOnly(true);
    cookie.setSecure(true);
    cookie.setPath("/");

    if (context instanceof JEEContext) {
      JEEContext jeeContext = (JEEContext) context;
      HttpServletResponse response = jeeContext.getNativeResponse();
      response.addCookie(cookie);
    }

    delegate.set(context, key, value);
  }

  @Override
  public boolean destroySession(WebContext context)
  {
    return delegate.destroySession(context);
  }

  @Override
  public Optional<Object> getTrackableSession(WebContext context)
  {
    return delegate.getTrackableSession(context);
  }

  @Override
  public Optional<SessionStore> buildFromTrackableSession(WebContext context, Object trackableSession)
  {
    return delegate.buildFromTrackableSession(context, trackableSession);
  }

  @Override
  public boolean renewSession(WebContext context)
  {
    return delegate.renewSession(context);
  }

  /**
   * Serialize object using standard Java serialization
   */
  private byte[] serializeProfile(Object obj)
  {
    Preconditions.checkNotNull(obj, "Object to serialize cannot be null");
    
    if (!(obj instanceof Serializable)) {
      throw new IllegalArgumentException("Object must be Serializable");
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
      oos.flush();
      return baos.toByteArray();
    }
    catch (IOException e) {
      throw new RuntimeException("Failed to serialize object", e);
    }
  }

  /**
   * Deserialize object using standard Java serialization
   */
  private Object deserializeProfile(byte[] data)
  {
    Preconditions.checkNotNull(data, "Data to deserialize cannot be null");
    
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
         ObjectInputStream ois = new ObjectInputStream(bais)) {
      return ois.readObject();
    }
    catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize object", e);
    }
  }
}
