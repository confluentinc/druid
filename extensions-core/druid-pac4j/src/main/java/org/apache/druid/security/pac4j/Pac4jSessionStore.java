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
import com.google.common.io.ByteStreams;
import org.apache.druid.crypto.CryptoService;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.util.Pac4jConstants;
import org.pac4j.jee.context.JEEContext;
import org.pac4j.jee.context.session.JEESessionStore;

import javax.annotation.Nullable;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Code here is slight adaptation from Apache Knox KnoxSessionStore
 * for storing oauth session information in encrypted cookies.
 */
public class Pac4jSessionStore implements SessionStore
{
  private static final Logger LOGGER = new Logger(Pac4jSessionStore.class);
  
  public static final String PAC4J_SESSION_PREFIX = "pac4j.session.";
  
  private final JEESessionStore delegate = JEESessionStore.INSTANCE;
  private final CryptoService cryptoService;

  public Pac4jSessionStore(String cookiePassphrase)
  {
    this.cryptoService = new CryptoService(
        cookiePassphrase,
        "AES",
        "CBC", 
        "PKCS5Padding",
        "PBKDF2WithHmacSHA256",
        128,
        65536,
        128
    );
  }

  @Override
  public Optional<String> getSessionId(WebContext context, boolean createSession)
  {
    return delegate.getSessionId(context, createSession);
  }

  @Override
  public Optional<Object> get(WebContext context, String key)
  {
    final Cookie cookie = getCookie(context, PAC4J_SESSION_PREFIX + key);
    Object value = null;
    if (cookie != null && cookie.getValue() != null) {
      value = uncompressDecryptBase64(cookie.getValue());
    }
    LOGGER.debug("Get from session: [%s] = [%s]", key, value);
    return Optional.ofNullable(value);
  }

  @Override
  public void set(WebContext context, String key, @Nullable Object value)
  {
    Object profile = value;
    Cookie cookie;

    if (value == null) {
      cookie = new Cookie(PAC4J_SESSION_PREFIX + key, "");
      cookie.setMaxAge(0);
    } else {
      if (Pac4jConstants.USER_PROFILES.equals(key)) {
        /* trim the profile object */
        profile = clearUserProfile(value);
      }
      LOGGER.debug("Save in session: [%s] = [%s]", key, profile);
      
      String encryptedValue = compressEncryptBase64(profile);
      cookie = new Cookie(PAC4J_SESSION_PREFIX + key, encryptedValue);
      cookie.setMaxAge(900); // 15 minutes
    }

    cookie.setHttpOnly(true);
    cookie.setSecure(isHttpsOrSecure(context));
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

  @Nullable
  private String compressEncryptBase64(final Object o)
  {
    if (o == null || "".equals(o)
        || (o instanceof Map<?, ?> && ((Map<?, ?>) o).isEmpty())) {
      return null;
    } else {
      byte[] bytes = serializeToBytes((Serializable) o);
      
      bytes = compress(bytes);
      if (bytes.length > 3000) {
        LOGGER.warn("Cookie too big, it might not be properly set");
      }

      return StringUtils.encodeBase64String(cryptoService.encrypt(bytes));
    }
  }

  @Nullable
  private Serializable uncompressDecryptBase64(final String v)
  {
    if (v != null && !v.isEmpty()) {
      byte[] bytes = StringUtils.decodeBase64String(v);
      if (bytes != null) {
        return deserializeFromBytes(uncompress(cryptoService.decrypt(bytes)));
      }
    }
    return null;
  }

  private byte[] compress(final byte[] data)
  {
    try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(data.length)) {
      try (GZIPOutputStream gzip = new GZIPOutputStream(byteStream)) {
        gzip.write(data);
      }
      return byteStream.toByteArray();
    }
    catch (IOException ex) {
      throw new RuntimeException("Compression failed", ex);
    }
  }

  private byte[] uncompress(final byte[] data)
  {
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
         GZIPInputStream gzip = new GZIPInputStream(inputStream)) {
      return ByteStreams.toByteArray(gzip);
    }
    catch (IOException ex) {
      throw new RuntimeException("Decompression failed", ex);
    }
  }

  /**
   * Serialize object using standard Java serialization
   */
  private byte[] serializeToBytes(Serializable obj)
  {
    Preconditions.checkNotNull(obj, "Object to serialize cannot be null");
    
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
  private Serializable deserializeFromBytes(byte[] data)
  {
    Preconditions.checkNotNull(data, "Data to deserialize cannot be null");
    
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
         ObjectInputStream ois = new ObjectInputStream(bais)) {
      return (Serializable) ois.readObject();
    }
    catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize object", e);
    }
  }

  /**
   * Clear sensitive data from user profiles before storing in cookies
   */
  private Object clearUserProfile(final Object value)
  {
    if (value instanceof Map<?, ?>) {
      final Map<String, CommonProfile> profiles = (Map<String, CommonProfile>) value;
      profiles.forEach((name, profile) -> {
        // In pac4j 5.x, we need to manually clear sensitive data
        // since removeLoginData() is no longer available
        if (profile != null) {
          profile.removeAttribute("access_token");
          profile.removeAttribute("refresh_token");
          profile.removeAttribute("id_token");
          profile.removeAttribute("credentials");
        }
      });
      return profiles;
    } else if (value instanceof CommonProfile) {
      final CommonProfile profile = (CommonProfile) value;
      profile.removeAttribute("access_token");
      profile.removeAttribute("refresh_token");
      profile.removeAttribute("id_token");
      profile.removeAttribute("credentials");
      return profile;
    }
    return value;
  }

  /**
   * Get cookie from request - replacement for ContextHelper.getCookie
   */
  private Cookie getCookie(WebContext context, String name)
  {
    if (context instanceof JEEContext) {
      JEEContext jeeContext = (JEEContext) context;
      HttpServletRequest request = jeeContext.getNativeRequest();
      Cookie[] cookies = request.getCookies();
      if (cookies != null) {
        for (Cookie cookie : cookies) {
          if (name.equals(cookie.getName())) {
            return cookie;
          }
        }
      }
    }
    return null;
  }

  /**
   * Check if connection is secure - replacement for ContextHelper.isHttpsOrSecure
   */
  private boolean isHttpsOrSecure(WebContext context)
  {
    if (context instanceof JEEContext) {
      JEEContext jeeContext = (JEEContext) context;
      HttpServletRequest request = jeeContext.getNativeRequest();
      return request.isSecure() || 
             "https".equalsIgnoreCase(request.getScheme()) ||
             "https".equalsIgnoreCase(request.getHeader("X-Forwarded-Proto"));
    }
    return false;
  }
}
