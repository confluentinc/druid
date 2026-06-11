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

package org.apache.druid.storage.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class S3UtilsTest
{
  @Test
  public void testRetryWithIOExceptions()
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        IOException.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              count.incrementAndGet();
              throw new IOException("hmm");
            },
            maxRetries
        ));
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWith4XXErrors()
  {
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        IOException.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              if (count.incrementAndGet() >= 2) {
                return "hey";
              } else {
                AmazonS3Exception s3Exception = new AmazonS3Exception("a 403 s3 exception");
                s3Exception.setStatusCode(403);
                throw new IOException(s3Exception);
              }
            },
            3
        ));
    Assert.assertEquals(1, count.get());
  }

  @Test
  public void testRetryWith5XXErrorsNotExceedingMaxRetries() throws Exception
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    S3Utils.retryS3Operation(
        () -> {
          if (count.incrementAndGet() >= maxRetries) {
            return "hey";
          } else {
            AmazonS3Exception s3Exception = new AmazonS3Exception("a 5xx s3 exception");
            s3Exception.setStatusCode(500);
            throw new IOException(s3Exception);
          }
        },
        maxRetries
    );
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWith5XXErrorsExceedingMaxRetries()
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        IOException.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              if (count.incrementAndGet() > maxRetries) {
                return "hey";
              } else {
                AmazonS3Exception s3Exception = new AmazonS3Exception("a 5xx s3 exception");
                s3Exception.setStatusCode(500);
                throw new IOException(s3Exception);
              }
            },
            maxRetries
        )
    );
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWith5XXErrorsWrappedInSdkClientException() throws Exception
  {
    // Mimics a multipart upload part failure: TransferManager wraps the retryable AmazonS3Exception in a
    // generic SdkClientException, leaving the 503 only on the cause chain.
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    S3Utils.retryS3Operation(
        () -> {
          if (count.incrementAndGet() >= maxRetries) {
            return "hey";
          } else {
            AmazonS3Exception s3Exception = new AmazonS3Exception("Service Unavailable");
            s3Exception.setStatusCode(503);
            throw new SdkClientException(
                "Unable to complete multi-part upload. Individual part upload failed: Service Unavailable",
                s3Exception
            );
          }
        },
        maxRetries
    );
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testRetryWithInternalErrorInOkResponse() throws Exception
  {
    // Mimics completeMultipartUpload failing with error code "InternalError" inside a 200 OK response,
    // surfaced wrapped in an IOException.
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    S3Utils.retryS3Operation(
        () -> {
          if (count.incrementAndGet() >= maxRetries) {
            return "hey";
          } else {
            AmazonS3Exception s3Exception =
                new AmazonS3Exception("We encountered an internal error. Please try again.");
            s3Exception.setStatusCode(200);
            s3Exception.setErrorCode("InternalError");
            throw new IOException(s3Exception);
          }
        },
        maxRetries
    );
    Assert.assertEquals(maxRetries, count.get());
  }

  @Test
  public void testNoRetryWith4XXErrorsWrappedInSdkClientException()
  {
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        SdkClientException.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              count.incrementAndGet();
              AmazonS3Exception s3Exception = new AmazonS3Exception("Access Denied");
              s3Exception.setStatusCode(403);
              throw new SdkClientException(
                  "Unable to complete multi-part upload. Individual part upload failed: Access Denied",
                  s3Exception
              );
            },
            3
        )
    );
    Assert.assertEquals(1, count.get());
  }

  @Test
  public void testNoRetryWithInterruptedExceptionWrappedInSdkClientException()
  {
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows(
        SdkClientException.class,
        () -> S3Utils.retryS3Operation(
            () -> {
              count.incrementAndGet();
              throw new SdkClientException("Upload aborted", new InterruptedException());
            },
            3
        )
    );
    Assert.assertEquals(1, count.get());
  }

  @Test
  public void testRetryWithSdkClientException() throws Exception
  {
    final int maxRetries = 3;
    final AtomicInteger count = new AtomicInteger();
    S3Utils.retryS3Operation(
        () -> {
          if (count.incrementAndGet() >= maxRetries) {
            return "hey";
          } else {
            throw new SdkClientException(
                "Unable to find a region via the region provider chain. "
                + "Must provide an explicit region in the builder or setup environment to supply a region."
            );
          }
        },
        maxRetries
    );
    Assert.assertEquals(maxRetries, count.get());
  }
}
