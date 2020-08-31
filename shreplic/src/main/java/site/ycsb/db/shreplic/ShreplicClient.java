/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.shreplic;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import go.ycsb.Ycsb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Shreplic binding.
 */
public class ShreplicClient extends DB {

  private static final int TIMEOUT = Integer.MAX_VALUE; // in ms.

  private ExecutorService executorService;
  private Ycsb.ShreplicClient client;
  private boolean verbose;
  private boolean hasFailed;

  public ShreplicClient() {
  }

  @Override
  public void init() throws DBException {
    verbose = false;
    hasFailed = false;
    executorService = Executors.newFixedThreadPool(1);

    boolean leaderless = false;
    if ("true".equals(getProperties().getProperty("leaderless"))) {
      leaderless = true;
    }

    boolean fast = false;
    if ("true".equals(getProperties().getProperty("fast"))) {
      fast = true;
    }

    verbose = false;
    if ("true".equals(getProperties().getProperty("v"))) {
      verbose = true;
    }

    boolean localreads = false;
    if ("true".equals(getProperties().getProperty("localreads"))) {
      localreads = true;
    }

    String type = "base";
    if (getProperties().containsKey("type")) {
      type = getProperties().getProperty("type");
    }

    String maddr = "localhost";
    if (getProperties().containsKey("maddr")) {
      type = getProperties().getProperty("type");
    }

    int mport = 7087;
    if (getProperties().containsKey("mport")) {
      mport = Integer.parseInt(getProperties().getProperty("mport"));
    }

    String server = "";
    if (getProperties().containsKey("server")) {
      server = getProperties().getProperty("server");
    }

    String args = "";
    if (getProperties().containsKey("args")) {
      args = getProperties().getProperty("args");
    }

    client = Ycsb.NewShreplicClient(type, maddr, server, mport, fast, localreads, leaderless, verbose, args);

    try {
      client.Connect();
    } catch (Exception e) {
      e.printStackTrace();
      throw new DBException("cannot connect to DB");
    }
  }

  @Override
  public void cleanup() {
    client.Disconnect();
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (hasFailed) {
      return Status.ERROR;
    }
    try {

      byte[] data = marshal(StringByteIterator.getStringMap(values));

      CompletableFuture.runAsync(() -> client.Write(hash(key), data),
                                 executorService).get(TIMEOUT, TimeUnit.MILLISECONDS);

      if (verbose) {
        System.out.println("INSERT: " + key + " -> " + values);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      hasFailed = true;
      client.Disconnect();
    }
    return Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (hasFailed) {
      return Status.ERROR;
    }
    try {
      result = new HashMap<>();
      final byte[][] data = new byte[1][1];

      CompletableFuture.runAsync(() -> data[0] = client.Read(hash(key)),
                                 executorService).get(TIMEOUT, TimeUnit.MILLISECONDS);

      if (data[0] != null) {
        StringByteIterator.putAllAsByteIterators(result, unmarshal(data[0]));
      }

      if (verbose) {
        System.out.println("READ: " + key + " -> " + result);
      }
      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
      hasFailed = true;
      client.Disconnect();
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    if (hasFailed) {
      return Status.ERROR;
    }

    try {
      result = new Vector<>();
      HashMap<String, ByteIterator> item = new HashMap<>();
      final byte[][] data = new byte[1][1];

      CompletableFuture.runAsync(() -> data[0] = client.Scan(hash(startkey), recordcount),
                                 executorService).get(TIMEOUT, TimeUnit.MILLISECONDS);

      if (data[0] != null) {
        StringByteIterator.putAllAsByteIterators(item, unmarshal(data[0]));
        result.add(item);
      }

      if (verbose) {
        System.out.println("SCAN: " + startkey + "[0-" + recordcount + "] -> " + result.toString());
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      hasFailed = true;
      client.Disconnect();
    }
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (hasFailed) {
      return Status.ERROR;
    }
    try {
      byte[] data = marshal(StringByteIterator.getStringMap(values));

      CompletableFuture.runAsync(() -> client.Write(hash(key), data),
                                 executorService).get(TIMEOUT, TimeUnit.MILLISECONDS);

      if (verbose) {
        System.out.println("UPDATE: " + key + " -> " + values);
      }
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      hasFailed = true;
      client.Disconnect();
    }
    return Status.ERROR;

  }


  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  // adapted from String.hashCode()
  public static long hash(String string) {
    long h = 1125899906842597L; // prime
    int len = string.length();

    for (int i = 0; i < len; i++) {
      h = 31 * h + string.charAt(i);
    }
    return h;
  }

  private static byte[] marshal(Map<String, String> map) throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(byteOut);
    out.writeObject(map);
    return byteOut.toByteArray();
  }

  private static Map<String, String> unmarshal(byte[] data) throws IOException, ClassNotFoundException {
    ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
    ObjectInputStream in = new ObjectInputStream(byteIn);
    return (Map<String, String>) in.readObject();
  }
}
