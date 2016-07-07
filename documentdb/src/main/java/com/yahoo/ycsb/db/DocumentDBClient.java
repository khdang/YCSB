/*
 * Copyright 2016 Microsoft.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.yahoo.ycsb.db;

import com.microsoft.azure.documentdb.*;
import com.yahoo.ycsb.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

/**
 * DocumentDB v1.00 client for YCSB.
 */

public class DocumentDBClient extends DB {

  private static final String DATABASES_PATH_SEGMENT = "dbs";
  private static final String COLLECTIONS_PATH_SEGMENT = "colls";
  private static final String DOCUMENTS_PATH_SEGMENT = "docs";
  private static final Logger LOGGER = Logger.getLogger(DocumentDBClient.class);

  private DocumentClient client;
  private String databaseForTest = "testdb";
  private boolean useSinglePartitionCollection = false;
  private boolean useUpsert = false;

  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("documentdb.debug", null);
    if (null != debug && "true".equalsIgnoreCase(debug)) {
      LOGGER.setLevel(Level.DEBUG);
    }

    String masterKey = getProperties().getProperty("documentdb.masterKey", null);
    String host = getProperties().getProperty("documentdb.host", null);

    if (null == masterKey || masterKey.length() < 1) {
      throw new DBException("Missing primary key attribute name, cannot continue");
    }

    String partitionedCollectionUsage = getProperties().getProperty("documentdb.useSinglePartitionCollection", null);
    if (null != partitionedCollectionUsage && "true".equalsIgnoreCase(partitionedCollectionUsage)) {
      useSinglePartitionCollection = true;
    }

    String useUpsertStr = getProperties().getProperty("documentdb.useUpsert", null);
    if (null != useUpsertStr && "true".equalsIgnoreCase(useUpsertStr)) {
      useUpsert = true;
    }

    String dbForTest = getProperties().getProperty("documentdb.databaseForTest", null);
    if (null != dbForTest && dbForTest.length() > 1) {
      this.databaseForTest = dbForTest;
    }

    try {
      this.client = new DocumentClient(host, masterKey, new ConnectionPolicy(), ConsistencyLevel.Session);
      LOGGER.info("DocumentDB connection created with " + host);
    } catch (Exception e) {
      LOGGER.error("DocumentDBClient.init(): Could not initialize DocumentDB client.", e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("readkey: " + key + " from table: " + table);
    }

    String documentLink = getDocumentLink(table, key);
    Document document = null;

    try {
      document = this.client.readDocument(documentLink, getRequestOptions(key)).getResource();
    } catch (DocumentClientException e) {
      LOGGER.error(e);
      return Status.ERROR;
    }

    if (null != document) {
      result.putAll(extractResult(document));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Result: " + document.toString());
      }
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
    }

    List<Document> documents = this.client.queryDocuments(getDocumentCollectionLink(table),
            new SqlQuerySpec("SELECT TOP @recordcount * FROM root r WHERE r.id >= @startkey",
                    new SqlParameterCollection(
                      new SqlParameter("@recordcount", recordcount),
                      new SqlParameter("@startkey", startkey))),
            getFeedOptions()).getQueryIterable().toList();

    for (Document document : documents) {
      result.add(extractResult(document));
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("updatekey: " + key + " from table: " + table);
    }

    String documentLink = getDocumentLink(table, key);
    Document document = null;

    try {
      document = this.client.readDocument(documentLink, getRequestOptions(key)).getResource();
    } catch (DocumentClientException e) {
      LOGGER.error(e);
      return Status.ERROR;
    }

    if (null != document) {
      for (Entry<String, ByteIterator> entry : values.entrySet()) {
        document.set(entry.getKey(), entry.getValue().toString());
      }

      try {
        this.client.replaceDocument(document, getRequestOptions(key));
      } catch (DocumentClientException e) {
        LOGGER.error(e);
        return Status.ERROR;
      }
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("insertkey: " + key + " from table: " + table);
    }

    Document documentDefinition = new Document(
            "{" +
                    "  'id': '" + key + "', " +
                    "  'name': 'sample document'," +
                    "  'foo': 'bar Ã¤Â½Â Ã¥Â¥Â½'," +  // foo contains some UTF-8 characters.
                    "  'key': 'value'" +
                    "}");
    try {
      if (useUpsert) {
        Document document = this.client.upsertDocument(getDocumentCollectionLink(table),
          documentDefinition,
          getRequestOptions(key),
          true).getResource();
      } else {
        Document document = this.client.createDocument(getDocumentCollectionLink(table),
          documentDefinition,
          getRequestOptions(key),
          true).getResource();
      }
    } catch (DocumentClientException e) {
      LOGGER.error(e);
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("deletekey: " + key + " from table: " + table);
    }

    try {
      this.client.deleteDocument(getDocumentLink(table, key), getRequestOptions(key));
    } catch (DocumentClientException e) {
      LOGGER.error(e);
      return Status.ERROR;
    }

    return Status.OK;
  }

  private HashMap<String, ByteIterator> extractResult(Document item) {
    if (null == item) {
      return null;
    }
    HashMap<String, ByteIterator> rItems = new HashMap<>(item.getHashMap().size());

    for (Entry<String, Object> attr : item.getHashMap().entrySet()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format("Result- key: %s, value: %s", attr.getKey(), attr.getValue().toString()));
      }
      rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().toString()));
    }
    return rItems;
  }

  private FeedOptions getFeedOptions() {
    if (useSinglePartitionCollection) {
      return null;
    }
    FeedOptions feedOptions = new FeedOptions();
    feedOptions.setEnableCrossPartitionQuery(true);
    return feedOptions;
  }

  private RequestOptions getRequestOptions(String partitionKey) {
    if (useSinglePartitionCollection) {
      return null;
    }
    RequestOptions requestOptions = new RequestOptions();
    requestOptions.setPartitionKey(new PartitionKey(partitionKey));
    return requestOptions;
  }

  private String getDocumentCollectionLink(String table) {
    return String.format("%s/%s/%s/%s",
            DATABASES_PATH_SEGMENT,
            this.databaseForTest,
            COLLECTIONS_PATH_SEGMENT,
            table);
  }

  private String getDocumentLink(String table, String key) {
    return String.format("%s/%s/%s",
            getDocumentCollectionLink(table),
            DOCUMENTS_PATH_SEGMENT,
            key);
  }
}
