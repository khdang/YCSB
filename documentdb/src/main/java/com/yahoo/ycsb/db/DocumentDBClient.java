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

  private DocumentClient client;

  private static String databaseForTest = "testdb";
//  private static String collectionForTest = "usertable";
  private static final boolean IS_NAME_BASED = true;

  private static final String DATABASES_PATH_SEGMENT = "dbs";
  private static final String COLLECTIONS_PATH_SEGMENT = "colls";
  private static final String DOCUMENTS_PATH_SEGMENT = "docs";
  private static final String DEFAULT_ENDPONT = "https://localhost:443/";
  private static final String DEFAULT_PRIMARYKEY =
          "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
  private static final boolean DEFAULT_DEBUG = true;

  private static final Logger LOGGER = Logger.getLogger(DocumentDBClient.class);

  private String endpoint = "https://localhost:443/";

  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("documentdb.debug", String.valueOf(DEFAULT_DEBUG));

    if (null != debug && "true".equalsIgnoreCase(debug)) {
      LOGGER.setLevel(Level.DEBUG);
    }

    String primaryKey = getProperties().getProperty("documentdb.primaryKey", DEFAULT_PRIMARYKEY);
    String configuredEndpoint = getProperties().getProperty("documentdb.host", DEFAULT_ENDPONT);

    if (null == primaryKey || primaryKey.length() < 1) {
      throw new DBException("Missing primary key attribute name, cannot continue");
    }

    if (null != configuredEndpoint) {
      this.endpoint = configuredEndpoint;
    }

    try {
      this.client = new DocumentClient(this.endpoint, primaryKey, new ConnectionPolicy(), ConsistencyLevel.Session);
      LOGGER.info("DocumentDB connection created with " + this.endpoint);
    } catch (Exception e1) {
      LOGGER.error("DocumentDBClient.init(): Could not initialize DocumentDB client.", e1);
    }

//        // Create the database for test.
//        Database databaseDefinition = new Database();
//        databaseDefinition.setId(databaseForTest);
//        try {
//            Database db = this.client.createDatabase(databaseDefinition, null).getResource();
//        } catch (DocumentClientException ex) {
//            LOGGER.error("DocumentDBClient.init(): Could not create database for test", ex);
//        }
//
//        // Create the collection for test.
//        DocumentCollection collectionDefinition = new DocumentCollection();
//        collectionDefinition.setId(collectionForTest);
//
//        try {
//            DocumentCollection collection = this.client
//                .createCollection(getDatabaseNameLink(this.databaseForTest), collectionDefinition, null)
//                .getResource();
//        } catch (DocumentClientException ex) {
//            LOGGER.error("DocumentDBClient.init(): Could not create collection for test", ex);
//        }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("readkey: " + key + " from table: " + table);
    }

    String documentLink = getDocumentLink(table, key);
    Document document = null;

    try {
      document = this.client.readDocument(documentLink, null).getResource();
    } catch (DocumentClientException ex) {
      LOGGER.error(ex);
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
            null).getQueryIterable().toList();

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
      document = this.client.readDocument(documentLink, null).getResource();
    } catch (DocumentClientException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    }

    if (null != document) {
      for (Entry<String, ByteIterator> entry : values.entrySet()) {
        document.set(entry.getKey(), entry.getValue().toString());
      }

      try {
        this.client.replaceDocument(document, null);
      } catch (DocumentClientException ex) {
        LOGGER.error(ex);
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
      Document document = this.client.createDocument(getDocumentCollectionLink(table),
              documentDefinition,
              null,
              true).getResource();
    } catch (DocumentClientException ex) {
      LOGGER.error(ex);
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
      this.client.deleteDocument(getDocumentLink(table, key), null);
    } catch (DocumentClientException ex) {
      LOGGER.error(ex);
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

  public static String getDatabaseNameLink(String databaseId) {
    return DATABASES_PATH_SEGMENT + "/" + databaseId;
  }

  public static String getDocumentCollectionLink(String table) {
    return String.format("%s/%s/%s/%s",
            DATABASES_PATH_SEGMENT,
            databaseForTest,
            COLLECTIONS_PATH_SEGMENT,
            table);
  }

  public static String getDocumentLink(String table, String key) {
    return String.format("%s/%s/%s",
            getDocumentCollectionLink(table),
            DOCUMENTS_PATH_SEGMENT,
            key);
  }
}
