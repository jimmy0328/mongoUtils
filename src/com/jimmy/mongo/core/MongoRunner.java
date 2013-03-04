package com.jimmy.mongo.core;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

public class MongoRunner {

	/**
	 * retrieve All the Collections Name
	 * 
	 * @return List
	 * @throws UnknownHostException
	 */
	public List<String> getCollections() throws UnknownHostException {
		Set<String> colls = DataSource.getInstance().getConnect()
				.getCollectionNames();
		List<String> collectionList = new ArrayList<String>();
		for (String table : colls) {
			collectionList.add(table);
		}
		return collectionList;
	}

	public static DB getConnect() throws UnknownHostException {
		return DataSource.getInstance().getConnect();
	}

	/**
	 * retrieve the specified Table Data
	 * 
	 * @param TableName
	 * @return
	 */
	public DBCollection getCollection(String TableName) {
		try {
			return DataSource.getInstance().getConnect()
					.getCollection(TableName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * find All data By condition
	 * 
	 * @param TableName
	 * @param keyMap
	 * @return
	 */
	private DBCursor queryByKey(String TableName, Map keyMap) {
		BasicDBObject ref = new BasicDBObject();
		ref.putAll(keyMap);
		return getCollection(TableName).find(ref);
	}

	/**
	 * find All data
	 * 
	 * @param TableName
	 * @return
	 */
	public List queryAll(String TableName) {
		DBCursor dbCursor = getCollection(TableName).find();
		List<Map> list = new ArrayList<Map>();
		while (dbCursor.hasNext()) {
			DBObject obj = dbCursor.next();
			list.add(obj.toMap());
		}
		return list;
	}

	/**
	 * find By condition
	 * 
	 * @param TableName
	 * @param keyMap
	 *            Map<key,value>
	 * @return
	 */
	public List query(String TableName, Map keyMap) {
		return DBCursorToList(queryByKey(TableName, keyMap));
	}

	private List DBCursorToList(DBCursor dbCursor) {
		List<Map> list = new ArrayList<Map>();
		while (dbCursor.hasNext()) {
			DBObject obj = dbCursor.next();
			list.add(obj.toMap());
		}
		return list;
	}

	public boolean insert(String TableName, Map info) {
		WriteResult wr = getCollection(TableName).insert(
				new BasicDBObject(info));
		if (wr.getN() > 0) {
			return true;
		}
		return false;
	}

	public void delete(String TableName, Map info) {
		BasicDBObject delObj = new BasicDBObject();
		delObj.putAll(info);
		getCollection(TableName).remove(delObj);
	}

	public void update(String TableName, Map updateQuery, Map object) {
		BasicDBObject newObj = new BasicDBObject();
		BasicDBObject oldObj = new BasicDBObject();
		newObj.putAll(object);
		oldObj.putAll(updateQuery);
		getCollection(TableName).update(oldObj, newObj);
	}

	public boolean insertOrUpdate(String TableName, Map updateQuery, Map object) {
		try {
			if (query(TableName, updateQuery).size() > 0) {
				update(TableName, updateQuery, object);
			} else {
				insert(TableName, object);
			}
		} catch (Exception e) {
			return false;
		}
		return true;

	}

}
