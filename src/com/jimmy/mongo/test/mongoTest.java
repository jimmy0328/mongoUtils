package com.jimmy.mongo.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jimmy.mongo.core.MongoRunner;
import com.mongodb.BasicDBObject;

public class mongoTest {

	MongoRunner mr = new MongoRunner();
	static String table = "da_file";

	public void query() {

		Map keyMap = new HashMap();
		keyMap.put("user", "jimmy1");
		keyMap.put("fileName", "test1");
		// List<Map> list = mr.query(table, keyMap);
		List<Map> list = mr.queryAll(table);
		for (Map m : list) {
			System.out.println(m);
		}
	}

	public void insertOrUpdateTest() {
		String fname = "test6";
		Map updateQuery = new HashMap();
		updateQuery.put("fileName", fname);

		Map newObj = new HashMap();
		newObj.put("fileName", fname);
		newObj.put("date", "20130207");
		newObj.put("time", "112705");
		newObj.put("user", "ffff");
		newObj.put("id", "555");

		mr.insertOrUpdate(table, updateQuery, newObj);
	}

	public void insertOrUpdate() {
		String fname = "test4";
		Map newDocument = new HashMap();
		newDocument.put("fileName", fname);
		boolean isNew = true;
		if (mr.query(table, newDocument).size() > 0) {
			isNew = false;
		}
		newDocument.put("date", "20130207");
		newDocument.put("time", "112705");
		newDocument.put("user", "jimmy2");
		newDocument.put("id", "22222");
		System.out.println("isNew::" + isNew);
		if (isNew) {
			mr.insert(table, newDocument);
		} else {
			mr.update(table, new BasicDBObject().append("fileName", fname),
					newDocument);
		}

	}

	public void deleteAll() {
		// mr.delete("", "");
		Map d = new HashMap();
		d.put("fileName", "DLGO62.gul");
		mr.delete(table, d);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		mongoTest test = new mongoTest();
		// test.insert();

		// test.insertOrUpdateTest();
		// test.deleteAll();
		test.query();

	}
}
