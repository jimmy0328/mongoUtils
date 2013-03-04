package com.jimmy.mongo.gfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;

import com.jimmy.mongo.core.MongoRunner;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class GFSUtils implements GFSObject {

	private static String GridFSTable = "fs";

	public static GridFS gfs = null;

	public static GridFS get() throws IOException {

		if (null == gfs) {
			config();
			gfs = new GridFS(MongoRunner.getConnect(), GridFSTable);
			System.out.println(" get connect !!");
		}

		return gfs;
	}

	private static void config() throws IOException {
		System.out.println("Datasource Config!");
		Properties prop = new Properties();
		GridFSTable = prop.getProperty("gfsName");
	}

	/**
	 * 寫入檔案到Mongo
	 * 
	 * @param soruce
	 * @throws IOException
	 */
	public static void insert(File soruce) throws IOException {
		insert(soruce, null);
	}

	/**
	 * 寫入檔案到Mongo
	 * 
	 * @param soruce
	 * @throws IOException
	 */
	public static void insert(File soruce, String newName, Map m)
			throws IOException {
		DBObject obj = new BasicDBObject();
		obj.putAll(m);
		insert(soruce, newName, "file", obj);
	}

	/**
	 * 寫入檔案
	 * 
	 * @param filePath
	 *            檔案路徑
	 * @throws IOException
	 */
	public static void insert(String filePath) throws IOException {
		File file = new File(filePath);
		insert(file);
	}

	/**
	 * 寫入檔案到Mongo
	 * 
	 * @param soruce
	 * @throws IOException
	 */
	public static void insert(File srcDir, String newName) throws IOException {
		insert(srcDir, newName, "file", null);
	}

	public static void insertOrUpdate(File srcDir, String fileName)
			throws IOException {

		List<GridFSDBFile> fileList = find(fileName);

		if (fileList.size() == 0) {
			insert(srcDir, null);
			return;
		}

		for (GridFSDBFile f : fileList) {
			ObjectId id = (ObjectId) f.get(GFSObject.ID);
			insert(srcDir, null);
			deleteById(id);
		}

	}

	/**
	 * 寫入檔案到Mongo
	 * 
	 * @param soruce
	 * @throws IOException
	 */
	public static void insert(File srcDir, String newName, String defaultType,
			DBObject metadata) throws IOException {

		GridFSInputFile gfsFile = GFSUtils.get().createFile(srcDir);
		if (null == newName) {
			newName = srcDir.getName();
		}
		gfsFile.setFilename(newName);

		if (null != defaultType) {
			gfsFile.setContentType(defaultType);
		}
		if (null != metadata) {
			gfsFile.setMetaData(metadata);
		}

		gfsFile.save();

	}

	/**
	 * 找出Mongo中的檔案
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static GridFSDBFile findOne(String fileName) throws IOException {
		return GFSUtils.get().findOne(fileName);
	}

	public static List<GridFSDBFile> find(String fileName) throws IOException {
		return GFSUtils.get().find(fileName);
	}

	public static List<GridFSDBFile> findByObject(DBObject query)
			throws IOException {
		return GFSUtils.get().find(query);
	}

	public static List<Map> listFiles() throws IOException {
		List<DBObject> files = GFSUtils.list();
		List<Map> fileList = new ArrayList<Map>();
		for (DBObject obj : files) {
			Map m = new HashMap();
			m.put(ID, obj.get(ID));
			m.put(LENGTH, obj.get(LENGTH));
			m.put(FILENAME, obj.get(FILENAME));
			m.put(CONTENT_TYPE, obj.get(CONTENT_TYPE));
			m.put(UPLOAD_DATE, obj.get(UPLOAD_DATE));
			fileList.add(m);
		}
		return fileList;

	}

	private static List<DBObject> list() {
		DBCursor cursor = null;
		try {
			cursor = GFSUtils.get().getFileList();
		} catch (Exception e) {
			e.printStackTrace();
		}
		List<DBObject> fileList = new ArrayList<DBObject>();
		while (cursor.hasNext()) {
			fileList.add((DBObject) cursor.next());
		}
		return fileList;
	}

	/**
	 * 依檔名找出所有檔案
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static List<Map> findByName(String fileName) throws IOException {

		List<GridFSDBFile> list = GFSUtils.get().find(fileName);
		List<Map> fileList = new ArrayList<Map>();
		for (DBObject obj : list) {
			Map m = new HashMap();
			m.put(ID, obj.get(ID));
			m.put(LENGTH, obj.get(LENGTH));
			m.put(FILENAME, obj.get(FILENAME));
			m.put(CONTENT_TYPE, obj.get(CONTENT_TYPE));
			m.put(UPLOAD_DATE, obj.get(UPLOAD_DATE));
			m.put(METADATA, obj.get(METADATA));
			fileList.add(m);
		}
		return fileList;
	}

	/**
	 * 透過metadata裡的欄位去找出一筆資料
	 * 
	 * @param key
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static DBObject findOneByMetadata(String key, String value)
			throws IOException {
		BasicDBObject query = new BasicDBObject("metadata." + key, value);
		return GFSUtils.get().findOne(query);
	}

	/**
	 * 透過metadata裡的欄位去找出一筆資料的內容
	 * 
	 * @param key
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static String retriveContextByMetadata(String key, String value)
			throws IOException {
		DBObject obj = findOneByMetadata(key, value);
		if (null == obj) {
			return "";
		}
		return retriveContextById("" + obj.get(ID));
	}

	/**
	 * 依檔名找出第一筆
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static GridFSDBFile findOneByName(String fileName)
			throws IOException {
		return GFSUtils.get().findOne(fileName);
	}

	/**
	 * 取出檔案內容
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static String retriveContextByName(String fileName)
			throws IOException {
		GridFSDBFile file = (GridFSDBFile) findOneByName(fileName);
		if (file == null) {
			return "";
		}
		return IOUtils.toString(file.getInputStream(), "UTF-8");
	}

	public static String retriveContextById(String id) throws IOException {
		GridFSDBFile file = (GridFSDBFile) findById(id);
		if (file == null) {
			return "";
		}
		return IOUtils.toString(file.getInputStream(), "UTF-8");
	}

	/**
	 * 依id找出梢
	 * 
	 * @param id
	 * @throws IOException
	 */
	public static GridFSDBFile findById(String id) throws IOException {
		return GFSUtils.get().find(new ObjectId(id));
	}

	/**
	 * 找出Mongo中的檔案並儲存
	 * 
	 * @param soruce
	 * @throws IOException
	 */
	public static void findAndSave(String fileName, String desDir)
			throws IOException {
		GridFSDBFile gridFile = findOne(fileName);
		if (null == gridFile) {
			System.err.println("# GridFS not find [" + fileName + "] file.");
			return;
		}
		gridFile.writeTo(desDir);
	}

	/**
	 * 刪除檔案
	 * 
	 * @param fileName
	 * @throws IOException
	 */
	public static void delete(String fileName) throws IOException {
		GridFS gfsFile = GFSUtils.get();
		gfsFile.remove(gfsFile.findOne(fileName));
	}

	/**
	 * 使用ID來刪除檔案
	 * 
	 * @param id
	 * @throws IOException
	 */
	public static void deleteById(ObjectId id) throws IOException {
		GFSUtils.get().remove(id);
	}

	/**
	 * 使用ID來刪除檔案
	 * 
	 * @param id
	 * @throws IOException
	 */
	public static void deleteById(String id) throws IOException {
		GFSUtils.get().remove(new ObjectId(id));
	}

}
