package com.jimmy.mongo.test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.jimmy.mongo.gfs.GFSUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class GFSUtilsTest {

	public void insert() {
		String path = "D:\\架構說明.pptx";
		File file = new File(path);
		try {
			GFSUtils.insert(file);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void findOne() {
		try {
			String context = new GFSUtils().retriveContextByName("GFSUtils");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public String getFileContext(String fileName) throws IOException {
		return GFSUtils.retriveContextByName(fileName);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		GFSUtilsTest test = new GFSUtilsTest();

		System.out.println("---------------------------------------");
		try {
			// System.out.println(GFSUtils.findById("512347c859db0d7f60846771"));
			DBObject obj = new BasicDBObject();
			obj.put("applyId", "1361242515571");
			BasicDBObject query = new BasicDBObject("metadata.applyId",
					"1361242515571");
			System.out.println(query);
			DBObject dbo = GFSUtils.get().findOne(query);
			System.out.println(dbo);
//			String id = "" + dbo.get("_id");
//			System.out.println(GFSUtils.retriveContextById(id));

			// Iterator<String> it = dbo.keySet().iterator();
			//
			// while(it.hasNext()){
			// System.out.println(it.next());
			// }

		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			List<Map> l = GFSUtils.findByName("aaaa.gul");
			// List l = GFSUtils.listFiles();
			for (int i = 0; i < l.size(); i++) {
				Map m = (Map) l.get(i);
				// System.out.println(m);
				// System.out.println(GFSUtils.retriveContextById(""+m.get("_id")));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}