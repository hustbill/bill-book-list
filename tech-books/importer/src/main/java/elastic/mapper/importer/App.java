package elastic.mapper.importer;



/**
 * Using Elasticsearch Mapper Plugin
 *
 */

import java.util.*;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import static org.elasticsearch.node.NodeBuilder.*;


public class App {
	public static void main(String[] args) {
		System.out.println("Connect to Elasticsearch!");

		// ref: https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/node-client.html

		// https://github.com/elastic/elasticsearch/issues/13155
		Node node = nodeBuilder()
				.settings(Settings.settingsBuilder()
						.put("path.home", "/Users/huazhang/elk-data")).local(true).node();
		Client client = node.client();

		client.prepareIndex("kodcucom", "article", "1")
		              .setSource(putJsonDocument("ElasticSearch: Java API",
		                                         "ElasticSearch provides the Java API, all operations "
		                                         + "can be executed asynchronously using a client object.",
		                                         new Date(),
		                                         new String[]{"elasticsearch"},
		                                         "Hüseyin Akdoğan")).execute().actionGet();
     

		GetResponse getResponse = client.prepareGet("kodcucom", "article", "1").execute().actionGet();
		Map<String, Object> source = getResponse.getSource();
		System.out.println("------------------------------");
		System.out.println("Index: " + getResponse.getIndex());
		System.out.println("Type: " + getResponse.getType());
		System.out.println("Id: " + getResponse.getId());
		System.out.println("Version: " + getResponse.getVersion());
		System.out.println(source);
		System.out.println("------------------------------");
		
		node.close();

	}

	//fieldQuery(field, value)

	public static void searchDocument(Client client, String index, String type, String field, String value) {
		SearchResponse response = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_AND_FETCH)
				.setQuery("hello").setFrom(0).setSize(60).setExplain(true).execute().actionGet();
		SearchHit[] results = response.getHits().getHits();
		System.out.println("Current results: " + results.length);
		for (SearchHit hit : results) {
			System.out.println("------------------------------");
			Map<String, Object> result = hit.getSource();
			System.out.println(result);
		}
	}

	// Creating index
	public static Map<String, Object> putJsonDocument(String title, String content, Date postDate, String[] tags,
			String author) {
		Map<String, Object> jsonDocument = new HashMap<String, Object>();
		jsonDocument.put("title", title);
		jsonDocument.put("conten", content);
		jsonDocument.put("postDate", postDate);
		jsonDocument.put("tags", tags);
		jsonDocument.put("author", author);
		return jsonDocument;
	}

	// Updating index
	public static void updateDocument(Client client, String index, String type, String id, String field,
			String newValue) {
		Map<String, Object> updateObject = new HashMap<String, Object>();
		updateObject.put(field, newValue);
//		client.prepareUpdate(index, type, id).setScript("ctx._source." + field + "=" + field)
//				.setScriptParams(updateObject).execute().actionGet();
	}
	
	// Deleting index
	public static void deleteDocument(Client client, String index, String type, String id){
        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        System.out.println("Information on the deleted document:");
        System.out.println("Index: " + response.getIndex());
        System.out.println("Type: " + response.getType());
        System.out.println("Id: " + response.getId());
        System.out.println("Version: " + response.getVersion());
    }
}
