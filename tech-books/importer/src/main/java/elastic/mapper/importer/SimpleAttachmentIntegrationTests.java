package elastic.mapper.importer;

//package org.elasticsearch.plugin.mapper.attachments.test;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.node.NodeBuilder.*;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;

import java.util.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class SimpleAttachmentIntegrationTests {

	private final ESLogger logger = Loggers.getLogger(getClass());

	private Node node;

	@BeforeClass
	public void setupServer() {
		// node = nodeBuilder().local(true).settings(settingsBuilder()
		// .put("cluster.name", "test-cluster-" +
		// NetworkUtils.getLocalAddress())
		// .put("gateway.type", "none")).node();
		Node node = nodeBuilder().settings(Settings.settingsBuilder().put("path.home", "/Users/huazhang/elk-data"))
				.local(true).node();
	}

	@AfterClass
	public void closeServer() {
		node.close();
	}

	// http://mvnrepository.com/artifact/org.testng/testng/6.9.9
	@BeforeMethod
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

	@BeforeMethod
	public void createIndex() {
		logger.info("creating index [accountindex]");
		// node.client().admin().indices().create(createIndexRequest("accountindex").settings(Settings.settingsBuilder().put("index.numberOfReplicas",
		// 0))).actionGet();

		node.client().prepareIndex("accountindex", "article", "1")
				.setSource(putJsonDocument("ElasticSearch: Java API",
						"ElasticSearch provides the Java API, all operations "
								+ "can be executed asynchronously using a client object.",
						new Date(), new String[] { "elasticsearch" }, "Hüseyin Akdoğan"))
				.execute().actionGet();
		node.close();

		logger.info("Running Cluster Health");
		ClusterHealthResponse clusterHealth = node.client().admin().cluster()
				.health(clusterHealthRequest().waitForGreenStatus()).actionGet();
		logger.info("Done Cluster Health, status " + clusterHealth.status());
		// assertThat(clusterHealth.timedOut(), equalTo(false));
		// assertThat(clusterHealth.status(),
		// equalTo(ClusterHealthStatus.GREEN));
	}

	@AfterMethod
	public void deleteIndex() {
		logger.info("deleting index [accountindex]");
		node.client().admin().indices().delete(deleteIndexRequest("accountindex")).actionGet();
	}

	@Test
	public void testSimpleAttachment() throws Exception {
		String mapping = copyToStringFromClasspath(
				"/Users/huazhang/git/elasticsearch-mapper-example/importer/src/main/resources/test-mapping.json");
		byte[] html = copyToBytesFromClasspath(
				"/Users/huazhang/git/elasticsearch-mapper-example/importer/src/main/resources/testXHTML.html");

		node.client().admin().indices().putMapping(putMappingRequest("accountindex").type("person").source(mapping))
				.actionGet();

		node.client().index(
				indexRequest("test").type("person").source(jsonBuilder().startObject().field("file", html).endObject()))
				.actionGet();
		node.client().admin().indices().refresh(refreshRequest()).actionGet();

		// CountResponse countResponse =
		// node.client().count(countRequest("test").query( "test
		// document")).actionGet();
		// CountResponse countResponse =
		// node.client().count(countRequest("test").query(fieldQuery("file.title",
		// "test document"))).actionGet();
		// assertThat(countResponse.count(), equalTo(1l));

		// countResponse =
		// node.client().count(countRequest("test")..actionGet();
		// countResponse =
		// node.client().count(countRequest("test").query(fieldQuery("file",
		// "tests the ability"))).actionGet();
		// assertThat(countResponse.count(), equalTo(1l));
	}
}