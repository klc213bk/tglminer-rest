package com.transglobe.tglminer.rest.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.transglobe.tglminer.rest.bean.EtlNameBo.ConsumerStatusEnum;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.tglminer.rest.bean.HealthETL;
import com.transglobe.tglminer.rest.bean.HealthTopicEnum;
import com.transglobe.tglminer.rest.bean.TglminerSPEnum;
import com.transglobe.tglminer.rest.bean.TglminerTableEnum;


@Service
public class TglminerService {
	static final Logger LOG = LoggerFactory.getLogger(TglminerService.class);

	@Value("${tglminer.db.driver}")
	private String tglminerDbDriver;

	@Value("${tglminer.db.url}")
	private String tglminerDbUrl;

	@Value("${tglminer.db.username}")
	private String tglminerDbUsername;

	@Value("${tglminer.db.password}")
	private String tglminerDbPassword;

	@Value("${kafka.rest.url}")
	private String kafkaRestUrl;
	
	public void cleanup() throws Exception{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			// drop user SP
			Set<String> spSet = new HashSet<>();
			for (TglminerSPEnum e : TglminerSPEnum.values()) {
				spSet.add(e.getSpName());
			}

			sql = "select OBJECT_NAME from dba_objects where object_type = 'PROCEDURE' and owner = 'TGLMINER'";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				String sp = rs.getString("OBJECT_NAME");
				if (spSet.contains(sp)) {
					executeScript(conn, "DROP PROCEDURE " + sp);
					LOG.info(">>> SP={} dropped", sp);
				}
			}
			rs.close();
			pstmt.close();

			// drop user tables
			Set<String> tbSet = new HashSet<>();
			for (TglminerTableEnum tableEnum : TglminerTableEnum.values()) {
				tbSet.add(tableEnum.getTableName());
			}

			// drop user tables
			sql = "select TABLE_NAME from USER_TABLES";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				String table = rs.getString("TABLE_NAME");
				if (tbSet.contains(table)) {
					executeScript(conn, "DROP TABLE " + table);
					LOG.info(">>> table={} dropped", table); 
				}
			}
			pstmt.close();
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}
	
	}
	public void initialize() throws Exception{
		Connection conn = null;
		
		CallableStatement cstmt = null;
		try {
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);
			conn.setAutoCommit(false);
			
			for (TglminerTableEnum e : TglminerTableEnum.values()) {
				LOG.info(">>>>>>> create TABLE file {}",e.getScriptFile());
				executeSqlScriptFromFile(conn, e.getScriptFile());
			}
			conn.commit();
			
			for (TglminerSPEnum e : TglminerSPEnum.values()) {
				LOG.info(">>>>>>> create SP file {}",e.getScriptFile());
				executeSqlScriptFromFile(conn, e.getScriptFile());
			}
			conn.commit();
			
			// insert data
			LOG.info(">>> insert etl name ");
			cstmt = conn.prepareCall("{call SP_INS_ETL_NAME(?,?,?,?,?)}");

			cstmt.setString(1,  HealthETL.CAT);
			cstmt.setString(2,  HealthETL.NAME);
			cstmt.setInt(3,  0);
			cstmt.setString(4, ConsumerStatusEnum.REGISTERED.name());
			cstmt.setString(5,  HealthETL.NOTE);
			cstmt.execute();
			cstmt.close();
			
			conn.commit();
			
			LOG.info(">>> insert kafka topic");
			insertTopic(conn, HealthETL.NAME, HealthTopicEnum.DDL.getTopic());
			insertTopic(conn, HealthETL.NAME, HealthTopicEnum.HEARTBEAT.getTopic());
			conn.commit();
			
			LOG.info(">>> insert logminer table");
//			deleteLogminerTable(conn, HealthETL.NAME);
			insertLogminerTable(conn, HealthETL.NAME, TglminerTableEnum.TM_HEARTBEAT.getTableName());
			conn.commit();
			
			conn.close();
		} finally {
			if (cstmt != null) cstmt.close();
			if (conn != null) conn.close();
		}
	
	}
	private void insertTopic(Connection conn, String etlName, String topic) throws Exception{
		LOG.info(">>>>>>>>>> insertTopic,{},{}", etlName, topic);

		CallableStatement cstmt = null;
		try {	
			
			Set<String> topicset = getTableKafkaTopics(conn, etlName);
			if (!topicset.contains(topic)) {
				cstmt = conn.prepareCall("{call SP_INS_KAFKA_TOPIC(?,?)}");
				cstmt.setString(1,  etlName);
				cstmt.setString(2,  topic);
				cstmt.execute();
				cstmt.close();

			}
			String response = kafkaRestService(kafkaRestUrl+"/listTopics", "GET");
			ObjectMapper mapper = new ObjectMapper();
			JsonNode jsonNode = mapper.readTree(response);
			String returnCode = jsonNode.get("returnCode").asText();
			String topicStr = jsonNode.get("topics").asText();
			
			List<String> topicList = mapper.readValue(topicStr, new TypeReference<List<String>>() {});
			Set<String> topicSet = new HashSet<>(topicList);
			
			LOG.info(">>>>>>>>>> returnCode={}, topicstr={}", returnCode, topicStr);
			if (topicSet.contains(topic)) {
				kafkaRestService(kafkaRestUrl+"/deleteTopic/" + topic, "POST");
				LOG.info(">>>>>>>>>>>> topic={} deleted ", topic);
			
			}

			kafkaRestService(kafkaRestUrl+"/createTopic/"+topic, "POST");
			LOG.info(">>>>>>>>>>>> topic={} created ", topic);
			
		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (cstmt != null) cstmt.close();
		}
	}
	private String kafkaRestService(String urlStr, String requestMethod) throws Exception {
		LOG.info(">>>>>>>>>>>> kafka service urlStr={}", urlStr);

		HttpURLConnection httpConn = null;
		URL url = null;
		try {
			url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod(requestMethod);
			int responseCode = httpConn.getResponseCode();
			//			LOG.info(">>>>>  responseCode={}",responseCode);

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
			StringBuffer response = new StringBuffer();
			String readLine = null;
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> sendHeartbeat responsecode={}, response={}", responseCode, response.toString());

			return response.toString();
		} finally {
			if (httpConn != null ) httpConn.disconnect();
		}
	}
	private void insertLogminerTable(Connection conn, String etlName, String tableName) throws Exception{
		LOG.info(">>>>>>>>>> insertLogminerTable");
		PreparedStatement pstmt = null;
		String sql = null;
		try {			
			sql = "insert into TM_LOGMINER_TABLE (ETL_NAME,TABLE_NAME) values (?,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, etlName);
			pstmt.setString(2, tableName);
			pstmt.executeUpdate();
			pstmt.close();

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private Set<String> getTableKafkaTopics(Connection conn, String etlName) throws Exception {

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		Set<String> topicset = new HashSet<>();
		try {
			
			sql = "select TOPIC from " + TglminerTableEnum.TM_KAFKA_TOPIC + " where ETL_NAME=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, etlName);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				topicset.add(rs.getString("TOPIC"));
			}
			rs.close();
			pstmt.close();

			return topicset;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
	}
	private void executeSqlScriptFromFile(Connection conn, String file) throws Exception {
		LOG.info(">>>>>>>>>>>> executeSqlScriptFromFile file={}", file);

		Statement stmt = null;
		try {

			ClassLoader loader = Thread.currentThread().getContextClassLoader();	
			try (InputStream inputStream = loader.getResourceAsStream(file)) {
				String createScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
				stmt = conn.createStatement();
				stmt.executeUpdate(createScript);
				stmt.close();
			} catch (SQLException | IOException e) {
				if (stmt != null) stmt.close();
				throw e;
			}

			LOG.info(">>>>>>>>>>>> executeSqlScriptFromFile Done!!!");

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (stmt != null) stmt.close();
		}
	}
	private void executeScript(Connection conn, String script) throws Exception {

		Statement stmt = null;
		try {

			stmt = conn.createStatement();
			stmt.executeUpdate(script);
			stmt.close();

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (stmt != null) stmt.close();
			
		}

	}
	
}
