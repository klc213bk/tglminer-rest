package com.transglobe.tglminer.rest.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.transglobe.tglminer.rest.bean.HealthETL;
import com.transglobe.tglminer.rest.bean.HealthSyncTableEnum;
import com.transglobe.tglminer.rest.bean.HealthTopicEnum;
import com.transglobe.tglminer.rest.bean.TglminerSPEnum;
import com.transglobe.tglminer.rest.bean.TglminerTableEnum;
import com.transglobe.tglminer.rest.bean.EtlNameBo.ConsumerStatusEnum;
import com.transglobe.tglminer.rest.bean.EtlNameBo.WithSyncEnum;
import com.transglobe.tglminer.rest.controller.InitController;
import com.transglobe.tglminer.rest.util.DbUtils;
import com.transglobe.tglminer.rest.util.HttpUtils;

@Service
public class InitService {
	static final Logger LOG = LoggerFactory.getLogger(InitController.class);

	@Value("${tglminer.db.driver}")
	private String tglminerDbDriver;

	@Value("${tglminer.db.url}")
	private String tglminerDbUrl;

	@Value("${tglminer.db.username}")
	private String tglminerDbUsername;

	@Value("${tglminer.db.password}")
	private String tglminerDbPassword;

	@Value("${health.etl.name}")
	private String healthEtlName;

	@Value("${connector.name}")
	private String connectorName;

	@Autowired
	KafkaService kafkaService;

	public void cleanup() throws Exception{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		try {

			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			removeEtl(healthEtlName);
			LOG.info(">>> retl={}, removed", healthEtlName); 


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
					DbUtils.executeScript(conn, "DROP PROCEDURE " + sp);
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
			sql = "select TABLE_NAME from USER_TABLES";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				String table = rs.getString("TABLE_NAME");
				if (tbSet.contains(table)) {
					DbUtils.executeScript(conn, "DROP TABLE " + table);
					LOG.info(">>> table={} dropped", table); 
				}
			}
			rs.close();
			pstmt.close();
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}

	}

	public void removeEtl(String etlName) throws Exception{
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql = null;
		try {

			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			LOG.info(">>>clear KafkaTopic Data");
			clearKafkaTopicData(conn, etlName);

			LOG.info(">>>clearLogminerSync");
			clearLogminerSync(conn, etlName);

			LOG.info(">>>delete EtlName");
			sql = "delete from TM_ETL_NAME where ETL_NAME=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, etlName);
			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}

	}
	public void createTopic(String etlName, String topic) throws Exception{
		Connection conn = null;

		CallableStatement cstmt = null;
		try {

			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			Set<String> topicSet = kafkaService.listTopics();

			cstmt = conn.prepareCall("{call SP_INS_KAFKA_TOPIC(?,?)}");
			cstmt.setString(1,  etlName);
			cstmt.setString(2,  topic);
			cstmt.execute();
			cstmt.close();

			if (topicSet.contains(topic)) {
				kafkaService.deleteTopic(topic);
				LOG.info(">>>>>>>>>>>> topic={} deleted ", topic);
			}
			kafkaService.createTopic(topic);

		} finally {
			if (cstmt != null) cstmt.close();
			if (conn != null) conn.close();
		}

	}
	private void clearLogminerSync(Connection conn, String etlName) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		try {
			LOG.info(">>> delete logminer sync table");

			
			Set<String> tableSet = getLogminerTables(conn, etlName);
			LOG.info(">>>> tableSet={}", String.join(",", tableSet));


			// restart connector
			LOG.info(">>>> resetOffset add sync table");
			Map<String,String> configmap = kafkaService.getUpdatedConnectorConfigMap(connectorName, Boolean.TRUE, WithSyncEnum.SYNC, tableSet) ;
			String restartConnectorStatus = kafkaService.restartConnector(connectorName, configmap);
			LOG.info(">>>> restartConnectorStatus status={}", restartConnectorStatus);

			String tableWhitelist = configmap.get("table.whitelist");
			String kafkaTopics = String.join(",", kafkaService.listTopics());
			LOG.info(">>>> tableWhitelist={}, kafkaTopics={}",tableWhitelist,  kafkaTopics);
			
			//update connector status
			sql = "update TM_LOGMINER_OFFSET SET TABLE_WHITE_LIST=?,KAFKA_TOPICS=?,STATUS=? where start_time = \n" +
					" (select start_time from TM_LOGMINER_OFFSET order by start_time desc \n" +
					" fetch next 1 row only)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, tableWhitelist);
			pstmt.setString(2, kafkaTopics);
			pstmt.setString(3, restartConnectorStatus);
			pstmt.executeUpdate();
			pstmt.close();

			LOG.info(">>>> update connector logminer connector status={} Done !!!", restartConnectorStatus);

			sql = "delete from TM_LOGMINER_TABLE where ETL_NAME=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, etlName);
			pstmt.executeUpdate();
			pstmt.close();
		}finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
	}
	private Set<String> getLogminerTables(Connection conn, String etlName) throws Exception {

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		Set<String> tableSet = new HashSet<>();
		try {

			sql = "select TABLE_NAME from TM_LOGMINER_TABLE where ETL_NAME=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, etlName);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				tableSet.add(rs.getString("TABLE_NAME"));
			}
			pstmt.close();

			return tableSet;
		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}

	}
//	private void updateETLName(Connection conn, String etlName, WithSyncEnum withSyncEnum, ConsumerStatusEnum consumerStatus) throws Exception {
//		PreparedStatement pstmt = null;
//		String sql = null;
//		try {
//			if (withSyncEnum != null && consumerStatus != null) {
//				sql = "update TM_ETL_NAME set WITH_SYNC=?, CONSUMER_STATUS=? where ETL_NAME=?";
//				pstmt = conn.prepareStatement(sql);
//
//				pstmt.setInt(1,  (WithSyncEnum.SYNC == withSyncEnum)? 1 : 0);
//				pstmt.setString(2,  consumerStatus.name());
//				pstmt.setString(3,  etlName);
//				pstmt.executeUpdate();
//				pstmt.close();
//			} else if (withSyncEnum != null && consumerStatus == null) {
//				sql = "update TM_ETL_NAME set WITH_SYNC=? where ETL_NAME=?";
//				pstmt = conn.prepareStatement(sql);
//
//				pstmt.setInt(1,  (WithSyncEnum.SYNC == withSyncEnum)? 1 : 0);
//				pstmt.setString(2,  etlName);
//				pstmt.executeUpdate();
//				pstmt.close();
//			} else if (withSyncEnum == null && consumerStatus != null) {
//				sql = "update TM_ETL_NAME set CONSUMER_STATUS=? where ETL_NAME=?";
//				pstmt = conn.prepareStatement(sql);
//
//				pstmt.setString(1, consumerStatus.name());
//				pstmt.setString(2,  etlName);
//				pstmt.executeUpdate();
//				pstmt.close();
//			}
//
//		} catch (Exception e1) {
//
//			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));
//
//			throw e1;
//		} finally {
//			if (pstmt != null) pstmt.close();
//		}
//	}
//

	//



	private void clearKafkaTopicData(Connection conn, String etlName) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		try {
			sql = "select TOPIC from TM_KAFKA_TOPIC where ETL_NAME=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, etlName);
			rs = pstmt.executeQuery();
			List<String> topicList = new ArrayList<>();
			while (rs.next()) {
				topicList.add(rs.getString("TOPIC"));
			}
			rs.close();
			pstmt.close();
			LOG.info(">>> list topics={}", String.join(",", topicList));

			Set<String> topicSet = kafkaService.listTopics();
			LOG.info(">>> list topics set ={}", String.join(",", topicSet));
			for (String t : topicList) {
				if (topicSet.contains(t)) {
					kafkaService.deleteTopic(t);
					LOG.info(">>>>>>>>>>>> topic={} deleted ", t);

				}
			}
			sql = "delete from TM_KAFKA_TOPIC where ETL_NAME=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, etlName);
			pstmt.executeUpdate();
			pstmt.close();
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
	}
	public void initialize() throws Exception{
		Connection conn = null;

		CallableStatement cstmt = null;
		try {

			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			LOG.info(">>> setupDbObjects ");
			setupDbObjects(conn);

			// insert etl
			LOG.info(">>> insert etl name ");
			cstmt = conn.prepareCall("{call SP_INS_ETL_NAME(?,?,?)}");

			cstmt.setString(1,  HealthETL.CAT);
			cstmt.setString(2,  HealthETL.NAME);
			cstmt.setString(3,  HealthETL.NOTE);
			cstmt.execute();
			cstmt.close();


			LOG.info(">>> insert kafka topic");
			for (HealthTopicEnum e : HealthTopicEnum.values()) {
				createTopic(HealthETL.NAME, e.getTopic());
			}
			
//			List<String> topicList = new ArrayList<>();
//			for (HealthTopicEnum e : HealthTopicEnum.values()) {
//				cstmt = conn.prepareCall("{call SP_INS_KAFKA_TOPIC(?,?)}");
//				cstmt.setString(1,  HealthETL.NAME);
//				cstmt.setString(2,  e.getTopic());
//				cstmt.execute();
//				cstmt.close();
//
//				topicList.add(e.getTopic());
//			}
//			Set<String> topicSet = kafkaService.listTopics();
//			for (String t : topicList) {
//				if (topicSet.contains(t)) {
//					kafkaService.deleteTopic(t);
//					LOG.info(">>>>>>>>>>>> topic={} deleted ", t);
//				}
//				kafkaService.createTopic(t);
//			}	

			LOG.info(">>> insert logminer table");
			for (HealthSyncTableEnum e : HealthSyncTableEnum.values()) {
				insertLogminerSyncTable(HealthETL.NAME,  e.getSyncTableName());
			}
			
			conn.close();

		} finally {
			if (cstmt != null) cstmt.close();
			if (conn != null) conn.close();
		}

	}
	private void setupDbObjects(Connection conn) throws Exception {


		for (TglminerTableEnum e : TglminerTableEnum.values()) {
			LOG.info(">>>>>>> create TABLE file {}",e.getScriptFile());
			DbUtils.executeSqlScriptFromFile(conn, e.getScriptFile());
		}

		for (TglminerSPEnum e : TglminerSPEnum.values()) {
			LOG.info(">>>>>>> create SP file {}",e.getScriptFile());
			DbUtils.executeSqlScriptFromFile(conn, e.getScriptFile());
		}

	}
	public void insertLogminerSyncTable(String etlName, String tableName) throws Exception{
		LOG.info(">>>>>>>>>> insertLogminerTable");
		Connection conn = null;
		CallableStatement cstmt = null;
		
		try {	
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			cstmt = conn.prepareCall("{call SP_INS_LOGMINER_TABLE(?,?)}");

			cstmt.setString(1,  etlName);
			cstmt.setString(2,  tableName);
			cstmt.execute();
			cstmt.close();

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (cstmt != null) cstmt.close();
			if (conn != null) conn.close();
		}
	}
}
