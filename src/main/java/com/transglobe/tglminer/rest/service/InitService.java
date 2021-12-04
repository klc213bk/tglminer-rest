package com.transglobe.tglminer.rest.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.transglobe.tglminer.rest.bean.HealthETL;
import com.transglobe.tglminer.rest.bean.HealthTopicEnum;
import com.transglobe.tglminer.rest.bean.TglminerSPEnum;
import com.transglobe.tglminer.rest.bean.TglminerTableEnum;
import com.transglobe.tglminer.rest.bean.EtlNameBo.ConsumerStatusEnum;
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

			LOG.info(">>> delete kafka topic");
			List<String> topicList = new ArrayList<>();
			topicList.add(HealthTopicEnum.DDL.getTopic());
			topicList.add(HealthTopicEnum.HEARTBEAT.getTopic());


			//			kafkaService.deleteKafkaTopics(topicList);
			Set<String> topicSet = kafkaService.listTopics();

			for (String t : topicList) {
				if (topicSet.contains(t)) {
					kafkaService.deleteTopic(t);
					LOG.info(">>>>>>>>>>>> topic={} deleted ", t);

				}
			}


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
			setupDbObjects();
			
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			// insert etl
			LOG.info(">>> insert etl name ");
			cstmt = conn.prepareCall("{call SP_INS_ETL_NAME(?,?,?,?,?)}");

			cstmt.setString(1,  HealthETL.CAT);
			cstmt.setString(2,  HealthETL.NAME);
			cstmt.setInt(3,  0);
			cstmt.setString(4, ConsumerStatusEnum.REGISTERED.name());
			cstmt.setString(5,  HealthETL.NOTE);
			cstmt.execute();
			cstmt.close();

			
			LOG.info(">>> insert kafka topic");
			for (HealthTopicEnum e : HealthTopicEnum.values()) {
				cstmt = conn.prepareCall("{call SP_INS_KAFKA_TOPIC(?,?)}");
				cstmt.setString(1,  HealthETL.NAME);
				cstmt.setString(2,  e.getTopic());
				cstmt.execute();
				cstmt.close();
			}
			
			List<String> topicList = new ArrayList<>();
			topicList.add(HealthTopicEnum.DDL.getTopic());
			topicList.add(HealthTopicEnum.HEARTBEAT.getTopic());

			Set<String> topicSet = kafkaService.listTopics();
			for (String t : topicList) {
				if (topicSet.contains(t)) {
					kafkaService.deleteTopic(t);
					LOG.info(">>>>>>>>>>>> topic={} deleted ", t);
				}
				kafkaService.createTopic(t);
			}	

			LOG.info(">>> insert logminer table");
			insertLogminerTable(conn, HealthETL.NAME, TglminerTableEnum.TM_HEARTBEAT.getTableName());
			
			conn.close();

		} finally {
			if (cstmt != null) cstmt.close();
			if (conn != null) conn.close();
		}

	}
	private void setupDbObjects() throws Exception {
		Connection conn = null;
		try {	
			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);
			
			for (TglminerTableEnum e : TglminerTableEnum.values()) {
				LOG.info(">>>>>>> create TABLE file {}",e.getScriptFile());
				DbUtils.executeSqlScriptFromFile(conn, e.getScriptFile());
			}
			
			for (TglminerSPEnum e : TglminerSPEnum.values()) {
				LOG.info(">>>>>>> create SP file {}",e.getScriptFile());
				DbUtils.executeSqlScriptFromFile(conn, e.getScriptFile());
			}
		} finally {
			if (conn != null) conn.close();
		}
	}
	private void insertLogminerTable(Connection conn, String etlName, String tableName) throws Exception{
		LOG.info(">>>>>>>>>> insertLogminerTable");
		CallableStatement cstmt = null;
		try {	
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
		}
	}
}
