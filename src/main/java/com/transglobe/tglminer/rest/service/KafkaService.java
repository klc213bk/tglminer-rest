package com.transglobe.tglminer.rest.service;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.tglminer.rest.bean.EtlNameBo.WithSyncEnum;
import com.transglobe.tglminer.rest.util.HttpUtils;


@Service
public class KafkaService {
	static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);
	
	@Value("${connect.rest.url}")
	private String connectRestUrl;
	
	@Value("${kafka.server.home}")
	private String kafkaServerHome;
	
	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;
	
	private Process listTopicsProcess;
	
	private Process deleteTopicProcess;

	private Process createTopicProcess;
	
	public Map<String,String> getUpdatedConnectorConfigMap(String connectorName, Boolean resetOffset, WithSyncEnum withSync, Set<String> tableSet) throws Exception {
		
		
		Map<String,String> configmap = getConnectorConfig(connectorName);
		LOG.info(">>>> configmap={}", configmap);

		if (Boolean.TRUE.equals(resetOffset)) {
			configmap.put("reset.offset", "true");
		} else {
			configmap.put("reset.offset", "false");
		}

		if (WithSyncEnum.SYNC == withSync) {
			String newsyncTables = String.join(",", tableSet);	
			LOG.info(">>>> add sync table:{}", newsyncTables);

			// "reset.offset", "table.whitelist"
			String newtableWhitelist = "";
			newtableWhitelist = configmap.get("table.whitelist") + "," + newsyncTables;
			newtableWhitelist = StringUtils.strip(newtableWhitelist, ",");
			configmap.put("table.whitelist", newtableWhitelist);


		} else if (WithSyncEnum.DROP_SYNC == withSync) {
			LOG.info(">>>> remove sync tableSet:{}", String.join(",", tableSet));

			String[] tableArr = configmap.get("table.whitelist").split(",");
			List<String> tableList = Arrays.asList(tableArr);
			LOG.info(">>>> existing sync tableList:{}", String.join(",", tableList));

			String newtableWhitelist = tableList.stream().filter(s -> !tableSet.contains(s)).collect(Collectors.joining(","));
			newtableWhitelist = StringUtils.strip(newtableWhitelist, ",");
			LOG.info(">>>> new newtableWhitelist={}", newtableWhitelist);

			configmap.put("table.whitelist", newtableWhitelist);


		} 

		LOG.info(">>>> new configmap={}", configmap);

		return configmap;
	}
	public String restartConnector(String connectorName, Map<String,String> configmap) throws Exception {
		LOG.info(">>>> restartConnector...");

		LOG.info(">>>> pause connector");
		pauseConnector(connectorName);

		LOG.info(">>>> delete connector");
		deleteConnector(connectorName);

		LOG.info(">>>> add sync table to config's whitelist");

		LOG.info(">>>> create connector");
		boolean result =createConnector(connectorName, configmap);
		LOG.info(">>>> create connector result={}", result);

		String status = getConnectorStatus(connectorName);

		LOG.info(">>>> connector status={}", status);

		return status;
	}
	
	public Map<String,String> getConnectorConfig(String connectorName) throws Exception {
		
		LOG.info(">>>>>>>>>>>> Sleep for 15 seconds for restarting");
		Thread.sleep(15000);
		
		Map<String,String> configmap = new HashMap<>();
		String urlStr = String.format(connectRestUrl+"/connectors/%s/config", connectorName);
		LOG.info(">>>>>>>>>>>> urlStr={} ", urlStr);
		HttpURLConnection httpCon = null;
		try {
			URL url = new URL(urlStr);
			httpCon = (HttpURLConnection)url.openConnection();
			httpCon.setRequestMethod("GET");
			int responseCode = httpCon.getResponseCode();
			String readLine = null;
			//			if (httpCon.HTTP_OK == responseCode) {
			BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> CONNECT REST responseCode={},response={}", responseCode, response.toString());

			configmap = new ObjectMapper().readValue(response.toString(), HashMap.class);

		} finally {
			if (httpCon != null ) httpCon.disconnect();
		}
		return configmap;

	}
	public boolean pauseConnector(String connectorName) throws Exception{
		String urlStr = connectRestUrl + "/connectors/" + connectorName + "/pause";
		LOG.info(">>>>> connector urlStr:" + urlStr);

		HttpURLConnection httpConn = null;
		//		DataOutputStream dataOutStream = null;
		int responseCode = -1;
		try {
			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("PUT");

			responseCode = httpConn.getResponseCode();
			LOG.info(">>>>> pause responseCode={}",responseCode);

			String readLine = null;

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> pause response={}",response.toString());

			if (202 == responseCode) {
				return true;
			} else {
				return false;
			}
		} finally {

			if (httpConn != null )httpConn.disconnect();
		}
	}
	public boolean deleteConnector(String connectorName) throws Exception{
		String urlStr = connectRestUrl + "/connectors/" + connectorName +"/";
		//		LOG.info(">>>>> connector urlStr:" + urlStr);
		HttpURLConnection httpConn = null;
		try {
			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("DELETE");
			int responseCode = httpConn.getResponseCode();

			LOG.info(">>>>> DELETE responseCode={}",responseCode);

			String readLine = null;

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> delete response={}",response.toString());

			if (204 == responseCode) {
				return true;
			} else {
				return false;
			}

		} finally {
			if (httpConn != null )httpConn.disconnect();
		}
	}
	public boolean createConnector(String connectorName, Map<String, String> configmap) throws Exception {
		LOG.info(">>>>>>>>>>>> createNewConnector");

		HttpURLConnection httpConn = null;
		DataOutputStream dataOutStream = null;
		try {

			//			Map<String, Object> map = new HashMap<>();
			//			map.put("name", connectorName);
			//			map.put("config", configmap);

			ObjectMapper objectMapper = new ObjectMapper();
			String configStr = objectMapper.writeValueAsString(configmap);


			String urlStr = connectRestUrl+"/connectors/" + connectorName + "/config";

			LOG.info(">>>>> connector urlStr={},reConfigStr={}", urlStr, configStr);

			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("PUT"); 
			httpConn.setDoInput(true);
			httpConn.setDoOutput(true);
			httpConn.setRequestProperty("Content-Type", "application/json");
			httpConn.setRequestProperty("Accept", "application/json");

			dataOutStream = new DataOutputStream(httpConn.getOutputStream());
			dataOutStream.writeBytes(configStr);

			dataOutStream.flush();

			int responseCode = httpConn.getResponseCode();
			LOG.info(">>>>> createNewConnector responseCode={}",responseCode);

			String readLine = null;

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();
			LOG.info(">>>>> create connenctor response={}",response.toString());

			if (200 == responseCode || 201 == responseCode) {
				return true;
			} else {
				return false;
			}

		}  finally {
			if (dataOutStream != null) {
				try {
					dataOutStream.flush();
					dataOutStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (httpConn != null )httpConn.disconnect();

		}
	}
	public String getConnectorStatus(String connector) throws Exception {
		String urlStr = connectRestUrl+"/connectors/" + connector+ "/status";
		HttpURLConnection httpCon = null;
		//		ConnectorStatus connectorStatus = null;
		try {
			URL url = new URL(urlStr);
			httpCon = (HttpURLConnection)url.openConnection();
			httpCon.setRequestMethod("GET");
			int responseCode = httpCon.getResponseCode();
			LOG.info(">>>>> getConnectorStatus responseCode:" + responseCode);

			String readLine = null;

			BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream(), "UTF-8"));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> getConnectorStatus response:" + response.toString());

			return response.toString();

			//			ObjectMapper objectMapper = new ObjectMapper();
			//			objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			//			connectorStatus = objectMapper.readValue(response.toString(), ConnectorStatus.class);

			//			JsonNode jsonNode = objectMapper.readTree(response.toString());
			//			JsonNode connectorNode = jsonNode.get("connector");
			//			JsonNode stateNode = connectorNode.get("state");
			//			String state = stateNode.asText();
			//
			//			if (httpCon.HTTP_OK == responseCode) {
			//				connectorStatus = new ConnectorStatus(connector, state, null);
			//			} else {
			//				connectorStatus = new ConnectorStatus(connector, state, response.toString());
			//			}
		} finally {
			if (httpCon != null ) httpCon.disconnect();
		}
	}
	
	public Set<String> listTopics() throws Exception {
		LOG.info(">>>>>>>>>>>> listTopics ");
		List<String> topics = new ArrayList<String>();
		try {
			if (listTopicsProcess == null || !listTopicsProcess.isAlive()) {
				ProcessBuilder builder = new ProcessBuilder();
				String script = "./bin/kafka-topics.sh" + " --list --bootstrap-server " + kafkaBootstrapServer;
				builder.command("sh", "-c", script);
//				builder.command(kafkaTopicsScript + " --list --bootstrap-server " + kafkaBootstrapServer);
				
//				builder.command(kafkaTopicsScript, "--list", "--bootstrap-server", kafkaBootstrapServer);
				
				builder.directory(new File(kafkaServerHome));
				listTopicsProcess = builder.start();

				ExecutorService listTopicsExecutor = Executors.newSingleThreadExecutor();
				listTopicsExecutor.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(listTopicsProcess.getInputStream()));
						reader.lines().forEach(topic -> topics.add(topic));
					}

				});
				int exitVal = listTopicsProcess.waitFor();
				if (exitVal == 0) {

					LOG.info(">>> Success!!! listTopics, exitVal={}", exitVal);
				} else {
					LOG.error(">>> Error!!! listTopics, exitcode={}", exitVal);
					String errStr = (topics.size() > 0)? topics.get(0) : "";
					throw new Exception(errStr);
				}
				
				
			} else {
				LOG.warn(" >>> listTopics is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, listTopics, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} catch (InterruptedException e) {
			LOG.error(">>> Error!!!, listTopics, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		}
		return new HashSet<>(topics);
	}
	public void createTopic(String topic) throws Exception {
		LOG.info(">>>>>>>>>>>> createTopic topic=={}", topic);
		try {
			if (createTopicProcess == null || !createTopicProcess.isAlive()) {
				ProcessBuilder builder = new ProcessBuilder();
				String script = "./bin/kafka-topics.sh --create --bootstrap-server " + kafkaBootstrapServer + " --replication-factor 1 --partitions 1 --topic " + topic;
				builder.command("sh", "-c", script);

				builder.directory(new File(kafkaServerHome));
				createTopicProcess = builder.start();

				int exitVal = createTopicProcess.waitFor();
				if (exitVal == 0) {
					LOG.info(">>> Success!!! createTopic:{}, exitcode={}", topic, exitVal);
				} else {
					LOG.error(">>> Error!!! createTopic:{}, exitcode={}", topic, exitVal);
				}
				LOG.info(">>> createTopicProcess isalive={}", createTopicProcess.isAlive());
				if (!createTopicProcess.isAlive()) {
					createTopicProcess.destroy();
				}
				
				
				//				if (!createTopicProcess.isAlive()) {
				//					LOG.info(">>>  createTopicProcess isAlive={}", createTopicProcess.isAlive());
				//					createTopicExecutor.shutdown();
				//					if (!createTopicExecutor.isTerminated()) {
				//						LOG.info(">>> createTopicExecutor is not Terminated, prepare to shutdown executor");
				//						createTopicExecutor.shutdownNow();
				//
				//						try {
				//							createTopicExecutor.awaitTermination(600, TimeUnit.SECONDS);
				//						} catch (InterruptedException e) {
				//							LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
				//									ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				//						}
				//
				//					} else {
				//						LOG.warn(">>> createTopicExecutor is already terminated");
				//					}
				//				} else {
				//					LOG.info(">>>  createTopicProcess isAlive={}, destroy it", createTopicProcess.isAlive());
				//					createTopicProcess.destroy();
				//				}

			} else {
				LOG.warn(" >>> createTopic is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, createTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} catch (InterruptedException e) {
			LOG.error(">>> Error!!!, createTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void deleteTopic(String topic) throws Exception {
		LOG.info(">>>>>>>>>>>> deleteTopic topic=={}", topic);
		try {
			if (deleteTopicProcess == null || !deleteTopicProcess.isAlive()) {
				ProcessBuilder builder = new ProcessBuilder();
				String script = "./bin/kafka-topics.sh --delete --bootstrap-server " + kafkaBootstrapServer + " --topic " + topic;
				builder.command("sh", "-c", script);

				builder.directory(new File(kafkaServerHome));
				deleteTopicProcess = builder.start();

				int exitVal = deleteTopicProcess.waitFor();
				if (exitVal == 0) {
					LOG.info(">>> Success!!! deleteTopic:{}, exitcode={}", topic, exitVal);
				} else {
					LOG.error(">>> Error!!! deleteTopic:{}, exitcode={}", topic, exitVal);
				}
				LOG.info(">>> deleteTopicProcess isalive={}", deleteTopicProcess.isAlive());
				if (!deleteTopicProcess.isAlive()) {
					deleteTopicProcess.destroy();
				}
				
				
				//				if (!createTopicProcess.isAlive()) {
				//					LOG.info(">>>  createTopicProcess isAlive={}", createTopicProcess.isAlive());
				//					createTopicExecutor.shutdown();
				//					if (!createTopicExecutor.isTerminated()) {
				//						LOG.info(">>> createTopicExecutor is not Terminated, prepare to shutdown executor");
				//						createTopicExecutor.shutdownNow();
				//
				//						try {
				//							createTopicExecutor.awaitTermination(600, TimeUnit.SECONDS);
				//						} catch (InterruptedException e) {
				//							LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
				//									ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				//						}
				//
				//					} else {
				//						LOG.warn(">>> createTopicExecutor is already terminated");
				//					}
				//				} else {
				//					LOG.info(">>>  createTopicProcess isAlive={}, destroy it", createTopicProcess.isAlive());
				//					createTopicProcess.destroy();
				//				}

			} else {
				LOG.warn(" >>> deleteTopic is currently Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, deleteTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} catch (InterruptedException e) {
			LOG.error(">>> Error!!!, deleteTopic, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
}
