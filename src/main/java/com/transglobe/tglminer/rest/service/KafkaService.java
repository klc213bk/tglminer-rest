package com.transglobe.tglminer.rest.service;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;


@Service
public class KafkaService {
	static final Logger LOG = LoggerFactory.getLogger(KafkaService.class);
	
	@Value("${connect.rest.url}")
	private String connectRestUrl;
	
	public Map<String,String> getConnectorConfig(String connectorName) throws Exception {
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
}
