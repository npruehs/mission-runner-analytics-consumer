package de.npruehs.missionrunner.analytics.consumer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class App 
{
	public static void main( String[] args ) throws IOException
	{
		// Create consumer.
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "missionrunnergroup");
		props.setProperty("enable.auto.commit", "false");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		// Subscribe to topic.
		consumer.subscribe(Arrays.asList("missionrunner"));

		// Wait before flushing to file.
		final int eventBatchSize = 3;

		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

		while (true) {
			// Poll for new records.
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				buffer.add(record);
			}

			if (buffer.size() >= eventBatchSize) {
				// Flush to HDFS.
				StringBuilder stringBuilder = new StringBuilder();
				
				for (int i = 0; i < buffer.size(); ++i) {
					stringBuilder.append(buffer.get(i).value() + "\r\n");
				}

				String eventLog = stringBuilder.toString();
				
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
				Date date = new Date();
				
				String fileName = "/user/npruehs/" + dateFormat.format(date) + ".txt";
				
				putToHDFS(fileName, eventLog);
				
				System.out.println(eventLog);
				System.out.println();
				
				consumer.commitSync();
				buffer.clear();
			}
		}
	}

	private static void putToHDFS(String path, String content) throws IOException {
		URL url = new URL("http://localhost:9870/webhdfs/v1" + path + "?user.name=npruehs&op=CREATE");
		
		URLConnection urlconnection = url.openConnection();
		urlconnection.setDoOutput(true);
		urlconnection.setDoInput(true);

		if (urlconnection instanceof HttpURLConnection) {
			((HttpURLConnection)urlconnection).setRequestMethod("PUT");
			((HttpURLConnection)urlconnection).setRequestProperty("Content-type", "text/html");
			((HttpURLConnection)urlconnection).connect();
		}

		BufferedOutputStream bos = new BufferedOutputStream(urlconnection.getOutputStream());
		BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(content.getBytes()));
		
		int i;

		while ((i = bis.read()) >= 0) {
			bos.write(i);
		}
		
		System.out.println(url);
		System.out.println(((HttpURLConnection)urlconnection).getResponseMessage());
		System.out.println();
	}
}
