 /**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package consumer.kafka.client;
import java.io.Serializable;
import java.util.Properties;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import consumer.kafka.MessageAndMetadata;
import consumer.kafka.ReceiverLauncher;

public class Consumer implements Serializable {

	private static final long serialVersionUID = 4332618245650072140L;

	public void start() throws InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		run();
	}

	private void run() {
		
		File configFile = new File("src/main/resources/kafka-spark.properties");
		
		try{
			FileReader reader = new FileReader(configFile);
			Properties props = new Properties();
			props.load(reader);
			
			//props.put("zookeeper.hosts", "10.252.1.136");
			//props.put("zookeeper.port", "2181");
			//props.put("zookeeper.broker.path", "/brokers");
			//props.put("kafka.topic", "test-topic");
			//props.put("kafka.consumer.id", "test-id");
			//props.put("zookeeper.consumer.connection", "10.252.5.113:2182");
			//props.put("zookeeper.consumer.path", "/spark-kafka");

			reader.close(); // close configFile reader
			
			//Create a SparkContext
			SparkConf _sparkConf = new SparkConf().setAppName("kafka-spark-consumer").set(
					"spark.streaming.receiver.writeAheadLog.enable", "false");
			// Create a Spark Streaming Context
			JavaStreamingContext jsc = new JavaStreamingContext(_sparkConf,
					new Duration(1000));

			// Specify number of Receivers you need.
			int numberOfReceivers = 3;
			

			JavaDStream<MessageAndMetadata> unionStreams = ReceiverLauncher.launch(
					jsc, props, numberOfReceivers, StorageLevel.MEMORY_ONLY());

			unionStreams.foreachRDD(new Function2<JavaRDD<MessageAndMetadata>, Time, Void>() {
				public Void call(JavaRDD<MessageAndMetadata> rdd, Time time) throws Exception {
					rdd.collect();
					System.out.println(" Number of records in this batch "+ rdd.count());
					return null;
					}
				});

			jsc.start();
			jsc.awaitTermination();
			
			}
		catch (FileNotFoundException ex){
			Logger LOG = Logger.getLogger(this.getClass());
			LOG.error("Config FileNotFound",ex);
			LOG.trace(null,ex);
			}
		catch (IOException ex) {
			Logger LOG = Logger.getLogger(this.getClass());
			LOG.error("Config IO Error",ex);
			LOG.trace(null,ex);
			}
		
		
	}

	public static void main(String[] args) throws Exception {

		Consumer consumer = new Consumer();
		consumer.start();
	}
}
