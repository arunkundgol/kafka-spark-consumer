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
	
	public static void main(String[] args) throws Exception {
		if (args.length <4){
			System.err.println("Usage: Consumer <brokers> <topics> \n"+
		"<brokers> is a list of one or more kafka brokers" +
					"<topic> is a list of one or more kafkatopics to consumer from \n\n");
			System.exit(1);
		}
		String brokers = args [0];
		String topic = args[1];
		
		String[] splitbroker = brokers.split(":");
		String hostname = splitbroker[0];
		String port = splitbroker[1];
		Properties props = new Properties();

		props.put("zookeeper.hosts", hostname);
		props.put("zookeeper.port", port);
		props.put("zookeeper.broker.path", "/brokers");
		props.put("kafka.topic", topic);
		props.put("kafka.consumer.id", "test-id");
		props.put("zookeeper.consumer.connection", brokers);
		props.put("zookeeper.consumer.path", "/spark-kafka");

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
	}
