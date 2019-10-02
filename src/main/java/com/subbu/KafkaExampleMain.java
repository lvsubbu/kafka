package com.subbu;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.nio.file.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

public class KafkaExampleMain {
    public static final String DATAPOINTSDIR = "/Users/subramoniapillai/Downloads/KafkaRequirement";
    public static final String ARCHIVESDIR = "/Users/subramoniapillai/Downloads/KafkaRequirementArcv";

    private ThreadPoolExecutor producerExecutors;
    private WatchService watcher;
    private WatchKey key;
    private Path path = Paths.get(DATAPOINTSDIR);

    public static void main(String[] args) {

        try {
            new KafkaExampleMain().start(Integer.valueOf(args[0]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void start(int maxThreads) throws Exception {

        // create thread pool
        producerExecutors = new ThreadPoolExecutor(1, maxThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        // handover the current files to the thread
        File directory = new File(DATAPOINTSDIR);
        File[] fList = directory.listFiles();
        for (File file : fList) {
             producerExecutors.execute(new KafkaProducerTask(file));
        }

        // wait for new files and handover to producer as well
        watcher = FileSystems.getDefault().newWatchService();
        key = path.register(watcher, ENTRY_CREATE);

        for (;;) {

            watcher.take();

            for (WatchEvent<?> event : key.pollEvents()) {
                Path path = ((WatchEvent<Path>)event).context();
                producerExecutors.execute(new KafkaProducerTask(path.toFile()));
            }

            key.reset();
        }
    }

    public class KafkaProducerTask implements Runnable {
        private File file;

        public KafkaProducerTask(File file) {
            this.file = file;
        }

        @Override
        public void run() {
            try {

                Properties properties = new Properties();
                properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
                properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

                Producer<String, String> producer = new KafkaProducer<>(properties);

                CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
                CsvMapper csvMapper = new CsvMapper();
                MappingIterator<LinkedHashMap> rd = csvMapper.readerFor(Map.class).with(csvSchema).readValues(file);

                while (rd.hasNext()) {
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.enable(SerializationFeature.INDENT_OUTPUT);
                    String value = mapper.writeValueAsString(rd.next());
                    ProducerRecord<String, String> pd = new ProducerRecord<>("trade_topic_3a", "", value);
                    producer.send(pd);
                }
                if (!new File(ARCHIVESDIR).exists()) {
                    new File(ARCHIVESDIR).mkdir();
                }
                System.out.println(ARCHIVESDIR);
                System.out.println(file.getName());

                file.renameTo(new File(ARCHIVESDIR+"/"+file.getName()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
