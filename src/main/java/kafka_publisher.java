import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class kafka_publisher extends Thread {
    public static void main(String[] args)
    {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("acks","all");
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        try
        {
            Producer<String, String> producer = new KafkaProducer(props);


            for(int i =0;i<10;i++)
            {
                producer.send(new ProducerRecord<String,String>("demo",Integer.toString(i),Integer.toString(i)));
                System.out.println("Message sent");
            }
            producer.close();
        }catch (Exception e){System.out.println(e);}
    }
}
