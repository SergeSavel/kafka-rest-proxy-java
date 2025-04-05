package pro.savel.kafka.producer;

import pro.savel.kafka.producer.requests.ProduceRequest;
import pro.savel.kafka.producer.requests.ProduceStringRequest;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

public class ProducerMapper {
    public static ProduceRequest mapProduceRequest(ProduceStringRequest stringRequest, UUID producerId) {
        var headers = new HashMap<String, byte[]>(stringRequest.headers().size());
        stringRequest.headers().forEach((key, value) -> headers.put(key, value.getBytes(StandardCharsets.UTF_8)));
        var request = new ProduceRequest();
        request.setId(producerId);
        request.setToken(stringRequest.token());
        request.setTopic(stringRequest.topic());
        request.setPartition(stringRequest.partition());
        request.setHeaders(headers);
        request.setKey(stringRequest.key().getBytes(StandardCharsets.UTF_8));
        request.setValue(stringRequest.value().getBytes(StandardCharsets.UTF_8));
        return request;
    }
}
