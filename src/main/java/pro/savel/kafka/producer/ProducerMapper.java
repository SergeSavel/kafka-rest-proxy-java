package pro.savel.kafka.producer;

import pro.savel.kafka.producer.requests.ProduceRequest;
import pro.savel.kafka.producer.requests.ProduceStringRequest;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

public class ProducerMapper {
    public static ProduceRequest mapProduceRequest(ProduceStringRequest stringRequest, UUID producerId) {
        var headers = new HashMap<String, byte[]>(stringRequest.getHeaders().size());
        stringRequest.getHeaders().forEach((key, value) -> headers.put(key, value.getBytes(StandardCharsets.UTF_8)));
        var request = new ProduceRequest();
        request.setId(producerId);
        request.setToken(stringRequest.getToken());
        request.setTopic(stringRequest.getTopic());
        request.setPartition(stringRequest.getPartition());
        request.setHeaders(headers);
        request.setKey(stringRequest.getKey().getBytes(StandardCharsets.UTF_8));
        request.setValue(stringRequest.getValue().getBytes(StandardCharsets.UTF_8));
        return request;
    }
}
