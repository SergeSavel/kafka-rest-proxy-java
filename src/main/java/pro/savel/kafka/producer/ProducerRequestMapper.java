package pro.savel.kafka.producer;

import pro.savel.kafka.producer.requests.ProducerSendRequest;
import pro.savel.kafka.producer.requests.ProducerSendStringRequest;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class ProducerRequestMapper {

    public static ProducerSendRequest mapProduceRequest(ProducerSendStringRequest stringRequest) {
        var headers = new HashMap<String, byte[]>(stringRequest.getHeaders().size());
        stringRequest.getHeaders().forEach((key, value) -> headers.put(key, value.getBytes(StandardCharsets.UTF_8)));
        var request = new ProducerSendRequest();
        request.setProducerId(stringRequest.getProducerId());
        request.setToken(stringRequest.getToken());
        request.setTopic(stringRequest.getTopic());
        request.setPartition(stringRequest.getPartition());
        request.setHeaders(headers);
        request.setKey(stringRequest.getKey().getBytes(StandardCharsets.UTF_8));
        request.setValue(stringRequest.getValue().getBytes(StandardCharsets.UTF_8));
        return request;
    }
}
