package service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Stream {

    public StreamsBuilder create(StreamsBuilder builder) {

        var jsonDeserializer = new JsonDeserializer();
        var jsonSerializer = new JsonSerializer();
        var jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        var stream1 = builder
                .stream(Config.SOURCE_TOPIC_1, Consumed.with(Serdes.String(), jsonSerde))
                .mapValues(value -> project(value, Projection.parseProjections(Config.SOURCE_TOPIC_1_PROJECTION)));

        var stream2 = builder
                .stream(Config.SOURCE_TOPIC_2, Consumed.with(Serdes.String(), jsonSerde))
                .mapValues(value -> project(value, Projection.parseProjections(Config.SOURCE_TOPIC_2_PROJECTION)));

        stream1.merge(stream2)
                .to(Config.DESTINATION_TOPIC, Produced.with(Serdes.String(), jsonSerde));

        return builder;
    }

    private JsonNode project(JsonNode value, List<Projection> projections) {
        try {
            if (projections.size() == 0) {
                return value;
            }
            var result = new JSONObject();
            projections.forEach(projection -> {
                result.put(projection.destination, value.at(projection.source).asText());
            });
            return new ObjectMapper().readTree(result.toString());
        } catch (Exception e) {
            return value;
        }
    }
}

class Projection {
    public String source;
    public String destination;

    public Projection(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }

    public static List<Projection> parseProjections(String projectionConfig) {
        if (projectionConfig.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.asList(projectionConfig.split(",")).stream()
                .map(x -> new Projection(x.split("->")[0], x.split("->")[1]))
                .collect(Collectors.toList());
    }
}