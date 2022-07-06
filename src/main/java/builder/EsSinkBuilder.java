package builder;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.HttpHost;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkElementIndex;

/**
 * @author wfs see {@link ElasticsearchSink.Builder}
 */
public class EsSinkBuilder<T> {
    private final transient ElasticsearchSink.Builder<T> esSinkBuilder;

    public EsSinkBuilder(
            List<String> esHosts,
            int esPort,
            ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
        checkElementIndex(0, esHosts.size());
        List<HttpHost> httpHosts = new ArrayList<>(1);
        for (String esHost : esHosts) {
            httpHosts.add(new HttpHost(esHost, esPort, "http"));
        }

        this.esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts, elasticsearchSinkFunction);
    }

    public EsSinkBuilder<T> setBulkFlushMaxActions(int value) {
        esSinkBuilder.setBulkFlushMaxActions(value);
        return this;
    }

    public EsSinkBuilder<T> setBulkFlushMaxSizeMb(int value) {
        esSinkBuilder.setBulkFlushMaxSizeMb(value);
        return this;
    }

    public EsSinkBuilder<T> setBulkFlushInterval(long value) {
        esSinkBuilder.setBulkFlushInterval(value);
        return this;
    }

    public EsSinkBuilder<T> setBulkFlushBackoff(boolean value) {
        esSinkBuilder.setBulkFlushBackoff(value);
        return this;
    }

    public EsSinkBuilder<T> setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType value) {
        esSinkBuilder.setBulkFlushBackoffType(value);
        return this;
    }

    public EsSinkBuilder<T> setBulkFlushBackoffRetries(int value) {
        esSinkBuilder.setBulkFlushBackoffRetries(value);
        return this;
    }

    public EsSinkBuilder<T> setBulkFlushBackoffDelay(long value) {
        esSinkBuilder.setBulkFlushBackoffDelay(value);
        return this;
    }

    public EsSinkBuilder<T> setRestClientFactory(RestClientFactory value) {
        esSinkBuilder.setRestClientFactory(value);
        return this;
    }

    public EsSinkBuilder<T> setFailureHandler(ActionRequestFailureHandler value) {
        esSinkBuilder.setFailureHandler(value);
        return this;
    }

    public EsSinkBuilder<T> setKeepAliveDurationMinutes(int value) {
        esSinkBuilder.setRestClientFactory(
                (RestClientFactory)
                        restClientBuilder ->
                                restClientBuilder.setHttpClientConfigCallback(
                                        httpAsyncClientBuilder -> {
                                            httpAsyncClientBuilder.setKeepAliveStrategy(
                                                    (httpResponse, httpContext) ->
                                                            Duration.ofMinutes(value).toMillis());
                                            return httpAsyncClientBuilder;
                                        }));
        return this;
    }

    public SinkFunction<T> build() {
        return esSinkBuilder.build();
    }
}
