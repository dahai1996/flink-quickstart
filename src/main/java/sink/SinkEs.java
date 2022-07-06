package sink;

import bean.RunEnv;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.client.RestClientBuilder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wfs
 */
public class SinkEs<T> {
    public List<HttpHost> httpHosts = new ArrayList<>(1);
    public ElasticsearchSink.Builder<T> esSinkBuilder;

    /**
     * 获取es sinkFunction
     *
     * @param runEnv 包含执行环境地址的枚举类
     * @param elasticsearchSinkFunction es转化单条数据的逻辑方法
     */
    public SinkEs(RunEnv runEnv, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
        httpHosts.add(new HttpHost(runEnv.getEsHost(), runEnv.getEsPort(), "http"));
        esSinkBuilder = new ElasticsearchSink.Builder<T>(httpHosts, elasticsearchSinkFunction);
        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setBulkFlushMaxSizeMb(1);
        esSinkBuilder.setBulkFlushInterval(5000L);
        esSinkBuilder.setRestClientFactory(
                new RestClientFactory() {
                    @Override
                    public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
                        restClientBuilder.setHttpClientConfigCallback(
                                new RestClientBuilder.HttpClientConfigCallback() {
                                    @Override
                                    public HttpAsyncClientBuilder customizeHttpClient(
                                            HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                        httpAsyncClientBuilder.setKeepAliveStrategy(
                                                new ConnectionKeepAliveStrategy() {
                                                    @Override
                                                    public long getKeepAliveDuration(
                                                            HttpResponse httpResponse,
                                                            HttpContext httpContext) {
                                                        return Duration.ofMinutes(5).toMillis();
                                                    }
                                                });
                                        return httpAsyncClientBuilder;
                                    }
                                });
                    }
                });
    }

    /**
     * 获取es sinkFunction see {@link ElasticsearchSink.Builder}
     *
     * @param runEnv 包含执行环境地址的枚举类
     * @param elasticsearchSinkFunction elasticsearchSinkFunction es转化单条数据的逻辑方法
     * @param bulkFlushMaxActions 刷新前缓冲的最大动作量
     * @param bulkFlushMaxSizeMb 刷新前缓冲区的最大数据大小（以MB为单位）
     * @param bulkFlushInterval 论缓冲操作的数量或大小如何都要刷新的时间间隔
     */
    public SinkEs(
            RunEnv runEnv,
            ElasticsearchSinkFunction<T> elasticsearchSinkFunction,
            int bulkFlushMaxActions,
            int bulkFlushMaxSizeMb,
            Long bulkFlushInterval) {
        httpHosts.add(new HttpHost(runEnv.getEsHost(), runEnv.getEsPort(), "http"));
        esSinkBuilder = new ElasticsearchSink.Builder<T>(httpHosts, elasticsearchSinkFunction);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        esSinkBuilder.setBulkFlushMaxSizeMb(bulkFlushMaxSizeMb);
        esSinkBuilder.setBulkFlushInterval(bulkFlushInterval);
    }

    public ElasticsearchSink<T> getSink() {
        return esSinkBuilder.build();
    }
}
