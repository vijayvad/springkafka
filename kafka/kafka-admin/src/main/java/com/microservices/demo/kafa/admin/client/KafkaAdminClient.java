package com.microservices.demo.kafa.admin.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.microservices.demo.kafa.admin.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.http.HttpClient;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class KafkaAdminClient {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;

    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;

    private final RetryTemplate retryTemplate;
    private final WebClient webClient;

    public KafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData, AdminClient adminClient, RetryTemplate retryTemplate, WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopics(){
        CreateTopicsResult createTopicsResult;
        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retries",t);
        }
        checkTopicsCreated();
    }
    public void checkTopicsCreated(){
        Collection<TopicListing> topics = getTopics();
        int retry = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleeptime = retryConfigData.getSleepTimeMs();

        for (String topic : kafkaConfigData.getTopicNamesToCreate())
        {
            while(!isTopicCreated(topics,topic))
            {
                checkMaxRetry(retry++,maxRetry);
                sleep(sleeptime);
                sleeptime *=multiplier;
                topics = getTopics();
            }
        }

    }

    public void checkSchemaRegistry(){
        int retry = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();
        Long sleeptime = retryConfigData.getSleepTimeMs();
        while(!getSchemaRegistryStatus().is2xxSuccessful())
        {
            checkMaxRetry(retry++,maxRetry);
            sleep(sleeptime);
            sleeptime *=multiplier;
        }
    }

    private HttpStatus getSchemaRegistryStatus(){
        try {
            return webClient
                    .method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
        } catch (Exception e) {
            return HttpStatus.SERVICE_UNAVAILABLE;
        }
    }

    private void sleep(Long sleeptime) {
        try {
            Thread.sleep(sleeptime);
        } catch (InterruptedException e) {
            throw new RuntimeException("Error in sleeping");
        }
    }

    private void checkMaxRetry(int retry, Integer maxRetry) {
        if(retry> maxRetry)
        {
            throw new KafkaClientException("maximum retry reached");
        }
    }

    private boolean isTopicCreated(Collection<TopicListing> topics, String topic) {

        if(topics == null) {return  false;}
        return topics.stream().anyMatch(t -> t.name().equals(topic));
    }

    private CreateTopicsResult doCreateTopics
            (RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        LOG.info("Number of topics creater :{}",topicNames.size(),
                retryContext.getRetryCount());

        List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumberOfPartitions(),
                kafkaConfigData.getReplicationFactor()))
                .collect(Collectors.toList());
        return adminClient.createTopics(kafkaTopics);

    }
    private Collection<TopicListing> getTopics(){
        Collection<TopicListing> topics;

        try {
            topics = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable t) {
            throw new KafkaClientException("Reached max number of retries",t);
        }
        return topics;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext)
            throws ExecutionException, InterruptedException {

        Collection<TopicListing> topics = adminClient.listTopics()
                .listings().get();

        if(topics != null)
        {
            topics.forEach(t-> LOG.debug("TOPIC NAME :{}",t.name()));
        }

        return topics;
    }

}
