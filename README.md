# experiment

e com.open.messaging.bigbro.broker.manager;

import com.open.messaging.bigbro.broker.BigQueueTextMessage;
import com.open.messaging.bigbro.broker.dto.DeliveryAcknowledgement;
import com.open.messaging.bigbro.broker.dto.QueueConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.channel.ChannelOption;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import java.time.Duration;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.retry.Retry;

@Slf4j
public class ReactiveMessageHandler implements MessageHandler {

  private final BigQueueManager manager;
  private final MeterRegistry meterRegistry;
  private final WebClient webClient;

  public ReactiveMessageHandler(BigQueueManager manager, MeterRegistry meterRegistry) {
    this.manager = manager;
    this.meterRegistry = meterRegistry;

    ConnectionProvider provider =
        ConnectionProvider.builder("consumer-pool")
            .maxConnections(500) // allow more concurrent connections
            .pendingAcquireMaxCount(5000) // allow larger waiting queue
            .maxIdleTime(Duration.ofSeconds(5))
            .maxLifeTime(Duration.ofSeconds(10))
            .pendingAcquireTimeout(Duration.ofSeconds(5))
            .lifo()
            .build();

    HttpClient httpClient =
        HttpClient.create(provider)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
            .responseTimeout(Duration.ofSeconds(2));

    this.webClient =
        WebClient.builder()
            .defaultHeader("Content-Type", "application/x-protobuf")
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            .build();
  }

  private static boolean isRetryableConnectionError(Throwable ex) {
    Throwable cause = ex;
    while (cause != null) {
      String msg = cause.getMessage();
      if (cause instanceof ConnectException
          || (msg != null && msg.contains("Connection reset"))
          || (msg != null && msg.contains("Connection refused"))) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  @Override
  public void handle(
      BigQueueTextMessage message, QueueConsumer request, Consumer<DeliveryAcknowledgement> ack) {
    MDC.put("messageid", message.getMessageID());
    String queue = request.getQueue();

    try {
      String callbackUrl = request.getCallbackUrl();

      webClient
          .post()
          .uri(callbackUrl)
          .header("queue", queue)
          .contentType(MediaType.APPLICATION_PROTOBUF)
          .bodyValue(message.toByteArray())
          .exchangeToMono(
              response -> {
                if (response.statusCode().is2xxSuccessful()) {
                  meterRegistry.counter("queue.consumer.total", "queue", queue).increment();
                  if (ack != null)
                    ack.accept(
                        new DeliveryAcknowledgement(
                            true,
                            message.getMessageID(),
                            String.valueOf(response.statusCode().value())));
                  return Mono.empty();
                } else {
                  meterRegistry.counter("queue.consumer.errors", "queue", queue).increment();
                  if (ack != null)
                    ack.accept(
                        new DeliveryAcknowledgement(
                            false,
                            message.getMessageID(),
                            String.valueOf(response.statusCode().value())));
                  if (response.statusCode() == HttpStatus.NOT_FOUND
                      || response.statusCode() == HttpStatus.NOT_IMPLEMENTED
                      || response.statusCode() == HttpStatus.SERVICE_UNAVAILABLE) {
                    manager.deliverToDeadLetter(queue, message.toByteArray());
                  }
                  return Mono.empty();
                }
              })
          .retryWhen(
              Retry.backoff(1, Duration.ofMillis(200))
                  .filter(ex -> isRetryableConnectionError(ex))
                  .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
          .doOnError(
              ex -> {
                meterRegistry.counter("queue.consumer.errors", "queue", queue).increment();

                if (ex instanceof ConnectException
                    || ex.getCause() instanceof ConnectException
                    || (ex.getMessage() != null && ex.getMessage().contains("Connection reset"))) {
                  log.error(
                      "Connection timeout/reset for queue [{}], message [{}]",
                      queue,
                      message.getMessageID());
                  manager.deliverToDeadLetter(queue, message.toByteArray());
                  if (ack != null)
                    ack.accept(
                        new DeliveryAcknowledgement(
                            false, message.getMessageID(), ex.getMessage()));

                } else if (ex instanceof SocketTimeoutException) {
                  log.warn(
                      "Read timeout for queue [{}], message [{}]: {}",
                      queue,
                      message.getMessageID(),
                      ex.getMessage());
                  if (ack != null)
                    ack.accept(
                        new DeliveryAcknowledgement(
                            true, message.getMessageID(), "accepted unsure: " + ex.getMessage()));

                } else if (ex instanceof WebClientResponseException wex) {
                  log.error(
                      "HTTP error [{}] for queue [{}], message [{}]",
                      wex.getStatusCode(),
                      queue,
                      message.getMessageID());
                  if (ack != null)
                    ack.accept(
                        new DeliveryAcknowledgement(
                            false, message.getMessageID(), wex.getMessage()));

                } else if (ex instanceof UnresolvedAddressException
                    || ex.getCause() instanceof UnresolvedAddressException) {
                  log.error("DNS resolution failed for queue [{}]: {}", queue, ex.getMessage());
                  manager.deliverToDeadLetter(queue, message.toByteArray());
                  if (ack != null)
                    ack.accept(
                        new DeliveryAcknowledgement(
                            false, message.getMessageID(), ex.getMessage()));

                } else {
                  log.error(
                      "General error for queue [{}], message [{}]: {}",
                      queue,
                      message.getMessageID(),
                      ex.getMessage());
                  if (ack != null)
                    ack.accept(
                        new DeliveryAcknowledgement(
                            false, message.getMessageID(), ex.getMessage()));
                }
              })
          .subscribe(null, ex -> log.debug("Error already handled: {}", ex.toString()));

    } catch (Exception e) {
      log.error(
          "Unhandled exception for queue [{}], message [{}]: {}",
          request.getQueue(),
          message.getMessageID(),
          e.getMessage(),
          e);
      if (ack != null) {
        ack.accept(new DeliveryAcknowledgement(false, message.getMessageID(), e.getMessage()));
      }
    } finally {
      MDC.remove("messageid");
    }
  }
}