  public Mono<String> publish(String queue, Message _message) {

    BrokerMessage brokerMessage = BrokerMessage.fromMessage(_message);
    BigQueueTextMessage message = BrokerMessage.toBigQueueTextMessage(brokerMessage);

    Map<String, JsonNode> allocations = cachedAllocations.get();
    String brokerUrl = this.brokerUrl;

    if (!CollectionUtils.isEmpty(allocations)) {
      JsonNode jsonNode = allocations.get(queue);
      if (jsonNode != null) {
        String host = jsonNode.get("meta").get("node").get("host").asText();
        int port = jsonNode.get("meta").get("node").get("port").asInt();
        brokerUrl = "http://" + host + ":" + port;
      }
    }

    String url = brokerUrl + "/api/queue/publish/" + queue;

    return webClient
        .post()
        .uri(url)
        .bodyValue(message.toByteArray())
        .retrieve()
        // --- Handle 4xx/503/504/502 as retryable ---
        .onStatus(
            status ->
                status.is4xxClientError()
                    || status == HttpStatus.SERVICE_UNAVAILABLE
                    || status == HttpStatus.BAD_GATEWAY
                    || status == HttpStatus.GATEWAY_TIMEOUT,
            clientResponse ->
                clientResponse
                    .bodyToMono(String.class)
                    .flatMap(
                        errorBody ->
                            Mono.error(
                                new BrokerRetryableException(
                                    "Retryable error: "
                                        + clientResponse.statusCode()
                                        + " - "
                                        + errorBody))))
        // --- Handle 5xx as non-retryable ---
        .onStatus(
            HttpStatusCode::is5xxServerError,
            clientResponse ->
                clientResponse
                    .bodyToMono(String.class)
                    .flatMap(
                        errorBody ->
                            Mono.error(
                                new BrokerNonRetryableException(
                                    "Non-retryable error: "
                                        + clientResponse.statusCode()
                                        + " - "
                                        + errorBody))))
        .bodyToMono(String.class)
        .doOnError(
            ex ->
                log.error(
                    "Error before mapping - Type: {}, Message: {}",
                    ex.getClass().getName(),
                    ex.getMessage(),
                    ex))
        // --- Map low-level transport and I/O errors ---
        .onErrorMap(
            throwable -> {
              if (throwable instanceof BrokerRetryableException
                  || throwable instanceof BrokerNonRetryableException) {
                return throwable;
              }

              Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;

              if (cause instanceof BrokerRetryableException
                  || cause instanceof BrokerNonRetryableException) {
                return cause;
              }

              // Connection failures (e.g. refused, timed out)
              if (cause instanceof java.net.ConnectException) {
                return new BrokerRetryableException(
                    "Connection failed: " + cause.getMessage(), cause);
              }

              // Handle Reactor Netty native exceptions
              if (cause instanceof io.netty.channel.unix.Errors.NativeIoException
                  || cause instanceof java.net.SocketException
                  || cause instanceof java.io.IOException) {

                String msg = cause.getMessage() != null ? cause.getMessage() : "";

                // Transient network conditions
                if (msg.contains("Connection reset")
                    || msg.contains("Broken pipe")
                    || msg.contains("Connection refused")
                    || msg.contains("recvAddress")) {
                  return new BrokerRetryableException("Transient network error: " + msg, cause);
                }
              }

              return new BrokerNonRetryableException(
                  "Unexpected error: " + throwable.getMessage(), throwable);
            })
        // --- Retry policy for retryable exceptions ---
        .retryWhen(
            Retry.backoff(3, Duration.ofSeconds(2))
                .filter(ex -> ex instanceof BrokerRetryableException)
                .onRetryExhaustedThrow(
                    (spec, signal) ->
                        new BrokerNonRetryableException(
                            "Retries exhausted: " + signal.failure().getMessage(),
                            signal.failure())))
        // Optional: log on retry
        .doOnError(
            ex -> {
              if (ex instanceof BrokerRetryableException) {
                log.error("[WARN] Retrying publish due to transient error: " + ex.getMessage());
              } else {
                log.error("[ERROR] Publish failed permanently: " + ex.getMessage());
              }
            });
  }


how to call
  Message msg = new Message();
msg.setBody("Hello — I am a single message!");

// reactive pipeline — no block()
Mono<String> publishMono = client.publish("myQueue", msg)
    .doOnSuccess(result -> 
        log.info("✔ Publish acknowledged: {}", result)
    )
    .doOnError(error -> {
        if (error instanceof BrokerRetryableException) {
            log.warn("⚠ Retriable failure detected inside pipeline: {}", error.getMessage());
        } else if (error instanceof BrokerNonRetryableException) {
            log.error("❌ Non-retryable failure detected inside pipeline: {}", error.getMessage());
        } else {
            log.error("❗ Unknown pipeline error: {}", error.getMessage(), error);
        }
    });

// subscribe without blocking — triggers pipeline execution
publishMono.subscribe(
    result -> log.debug("✔ Subscriber got publish success: {}", result),
    error -> {
        if (error instanceof BrokerRetryableException) {
            log.warn("⚠ Final failure AFTER RETRIES (initially retriable): {}", error.getMessage());
        } else if (error instanceof BrokerNonRetryableException) {
            log.error("❌ Publish failed and NOT RETRIABLE: {}", error.getMessage());
        } else {
            log.error("❗ Unexpected publish failure: {}", error.getMessage(), error);
        }
    }
);
