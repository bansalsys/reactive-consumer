package com.example.reactiveconsumer;

import com.example.reactiveconsumer.EventSink.Events;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import java.util.List;

@EnableScheduling
@SpringBootApplication
public class ReactiveConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReactiveConsumerApplication.class, args);
    }


}

@Component
@Slf4j
@RequiredArgsConstructor
class EventProducer {

    private final EventSink eventSink;

    @Scheduled(fixedRate = 10L)
    public void pollForMockEvents() {

        var events = WebClient.builder()
                              .baseUrl("http://localhost:8080")
                              .build()
                              .get()
                              .uri("events")
                              .retrieve()
                              .toEntity(Events.class);

        eventSink.queueRequest(events);
    }
}

@Component
@Slf4j
@RequiredArgsConstructor
class EventSink {

    private final Sinks.Many<Object> downstreamSink = Sinks.many().unicast().onBackpressureBuffer();


    @PostConstruct
    void addSubscribers() {
        if (downstreamSink.currentSubscriberCount() == 0) {
            subscribeToSink();
        }
    }

    public void queueRequest(Mono<ResponseEntity<Events>> events) {
        events.doOnNext(this::emitEvent).block();

    }

    private void emitEvent(ResponseEntity<Events> eventsResponseEntity) {
        eventsResponseEntity.getBody().getEvents().stream().parallel().forEach(event -> {
            final boolean result = this.downstreamSink.tryEmitNext(event).isSuccess();
            if (!result) {
                log.warn("Emit to the sink failed, items {}", event);
            } else {
                log.debug("Emit to the sink successful, items {}", event);
            }
        });
    }


    private void subscribeToSink() {
        downstreamSink.asFlux()
                      .parallel(5)
                      .runOn(Schedulers.boundedElastic())
                      .log()
                      .subscribe(inputItems -> {
                          log.warn("Received Object " + inputItems);
                          try {
                              Thread.sleep(10000);
                          } catch (InterruptedException e) {
                              e.printStackTrace();
                          }
                      });
    }


    @Data
    public static class Events {

        List<Event> events;

        @Data
        @ToString
        static class Event {

            String name;
            String value;
            Integer id;

        }
    }

}

