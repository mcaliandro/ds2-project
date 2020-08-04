package lpbcast;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Process extends AbstractActor {

    /*** Attributes of Process ***/

    // private static final int MAXBUFFER = 100;
    private int maxViewLength;
    private int maxSubsLength;
    private int maxUnsubsLength;
    private int maxEventsLength;
    private int maxEventIdsLength;

    private int id;
    private int currentRound; // TODO: where/when we should update currentRound?
    private Set<Event> events = new HashSet<>();
    private Set<Long> eventsIds = new HashSet<>();
    private Set<ActorRef> subscriptors = new HashSet<>();
    private Set<ActorRef> unsubscriptors = new HashSet<>();
    private Set<ActorRef> view = new HashSet<>();
    private Set<BufferedEvent> retrieveBuffer = new HashSet<>();


    /*** Data classes used in buffers ***/

    public class Event {
        public final long id;
        public final String description;
        public final ActorRef source;
        public int age;
        public Event(long id, String description, ActorRef source, int age) {
            this.id = id;
            this.description = description;
            this.source = source;
            this.age = age;
        }
    }

    public class BufferedEvent {
        public long eventId;
        public ActorRef gossipSender;
        public int round;
        public BufferedEvent(long id, ActorRef sender, int round) {
            this.eventId = id;
            this.gossipSender = sender;
            this.round = round;
        }
    }


    /*** Command classes ***/

    public static final class EmitGossip implements Serializable {}

    public static final class CreateEvent implements Serializable {
        public final String description;
        public CreateEvent(String description) {
            this.description = description;
        }
    }


    /*** Message classes ***/

    public static final class Gossip implements Serializable {
        public final ActorRef sender;
        public final Set<ActorRef> subscriptions;
        public final Set<ActorRef> unsubscriptions;
        public final Set<Event> events;
        public final Set<Long> eventIds;
        public Gossip(ActorRef sender, Set<ActorRef> subs, Set<ActorRef> unsubs, Set<Event> events, Set<Long> eventIds) {
            subs.add(sender);
            this.sender = sender;
            this.subscriptions = Collections.unmodifiableSet(subs);
            this.unsubscriptions = Collections.unmodifiableSet(unsubs);
            this.events = Collections.unmodifiableSet(events);
            this.eventIds = Collections.unmodifiableSet(eventIds);
        }
    }


    /*** Constructor of Process ***/

    public Process(int id) {
        this.id = id;
    }

    public static Props props(int id) {
        return Props.create(Process.class, () -> new Process(id));
    }


    /*** Private methods ***/

    private void RetrieveEvents() {

    }

    private void Broadcast(Event event) {
        events.add(event);
        // TODO: implement function REMOVE_OLDEST_NOTIFICATIONS()
    }

    private void Deliver(Event event) {

    }


    /*** Receive methods ***/

    private void onReceiveEmitGossip(EmitGossip emit) {
        Gossip gossip = new Gossip(getSelf(), subscriptors, unsubscriptors, events, eventsIds);
        Set<ActorRef> fanout = null; // TODO: implement function for "choose F random members in view"
        for (Event event : events) {
            event.age += 1;
        }
        for (ActorRef target : fanout) {
            // TODO: implement procedure for "send(target, gossip)"
        }
        events.clear();
    }

    private void onReceiveGossip(Gossip gossip) {

        // phase 1: update view and unsubs with unsubscriptions
        for (ActorRef unsub : gossip.unsubscriptions) {
            view.remove(unsub);
            subscriptors.remove(unsub);
            unsubscriptors.add(unsub);
        }
        while (unsubscriptors.size() > maxUnsubsLength) {
            // TODO: procedure for "remove random element from unsubs"
        }

        // phase 2: update view with new subscriptions
        for (ActorRef sub : gossip.subscriptions) {
            if (! view.contains(sub)) {
                view.add(sub);
                subscriptors.add(sub);
            }
        }
        while (view.size() > maxViewLength) {
            ActorRef target = null; // TODO: implement function for "get random element in view"
            view.remove(target);
            subscriptors.add(target);
        }
        while (subscriptors.size() > maxSubsLength) {
            // TODO: procedure for "remove random element from subs"
        }

        // phase 3: update events with new notifications
        for (Event event : gossip.events) {
            if (! eventsIds.contains(event.id)) {
                events.add(event);
                // TODO: implement function LPBDELIVER(event)
                eventsIds.add(event.id);
            }
        }
        for (Long eventId : gossip.eventIds) {
            if (! eventsIds.contains(eventId)) {
                BufferedEvent element = new BufferedEvent(eventId, gossip.sender, currentRound);
                retrieveBuffer.add(element);
            }
        }
        while (eventsIds.size() > maxEventIdsLength) {
            // TODO: implement procedure "remove oldest element from eventIds"
        }
        while (events.size() > maxEventsLength) {
            // TODO: implement procedure "remove oldest element from events"
        }

    }

    private void onReceiveCreateEvent(CreateEvent message) {
        Long eventId = System.currentTimeMillis(); // + random salt?
        Event event = new Event(eventId, message.description, getSelf(), 0);
        Broadcast(event);
    }


    /*** Overriding methods ***/

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CreateEvent.class, this::onReceiveCreateEvent)
                .match(EmitGossip.class, this::onReceiveEmitGossip)
                .match(Gossip.class, this::onReceiveGossip)
                .build();
    }

}
