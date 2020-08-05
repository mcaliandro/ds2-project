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
    private int maxRetrieveDelay;
    private int longAgo;
    private int L;
    private int H;

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


    // I think that RetrieveEvents and EmitGossip are same.
    // You should Schedule them.
    // For instance, EmitGossip starts at 1, interval 1
    // RetrieveEvents starts at 1.5, interval 1
    public static final class RetrieveEvents implements Serializable {}

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

    // private void RetrieveEvents() {

    // }


    // I have question about this function, could you explain to me???
    private void Broadcast(Event event) {
        events.add(event);
        this.remove_oldest_notifications()
    }

    private void Deliver(Event event) {

    }

    private void remove_oldest_notifications() {
        // out of date
        // boolean found = true;
        // while (events.size() > maxEventsLength && found) {
        //     found = false;
        //     for (int i = 0; i < events.size() - 1 && !found; i++) {
        //         for (int j = i + 1; j < events.size() && !found; j++) {
        //             Event e = events.get(i);
        //             Event e1 = events.get(j);
        //             // we can use event.id to represent generateround
        //             if (e.source == e1.source
        //                     && Math.abs(e.generateround - e1.generateround) > longAgo) { 
        //                 if (e.getGenerationRound() > e1.getGenerationRound()) {
        //                     this.events.remove(e1);
        //                 } else {
        //                     this.events.remove(e);
        //                 }
        //                 found = true;
        //             }
        //         }
        //     }
        // }

        // age
        while (events.size() > maxEventsLength) {
            Event candidate = events.get(0);
            for (int i = 1; i < events.size(); i++) {
                if (candidate.age < events.get(i).age) {    //choose max_age event to remove
                    candidate = events.get(i);
                }
            }
            events.remove(candidate);
        }

    }


    /*** Receive methods ***/

    private void onReceiveRetrieveEvents(RetrieveEvents retrieve) {

        for (BufferedEvent element: retrieveBuffer) {
            if (currentRound - e.round > maxRetrieveDelay) {    // waiting maxRetrieveDelay
                if (! eventsIds.contains(element.id)){  // the event has not been received in the meanwhile
                    Event event = null;
                    if (view.contain(element.gossipSender)) //  ask sender
                    if (event == null) {    //  try with a random one
                        // TODO: random choose in the view
                    }
                    if (event == null && this.allProcesses.containsKey(element.getCreator())) {}
                        // TODO: ask source node
                    if (event != null) { // if the event has arrived, deliver it
                        events.add(event)
                        // TODO: implement function LPBDELIVER(event)
                        eventsIds.add(event.id);

                    }
                } else {
                    retrieveBuffer.remove(element)
                }
            }

        }  
    }

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
            // update age
            for (Event event1 : this.events) {
                    if (event.id.equals(event1.id) && event1.age < event.age) {
                        event1.age = event.age
                    }
            }
        }


        // phase 4: update eventsIds
        for (Long eventId : gossip.eventIds) {
            if (! eventsIds.contains(eventId)) {
                BufferedEvent element = new BufferedEvent(eventId, gossip.sender, currentRound);
                retrieveBuffer.add(element);
            }
        }   


        // phase 5: remove oldest notification
        // I comment out_of_data fuction, 
        // Because in adaptive gossip-based algorithm we just need remove max age
        this.remove_oldest_notifications()

        // phase 6:
        while (eventsIds.size() > maxEventIdsLength) {
            // TODO: implement procedure "remove oldest element from eventIds"
        }



    }

    private void onReceiveCreateEvent(CreateEvent message) {
        Long eventId = System.currentTimeMillis(); // + random salt? what is meaning of that
        Event event = new Event(eventId, message.description, getSelf(), 0);    //   age = 0?
        Broadcast(event);
    }


    /*** Overriding methods ***/

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CreateEvent.class, this::onReceiveCreateEvent)
                .match(EmitGossip.class, this::onReceiveEmitGossip)
                .match(Gossip.class, this::onReceiveGossip)
                .match(RetrieveEvents.class, this::onReceiveRetrieveEvents)
                .build();
    }

}
