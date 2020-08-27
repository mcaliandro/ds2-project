package agb_bcast;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import com.typesafe.config.Config;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;


public class SystemNode extends AbstractActor {

    /* Attributes */

    private final String id;
    
    // general configurations about the system, and configurations of the adaptive gossip-based broadcast
    private Config systemConfig;
    
    // track the round of gossip protocol
    private int gossipRound;
    
    // size of local buffers
    private int buffersSize;
    
    // track the event broadcast frequency of new events
    private int broadcasts;
    
    // parameters for: resource availability, local congestion estimation, rate adaption
    private int s;				// sample period for distributed discovery of available resources
    private int minBuff;		// minimum buffer size of distributed available resources
    private int tokens;			// parameter used to bound the load of the system
    private double avgAge;		// average age of discarded events during local congestion estimation
    private double avgTokens;	// average number of available tokens
    private double rate;		// sender rate parameter for congestion adaptation 
    
    // queue of events to broadcast when the system is not congested
    private Queue<String> broadcastQueue = new LinkedList<>();

    // history about minimum local buffers size of the node
    private List<Integer> minBuffHistory = new ArrayList<>();
    
    // set of discarded events during the congestion estimation procedure
    private HashMap<String,Integer> lost = new HashMap<>();
    
    // buffers
    private Map<String,Integer> events = new HashMap<>();	// buffer of the recent system events
    private Map<String,Integer> eventIds = new HashMap<>();	// buffer of delivered events
    private List<String> retrieveBuf = new ArrayList<>();	// buffer of non-delivered events
    private List<ActorRef> subs = new ArrayList<>();		// buffer of recent joining nodes
    private List<ActorRef> unsubs = new ArrayList<>();		// buffer of recent leaving nodes
    private List<ActorRef> view = new ArrayList<>();		// buffer of the partial view of the system
       
    // actor receivers for managing actor status
    private AbstractActor.Receive ready;	// actor becomes ready
    private AbstractActor.Receive paused;	// actor becomes paused
    private AbstractActor.Receive started;	// actor becomes started
    
    // Akka schedulers
    private Cancellable newEventTimer;		// schedule the creation of new event
    private Cancellable nextPeriodTimer;	// schedule the next estimation period 
    private Cancellable nextGossipTimer;	// schedule the next gossip round
    private Cancellable restoreTokensTimer;	// schedule the tokens restore procedure
    
    // Akka Logging module
    private LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    
    
    /* Messages */

    // to node itself
    private static class CreateNewEvent implements Serializable {}		// create new event command
    private static class NextGossipRound implements Serializable {}		// next gossip round command
    private static class NextSamplePeriod implements Serializable {}	// next sample period command
    private static class RestoreTokens implements Serializable {}		// restore tokens command
	
	// from node manager
    public static class Pause implements Serializable {}	// command to pause the node execution
    public static class Resume implements Serializable {}	// command to resume the node execution
    
	public static class Start implements Serializable {		// command to initialize and start the node
		public final Config config;
		public final List<ActorRef> partialView;
		public Start(Config config, List<ActorRef> partialView) {
			this.config = config;
			this.partialView = Collections.unmodifiableList(partialView);
		}
	}
    
    // from other nodes
    public static class Gossip implements Serializable {	// gossip message
    	public final ActorRef sender;
    	public final int s;
    	public final int minBuff;
        public final Map<String,Integer> events;
        public final Map<String,Integer> eventIds;
        public final List<ActorRef> subs;
        public final List<ActorRef> unsubs;
        public Gossip(ActorRef sender, int s, int minBuff,
        		Map<String,Integer> events, Map<String,Integer> eventIds, 
        		List<ActorRef> subs, List<ActorRef> unsubs) {
        	subs.add(sender);
        	this.sender = sender;
        	this.s = s;
        	this.minBuff = minBuff;
        	this.events = Collections.unmodifiableMap(events);
            this.eventIds = Collections.unmodifiableMap(eventIds);
            this.subs = Collections.unmodifiableList(subs);
            this.unsubs = Collections.unmodifiableList(unsubs);
        }
    }
    
    
    /* Constructor */

    private SystemNode() {
        id = getSelf().path().name();
        
        // initially accept start message only
        ready = receiveBuilder()
                .match(Start.class, this::onReceiveStart)
                .build();
        
        // accept resume message when the actor becomes paused
        paused = receiveBuilder()
                .match(Resume.class, this::onReceiveResume)
                .build();
        
        // accept these message only when the actor becomes started
        started = receiveBuilder()
        		// accept messages from node itself
        		.match(CreateNewEvent.class, this::onReceiveCreateNewEvent)
                .match(NextGossipRound.class, this::onReceiveNextGossipRound)
                .match(NextSamplePeriod.class, this::onReceiveNextSamplePeriod)
                .match(RestoreTokens.class, this::onReceiveRestoreTokens)
                // accept messages from other SystemNode instances
                .match(Gossip.class, this::onReceiveGossip)
                // accept messages from node manager
                .match(Pause.class, this::onReceivePause)
                .build();
    }

    public static Props props() {
        return Props.create(SystemNode.class, () -> new SystemNode());
    }
    

    /* Private methods */
    
    private String snapshotGossipMessage(Gossip msg) {
    	return "\n GOSSIP MESSAGE CONTENT" +
    			"\n\t sender: " + msg.sender.path().name() + 
    			"\n\t s: " + s +
    			"\n\t minBuff: " + minBuff + 
    			"\n\t events: " + events.toString() +
    			"\n\t eventIds: " + eventIds.toString() +
    			"\n\t subs: " + subs.toString() +
    			"\n\t unsubs: " + unsubs.toString();
    }
    
    private String snapshotNodeState() {
    	return "\n ACTOR PROPERTIES" + 
    			"\n\t id: " + this.id + 
    		    "\n\t buffersSize: " + buffersSize +
    		    "\n GOSSIP PARAMETERS" +
    			"\n\t gossipRound: " + gossipRound +
    			"\n\t events: " + events.toString() +
    			"\n\t eventIds: " + eventIds.toString() +
    			"\n\t retrieveBuf: " + retrieveBuf.toString() +
    			"\n\t subs: " + subs.toString() +
    			"\n\t unsubs: " + unsubs.toString() + 
    			"\n\t view: " + view.toString() + 
    			"\n BROADCASTS INFO" +
    		    "\n\t broadcasts: " + broadcasts +
    		    "\n\t broadcastQueue: " + broadcastQueue.toString() + 
    		    "\n ADAPTIVE BCAST PARAMETERS" +
    		    "\n\t s: " + s +
    		    "\n\t minBuff: " + minBuff + 
    		    "\n\t tokens: " + tokens +
    		    "\n\t avgAge: " + avgAge + 
    		    "\n\t avgTokens: " + avgTokens +
    		    "\n\t rate: " + rate +
    		    "\n\t minBuffHistory: " + minBuffHistory.toString() + 
    		    "\n\t lost: " + lost.toString();
    }
    
    private Cancellable setupOnceScheduler(long delay, Serializable message) {
    	return getContext().getSystem().scheduler().scheduleOnce(
    			Duration.ofMillis(delay), getSelf(), message, 
    			getContext().getSystem().dispatcher(), getSelf());
    }
    
    private Cancellable setupPeriodicScheduler(long delay, Serializable message) {
    	return getContext().getSystem().scheduler().scheduleWithFixedDelay(
    			Duration.ofMillis(delay), Duration.ofMillis(delay), getSelf(), message, 
				getContext().getSystem().dispatcher(), getSelf());
    }
    
    
    /* Override createReceiver method */
    
    @Override
    public Receive createReceive() {
    	// after creation, actor is ready status and accepts only messages for that status
        return ready;
    }

    
    /* LPBcast primitives */
    
    private void Broadcast(String eventId) {
    	// every time the node broadcasts a new event, calculate avgTokens
    	avgTokens = (avgTokens + tokens / (++broadcasts)); // update broadcast frequency
    	
    	// while some tokens are available, first broadcast enqueued past events (if they exist)
    	while (tokens > 0 && broadcastQueue.size() > 0) {
    		String pastEventId = broadcastQueue.poll();
    		events.putIfAbsent(pastEventId, 0);
	    	eventIds.putIfAbsent(pastEventId, gossipRound);
    	}
    	// if there is an available token, then use it to broadcast the new event
    	if (tokens > 0) {
    		tokens--;
	    	events.putIfAbsent(eventId, 0);
	    	eventIds.putIfAbsent(eventId, gossipRound);
	        RemoveOldestNotifications();
    	}
    	// otherwise, enqueue the event and wait for an available token
    	else {
    		broadcastQueue.add(eventId);
    	}
    }

    private void Deliver(String eventId) {
    	// do nothing
    }
    
    private void RemoveOldestNotifications() {
    	// remove oldest notifications by their age
        while (events.size() > buffersSize) {
        	Map.Entry<String,Integer> oldest = events.entrySet().iterator().next();
            for (Map.Entry<String,Integer> entry : events.entrySet()) {
            	if (entry.getValue() > oldest.getValue()) {
            		oldest = entry;
            	}
            }
            // remove oldest event from events
            events.remove(oldest.getKey());
        }
    }
    
    
    /* On-Receive messages from node manager */

    private void onReceiveStart(Start msg) {
    	log.info("{}: INITIALIZE AND START", this.id);
    	getContext().become(started); // switch the actor to "started" state
    	gossipRound = 0; // set current gossip round - initially at 0
    	broadcasts = 0; // set the number of generated events - initially at 0
    	avgTokens = 0; // set the average token availability - initially at 0
    	systemConfig = msg.config; // load system configurations
    	
    	// update the partial view buffer
       	view = new ArrayList<>(msg.partialView);
    	
    	// configure the local buffers size with a random number
    	buffersSize = systemConfig.getInt("system.min-buffer") + new Random()
    			.nextInt(systemConfig.getInt("system.max-buffer") - systemConfig.getInt("system.min-buffer"));
    	
    	// initialize the period and the buffer estimation parameters
    	s = systemConfig.getInt("adaptive.delta");
    	minBuffHistory.add(0, 0);
    	for (int r = 1; r <= s; r++) {
    		minBuffHistory.add(r, buffersSize);
    	}
    	
    	// initialize minBuff with the local buffers size
    	minBuff = buffersSize;
    	
    	// initialize the emission rate with W
    	rate = systemConfig.getDouble("adaptive.W");
    	
    	// initialize the average age of discarded messages -> avgAge = (H + L) / 2
    	avgAge = (systemConfig.getInt("adaptive.H") + systemConfig.getInt("adaptive.L")) / 2;
    	
    	// initialize tokens with the maximum available
    	tokens = systemConfig.getInt("adaptive.max");
    	
    	// schedule the procedure to start a new gossip round -> every T ms
    	nextGossipTimer = setupPeriodicScheduler(systemConfig.getLong("gossip.T"), new NextGossipRound());
    	
    	// schedule the procedure to enter into a new estimation period -> every S ms
    	nextPeriodTimer = setupPeriodicScheduler(systemConfig.getLong("adaptive.S"), new NextSamplePeriod());
    	
    	// schedule the procedure to generate new events -> after <random> ms
    	long randomInterval = systemConfig.getInt("system.min-delay") + new Random()
    			.nextInt(systemConfig.getInt("system.max-delay") - systemConfig.getInt("system.min-delay"));
    	newEventTimer = setupOnceScheduler(randomInterval, new CreateNewEvent());
    }
    
    private void onReceivePause(Pause msg) {
    	log.info("{} PAUSED", this.id);
    	getContext().become(paused);	// switch the actor to "paused" state
    	
    	// cancel schedulers
    	restoreTokensTimer.cancel();	// cancel restore tokens timeout
    	nextGossipTimer.cancel();		// cancel gossip timeout
    	nextPeriodTimer.cancel();		// cancel sample period timeout
    	newEventTimer.cancel();			// cancel event generation timeout
    }
    
    private void onReceiveResume(Resume msg) {
    	log.info("{} RESUMED", this.id);
    	getContext().become(started);	// switch the actor to "started" state
    	
    	// reconfigure schedulers for starting a new gossip round -> every T ms
    	nextGossipTimer = setupPeriodicScheduler(systemConfig.getLong("gossip.T"), new NextGossipRound());
    	
    	// reconfigure schedulers for entering into a new estimation period -> every S ms
    	nextPeriodTimer = setupPeriodicScheduler(systemConfig.getLong("adaptive.S"), new NextSamplePeriod());
    	
    	// reconfigure schedulers for generating new events -> after <random> ms
    	long randomInterval = systemConfig.getInt("system.min-delay") + new Random()
    			.nextInt(systemConfig.getInt("system.max-delay") - systemConfig.getInt("system.min-delay"));
    	newEventTimer = setupOnceScheduler(randomInterval, new CreateNewEvent());
    }
    
    
    /* On-Receive messages from node itself */
    
    private void onReceiveCreateNewEvent(CreateNewEvent msg) {
    	String eventId = String.format("%s-%d", this.id, System.currentTimeMillis());
    	log.info("{}: CREATE NEW EVENT (id={})", this.id, eventId);
    	
    	Broadcast(eventId); // broadcast new event
    	
        // configure the scheduler for the next event creation -> after <random> ms
    	long randomInterval = systemConfig.getInt("system.min-delay") + new Random()
    			.nextInt(systemConfig.getInt("system.max-delay") - systemConfig.getInt("system.min-delay"));
        newEventTimer = setupOnceScheduler(randomInterval, new CreateNewEvent());
    }
    
    private void onReceiveNextSamplePeriod(NextSamplePeriod msg) {
    	s++; // enter new period
    	log.info("{}: ENTER NEW SAMPLE PERIOD (s={})", this.id, s);
    	
    	minBuffHistory.add(s, buffersSize); // add the local buffers size to the minBuff history
    	
    	// determine the local minimum buffer size of the last delta periods
    	// pseudo-code: minBuff := min { <minBuff of p at period (s-delta+1)>, ..., <minBuff of p at period s> }
    	minBuff = Collections.min(minBuffHistory.subList(s-systemConfig.getInt("adaptive.delta")+1, s+1));
    }
    
    private void onReceiveNextGossipRound(NextGossipRound msg) {
    	gossipRound++; // new gossip round
    	log.info("{}: EMIT GOSSIP (round={})", this.id, gossipRound);
    	
    	// add information to gossip message    	
    	Gossip gossip = new Gossip(getSelf(), s, minBuffHistory.get(s), 
    			new HashMap<>(events), new HashMap<>(eventIds),
    			new ArrayList<>(subs), new ArrayList<>(unsubs));
    	
    	// determine the gossip subset (fan-out)
    	List<ActorRef> fanout = new ArrayList<>(view);
    	Collections.shuffle(fanout);
    	fanout = fanout.subList(0, systemConfig.getInt("gossip.F"));
    	
        // update the age for all the events
        events.forEach((eventId, age) -> age++);
        
        // remove oldest events with age > k
        events.entrySet().removeIf(entry -> entry.getValue() > systemConfig.getInt("gossip.k"));
        
        // emit gossip 
        fanout.forEach(target -> target.tell(gossip, getSelf()));
        
        /* Throttle sender */
        // when new system resources are released, increase the emission rate by rH
        if ((avgAge > systemConfig.getInt("adaptive.H")) &&						// if (avgAge > H AND
        	(avgTokens > (systemConfig.getInt("adaptive.max") / 2)) &&			//     avgTokens > max AND
        	(rate > systemConfig.getDouble("adaptive.W"))) {					//     rate > W)
        	rate *= (1 + systemConfig.getDouble("adaptive.rH"));				// then rate = rate * (1 + rH)
        }
        // when the system is congested, reduce the emission rate by rL
        if ((avgAge < systemConfig.getInt("adaptive.L")) || 					// if (avgAge < L OR
        	(avgTokens > (systemConfig.getInt("adaptive.max") / 2))) {			//     avgTokens > max/2)
        	rate *= (1 - systemConfig.getDouble("adaptive.rL"));				// then rate = rate * (1 - rL)
        }
        
        // configure the scheduler for the next tokens restore procedure -> after (1/rate) * 100 ms
        restoreTokensTimer = setupOnceScheduler((long) (1 / rate) * 100, new RestoreTokens());
        
        // if debug logging is enabled, show the state of the actor in logs
        if (log.isDebugEnabled()) { 
    		log.debug("{}: SNAPSHOT NODE STATE {}", this.id, snapshotNodeState());
    	}
    }
    
    private void onReceiveRestoreTokens(RestoreTokens msg) {
    	log.info("{}: RESTORE TOKENS", this.id);
    	
    	// restore tokens 
    	if (tokens < systemConfig.getInt("adaptive.max")) {
    		tokens++;
    	}
    }
    
    
    /* On-Receive messages from other nodes */
    
    private void onReceiveGossip(Gossip gossip) {
    	log.info("{}: RECEIVE GOSSIP FROM {}", this.id, getSender().path().name());
    	
    	// if debug logging is enabled, show info about the received gossip in logs
    	if (log.isDebugEnabled()) { 
    		log.debug("{}: SNAPSHOT GOSSIP MESSAGE {}", this.id, snapshotGossipMessage(gossip));
    	}
    	
        /* Phase 1: update local view and local unsubs with gossip.unsubs */
        for (ActorRef unsub : gossip.unsubs) {
        	view.remove(unsub);
            subs.remove(unsub);
            if (! unsubs.contains(unsub)) {
            	unsubs.add(unsub);
            }
        }
        // while unsubs exceeds the max buffer size, remove a random element
        while (unsubs.size() > buffersSize) {
            ActorRef target = unsubs.get(new Random().nextInt(unsubs.size()));
            unsubs.remove(target);
        }

        /* Phase 2: update local view with gossip.subs */
        for (ActorRef sub : gossip.subs) {
        	if (sub != getSelf() && (! view.contains(sub))) {
                view.add(sub);
                if (! subs.contains(sub)) {
                	subs.add(sub);
                }
            }
        }
        // while view exceeds the max buffer size, remove a random element
        while (view.size() > buffersSize) {
            ActorRef target = view.get(new Random().nextInt(view.size()));
            view.remove(target);
            // put the element inside subs
            if (! subs.contains(target)) {
            	subs.add(target);
            }
        }
        // while subs exceeds the max buffer size, remove a random element
        while (subs.size() > buffersSize) {
            ActorRef target = subs.get(new Random().nextInt(subs.size()));
            subs.remove(target);
        }
        
        /* Phase 3: update events with new notifications */
        for (Map.Entry<String,Integer> entry : gossip.events.entrySet()) {
        	String eventId = entry.getKey();
        	Integer eventAge = entry.getValue();
        	// if the new notification has not been delivered, then deliver it and buffer the event
        	if (! eventIds.containsKey(eventId)) {
        		events.putIfAbsent(eventId, eventAge);
        		Deliver(eventId);
        		eventIds.put(eventId, gossipRound);
            }
        	// if the new notification has been buffered, then update its age
        	if (events.containsKey(eventId) && (events.get(eventId) < eventAge)) {
        		events.put(eventId, eventAge);
        	}
        }
        // buffer those events that have been delivered by the gossip sender but not by this node 
        for (String eventId : gossip.eventIds.keySet()) {
        	if (! eventIds.containsKey(eventId)) {
        		retrieveBuf.add(eventId);
        	}
        }
        
        /* Garbage collect eventIds (delivered events) */
        while (eventIds.size() > buffersSize) {
        	Map.Entry<String,Integer> oldest = eventIds.entrySet().iterator().next();
            for (Map.Entry<String,Integer> entry : eventIds.entrySet()) {
            	if (entry.getValue() > oldest.getValue()) {
            		oldest = entry;
            	}
            }
            // remove oldest event from eventIds
            eventIds.remove(oldest.getKey());
        }
        
        /* Update congestion estimate */
        double alpha = systemConfig.getDouble("adaptive.alpha");
        Map<String,Integer> events_lost = new HashMap<>(events);		// clone events
        events_lost.keySet().removeIf(key -> lost.containsKey(key));	// "events disjoint lost" set
        // while "events disjoint lost" set exceeds minBuff, remove the oldest element and add it to "lost" set
        while (events_lost.size() > minBuff) {
        	// identify the oldest element in "events disjoint lost" set
        	Map.Entry<String,Integer> oldest = events_lost.entrySet().iterator().next();
            for (Map.Entry<String,Integer> entry : events_lost.entrySet()) {
            	if (entry.getValue() > oldest.getValue()) {
            		oldest = entry;
            	}
            }
            // determine the average age of discarded events caused by the congestion estimation
        	avgAge = (alpha * avgAge) + ((1 - alpha) * oldest.getValue());
        	
        	lost.put(oldest.getKey(), oldest.getValue()); // add oldest to "lost" set
        	events_lost.remove(oldest.getKey()); // remove oldest from "events disjoint lost" set
        }
        
        /* Garbage collect events */
        RemoveOldestNotifications();
        
        /* Compute new known minimum buffer size */
        if ((gossip.s == this.s) && (gossip.minBuff < this.minBuffHistory.get(s))) {
        	minBuffHistory.set(s, gossip.minBuff);
        }
    }
    
}
