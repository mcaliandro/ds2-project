package agb_bcast;

import java.io.Serializable;
import java.util.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class Main {

	private static final Config systemConf = ConfigFactory.parseResources("system.conf");
	
	private static final ActorSystem system = ActorSystem.create(systemConf.getString("system.name"));	
	private static ArrayList<ActorRef> nodes = new ArrayList<>();
	
	private static boolean running = true;
	
	
	public static void main(String[] args) {
		boolean keepWorking = true;
		
		Scanner keyboard = new Scanner(System.in);
		String choice = new String();
				
		// create the system nodes - wait for the initialization to start
		create_system();
		
		System.out.println("\n>>> Press any key to start the system <<<");
		keyboard.nextLine();
		
		// initialize system nodes and start the gossip protocol
		start_system();
		System.out.println("\n=== SYSTEM IS STARTED ===");
		
		do {
			System.out.println("\n=== MAIN MENU ===");
			
			if (running)
				System.out.println("-> Press \"s\" to pause the system");
			else
				System.out.println("-> Press \"s\" to resume the system");
			
			System.out.println("-> Press \"q\" to terminate the system");
			System.out.println("\n=== WAITING FOR USER INPUT ===\n");
			choice = keyboard.nextLine();
			
			switch (choice) {
			case "s": 
				commute_system(); 
				break;
			case "q": 
				keepWorking = false; 
				break;
			default:
				// no valid choice, just print again the menu
				break;
			}
			
		} while (keepWorking);
		
		// terminate the system
		system.terminate();
		
		System.out.println("\n=== SYSTEM IS TERMINATED ===\n");
		keyboard.close();
	}
	
	
	/* system modifiers */
	
	private static void create_system() {
		// create the nodes of the system
		for (int id=0; id<systemConf.getInt("system.nodes"); id++)
			nodes.add(system.actorOf(SystemNode.props(), "NODE"+id));
	}
	
	private static void start_system() {
		List<ActorRef> partialView = new ArrayList<>();

		// configure the partial view for each node in the system
		for (ActorRef node : nodes) {
			partialView = new ArrayList<>(nodes); // clone the current system view
			partialView.remove(node); // remove this node from the partial view 
			Collections.shuffle(partialView); // shuffle the content
			
			// determine the partial view for this node
			partialView = partialView.subList(0, systemConf.getInt("gossip.F"));
			
			// configure "Start" message with the partial view
			SystemNode.Start msg = new SystemNode.Start(systemConf, new ArrayList<>(partialView));
			
			 // send "Start" message to the node
			node.tell(msg, ActorRef.noSender());
		}
	}
	
	private static void commute_system() {
		running = !running; // commute running value
		
		// running has commuted to true  -> resume the system
		// running has commuted to false -> pause the system
		Serializable msg = (running) ? new SystemNode.Resume() : new SystemNode.Pause();
		
		// resume/pause each node of the system
		for (ActorRef node : nodes)
			node.tell(msg, ActorRef.noSender());
		
		if (running)
			System.out.println("\n=== SYSTEM RESUMED ===");
		else
			System.out.println("\n=== SYSTEM PAUSED ===");
	}
	
}
