import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Controller {

    protected final int numberOfReplications;
    private int cport;
    protected int timeout;
    private int rebalancePeriod;

    // Tracking active connections (Dstores)
    protected final List<Socket> activeDStores = Collections.synchronizedList(new ArrayList<>());
    protected final Map<Socket, Integer> dstorePorts = new ConcurrentHashMap<>();

    //Map of safewriters for each socket to ensure thread safety when sending messages
    protected final Map<Socket,SafeWriter> writers = new ConcurrentHashMap<>();

    //Map providing information like state, size and DStores for a given filename
    protected final Map<String, FileInfo> index = new ConcurrentHashMap<>();

    //Maps a filename to the state of the operation being performed on it
    protected final Map<String, PendingOperation> suspendedAcks = new ConcurrentHashMap<>();

    //A set of ports which are yet to send their rebalanceACK
    protected final Set<Integer> waitingForRebalanceAcks = ConcurrentHashMap.newKeySet();

    //Timer for rebalanceACKs
    protected Timer rebalanceTimeoutTimer;

    public boolean rebalanceInProgress = false;

    protected final Queue<PendingClientRequest> clientRequestQueue = new ConcurrentLinkedQueue<>();

    //Track DStore responses to the LIST operation in the Rebalance method
    protected final Map<Integer, String> rebalanceListResponses = new ConcurrentHashMap<>();
    protected final Set<Integer> waitingForListResponses = ConcurrentHashMap.newKeySet();
    protected Timer listResponseTimer;



    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: java Controller <cport> <numberOfReplications> <timeout> <rebalance_period>");
            return;
        }

        int cport = Integer.parseInt(args[0]);
        int numberOfReplications = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);

        Controller controller = new Controller(cport, numberOfReplications, timeout, rebalancePeriod);
        controller.start();
    }

    public Controller(int cport, int numberOfReplications, int timeout, int rebalancePeriod) {
        this.cport = cport;
        this.numberOfReplications = numberOfReplications;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
    }

    public void start() {
        try (ServerSocket ss = new ServerSocket(cport)) {
            System.out.println("[Controller] Listening on port " + cport + "...");

            //Thread specifically for performing rebalance operations
            new Thread(() -> {
                try {
                    while (true) {
                        Thread.sleep(rebalancePeriod * 1000L);
                        synchronized (Controller.this) {
                            if (activeDStores.size() >= numberOfReplications && suspendedAcks.isEmpty() && !rebalanceInProgress) {
                                performRebalance();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    System.err.println("[Rebalancer] Interrupted: " + e.getMessage());
                }
            }).start();


            //Proceed to listen to new connections, and create a new thread to handle communication on each
            for(;;){
                Socket socket = ss.accept();
                System.out.println("[Controller] Accepted connection from " + socket.getInetAddress());

                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                createThread(socket);
            }
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private void createThread(Socket s) {
        new Thread(() -> {
            try{
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                String firstLine = in.readLine();
                if(firstLine == null){ s.close(); return; }

                if(firstLine.startsWith("JOIN ")){             // Dstore
                    int dPort = Integer.parseInt(firstLine.split(" ")[1]);
                    SafeWriter w = new SafeWriter(s);
                    synchronized(this){
                        writers.put(s,w);
                        dstorePorts.put(s,dPort);
                        activeDStores.add(s);
                        if (activeDStores.size() >= numberOfReplications && suspendedAcks.isEmpty() && !rebalanceInProgress){
                            performRebalance();      // immediate rebalance on new join
                        }
                    }
                    new Thread(new DStoreHandler(s, firstLine, this)).start();
                }else{                  // Client
                    new Thread(new ClientControllerHandler(
                            s, firstLine, this)).start();
                }
            }catch(IOException ignored){}
        }).start();
    }

    private synchronized void performRebalance() {
        if (rebalanceInProgress) {
            System.out.println("[Rebalancer] Rebalance already in progress, skipping.");
            return;
        }
        if (!suspendedAcks.isEmpty()) {
            System.out.println("[Rebalancer] Pending operations exist, postponing rebalance.");
            return;
        }

        System.out.println("[Rebalancer] Starting rebalance operation...");
        rebalanceInProgress = true;
        //Request file list from each DStore
        Map<Integer, List<String>> dstoreFiles = new HashMap<>();
        rebalanceListResponses.clear();
        waitingForListResponses.clear();

        for (Socket socket : activeDStores) {
            int port = dstorePorts.get(socket);
            SafeWriter sw = writers.get(socket);
            sw.sendSafeMessage("LIST");
            waitingForListResponses.add(port);
        }

        listResponseTimer = new Timer();
        listResponseTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                processListResponses();
            }
        }, timeout);
    }

    private void startRebalanceTimeout() {
        // Cancel any existing timer
        if (rebalanceTimeoutTimer != null) {
            rebalanceTimeoutTimer.cancel();
        }

        rebalanceTimeoutTimer = new Timer();
        rebalanceTimeoutTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                synchronized (Controller.this) {
                    if (!waitingForRebalanceAcks.isEmpty()) {
                        System.err.println("[Rebalancer] Timeout waiting for REBALANCE_COMPLETE from: " + waitingForRebalanceAcks);

                        // Log which DStores didn't respond
                        System.err.println("[Rebalancer] Non-responsive DStores: " + waitingForRebalanceAcks);

                        // Clean up rebalance state
                        waitingForRebalanceAcks.clear();
                        rebalanceInProgress = false;

                        // Process queued client requests
                        processQueuedRequests();

                        // Schedule a new rebalance attempt after a delay
                        // This helps the system recover and eventually achieve balance
                        Timer rescheduleTimer = new Timer();
                        rescheduleTimer.schedule(new TimerTask() {
                            @Override
                            public void run() {
                                synchronized (Controller.this) {
                                    // Only if conditions are right
                                    if (activeDStores.size() >= numberOfReplications &&
                                            suspendedAcks.isEmpty() &&
                                            !rebalanceInProgress) {
                                        System.out.println("[Rebalancer] Attempting rebalance again after timeout");
                                        performRebalance();
                                    }
                                }
                            }
                        }, 5000); // Wait 5 seconds before trying again
                    }
                }
            }
        }, timeout);
    }

    protected void processQueuedRequests() {
        synchronized (this) {
            while (!clientRequestQueue.isEmpty()) {
                PendingClientRequest request = clientRequestQueue.poll();
                try {
                    // Create a new handler for this request
                    ClientControllerHandler handler = new ClientControllerHandler(
                            request.clientSocket, request.command, this);
                    handler.handleCommand(request.command, request.out);
                } catch (Exception e) {
                    System.err.println("[Controller] Error processing queued request: " + e.getMessage());
                }
            }
        }
    }

    protected void processListResponses(){
        synchronized (this) {
            Map<Integer, List<String>> dstoreFiles = new HashMap<>();

            for (Map.Entry<Integer, String> entry : rebalanceListResponses.entrySet()) {
                int port = entry.getKey();
                String response = entry.getValue();

                if (response != null && response.startsWith("LIST")) {
                    List<String> files = Arrays.asList(response.split(" "));
                    files = files.subList(1, files.size()); // remove "LIST"
                    dstoreFiles.put(port, files);
                }
            }

            //Count how many DStores have each file (reverse the index of dstoreFiles)
            Map<String, List<Integer>> dStoresPerFile = new HashMap<>();
            for (Map.Entry<Integer, List<String>> entry : dstoreFiles.entrySet()) {
                int port = entry.getKey();
                for (String filename : entry.getValue()) {
                    dStoresPerFile.putIfAbsent(filename, new ArrayList<>());
                    dStoresPerFile.get(filename).add(port);
                }
            }
            //Check consistency, remove all files from the index that are not listed in a DStore
            Set<String> indexFiles = new HashSet<>(index.keySet());
            for (String file : indexFiles) {
                if (!dStoresPerFile.containsKey(file)) {
                    System.out.println("[Rebalancer] Removing file from index (missing on all Dstores): " + file);
                    index.remove(file);
                }
            }
            //Delete any files in DStores that are not listed in the index
            Map<Integer, List<String>> toDelete = new HashMap<>();
            for (Map.Entry<Integer, List<String>> entry : dstoreFiles.entrySet()) {
                int port = entry.getKey();
                for (String file : entry.getValue()) {
                    if (!index.containsKey(file)) {
                        toDelete.putIfAbsent(port, new ArrayList<>());
                        toDelete.get(port).add(file);
                    }
                }
            }

            //Map the ports of each DStore to a list of instructions to perform during the rebalance
            Map<Integer, List<RebalanceInstruction>> rebalanceInstructions = new HashMap<>();

            //Count the number of files in each DStore
            Map<Integer, Integer> fileCount = new HashMap<>();
            for (Integer port : dstoreFiles.keySet()) {
                fileCount.put(port, dstoreFiles.get(port).size());
            }

            //For each file, rebalance it
            for (Map.Entry<String, List<Integer>> entry : dStoresPerFile.entrySet()) {
                String file = entry.getKey();
                List<Integer> currentHolders = entry.getValue();

                Set<Integer> desiredFileHolders = new HashSet<>(currentHolders);

                //If there are less than R replications, add new replications to the DStore with the fewest current files until there are.
                while (desiredFileHolders.size() < numberOfReplications) {
                    Integer best = null;
                    for (Integer port : fileCount.keySet()) {
                        if (!desiredFileHolders.contains(port)) {
                            if (best == null || fileCount.get(port) < fileCount.get(best)) {
                                best = port;
                            }
                        }
                    }
                    if (best == null) break; // no eligible DStore left (unlikely)

                    desiredFileHolders.add(best);
                    fileCount.put(best, fileCount.get(best) + 1);
                }
                if (desiredFileHolders.size() == numberOfReplications) {

                    // Find the lightest Dstore not holding the file
                    Integer lightest = null;
                    for (Integer p : fileCount.keySet()) {
                        if (!desiredFileHolders.contains(p)) {
                            if (lightest == null || fileCount.get(p) < fileCount.get(lightest)) {
                                lightest = p;
                            }
                        }
                    }

                    // Find the heaviest current holder
                    Integer heaviest = null;
                    for (Integer p : desiredFileHolders) {
                        if (heaviest == null || fileCount.get(p) > fileCount.get(heaviest)) {
                            heaviest = p;
                        }
                    }

                    // Move the replica only if that would improve balance
                    if (lightest != null &&
                            heaviest != null &&
                            fileCount.get(heaviest) - fileCount.get(lightest) > 1) {

                        desiredFileHolders.remove(heaviest);
                        desiredFileHolders.add(lightest);

                        // Update counts for greedy balancing that follows
                        fileCount.put(heaviest, fileCount.get(heaviest) - 1);
                        fileCount.put(lightest,  fileCount.get(lightest)  + 1);


                        rebalanceInstructions
                                .computeIfAbsent(heaviest, k -> new ArrayList<>())
                                .add(new RebalanceInstruction(file, List.of(lightest)));
                        toDelete.computeIfAbsent(heaviest, k -> new ArrayList<>()).add(file);
                    }
                }
                //If there are more than R replications, remove from the DStore with the most files until there are
                while (desiredFileHolders.size() > numberOfReplications) {
                    Integer worst = null;
                    for (Integer port : desiredFileHolders) {
                        if (worst == null || fileCount.get(port) > fileCount.get(worst)) {
                            worst = port;
                        }
                    }
                    desiredFileHolders.remove(worst);
                    fileCount.put(worst, fileCount.get(worst) - 1);
                    toDelete.putIfAbsent(worst, new ArrayList<>());
                    toDelete.get(worst).add(file);
                }

                // Build rebalance instructions from a holder to a new receiver
                for (Integer port : desiredFileHolders) {
                    if (!currentHolders.contains(port)) {
                        // find someone who has it
                        for (Integer src : currentHolders) {
                            if (!src.equals(port)) {
                                rebalanceInstructions.putIfAbsent(src, new ArrayList<>());
                                rebalanceInstructions.get(src).add(new RebalanceInstruction(file, List.of(port)));
                                break;
                            }
                        }
                    }
                }
            }

            //Send the instructions to each port
            for (Socket socket : activeDStores) {
                SafeWriter sw = writers.get(socket);
                int port = dstorePorts.get(socket);
                List<RebalanceInstruction> instructions = rebalanceInstructions.getOrDefault(port, new ArrayList<>());
                List<String> removes = toDelete.getOrDefault(port, new ArrayList<>());

                StringBuilder msg = new StringBuilder("REBALANCE ");
                msg.append(instructions.size());
                for (RebalanceInstruction ri : instructions) {
                    msg.append(" ").append(ri.filename).append(" ").append(ri.targetPorts.size());
                    for (int p : ri.targetPorts) {
                        msg.append(" ").append(p);
                    }
                }
                msg.append(" ").append(removes.size());
                for (String f : removes) {
                    msg.append(" ").append(f);
                }
                sw.sendSafeMessage(msg.toString());
                System.out.println("[Rebalancer] Sent to DStore " + port + ": " + msg);

            }

            waitingForRebalanceAcks.clear();
            for (Socket socket : activeDStores) {
                int port = dstorePorts.get(socket);
                waitingForRebalanceAcks.add(port);
            }
            startRebalanceTimeout();
        }
    }

}




/**
 * Handles a connection between the controller and a single DStore
 */
class DStoreHandler implements Runnable {

    private final Socket socket;
    private final int dstorePort;
    private final Controller controller;

    public DStoreHandler(Socket socket, String firstLine, Controller controller) {
        this.socket = socket;
        this.dstorePort = Integer.parseInt(firstLine.split(" ")[1]);
        this.controller = controller;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("[DStoreControllerHandler] From Dstore " + dstorePort + ": " + line);
                handleMessage(line);
            }
        } catch (IOException e) {
            System.out.println("[DStoreControllerHandler] Socket error, Dstore " + dstorePort + " disconnected: " + e.getMessage());
        } finally {
            synchronized (controller) {
                controller.activeDStores.remove(socket);
                controller.dstorePorts.remove(socket);
                controller.writers.remove(socket);
                System.out.println("[DStoreControllerHandler] DStore " + dstorePort + " removed from active DStores.");
            }
        }
    }


    public void handleMessage(String line) {
        System.out.println("[DStoreHandler] Received: " + line);
        if (line.startsWith("STORE_ACK ")) {
            String filename = line.split(" ")[1];
            synchronized (controller) { //Must ensure thread safety when handling suspendedAcks
                PendingOperation progress = controller.suspendedAcks.get(filename);
                if (progress != null) {
                    progress.waitingFor.remove((Integer) dstorePort);
                    if (progress.waitingFor.isEmpty()) {
                        if (progress.timeoutTimer != null) {
                            progress.timeoutTimer.cancel();
                        }

                        controller.index.get(filename).state = FileInfo.State.STORE_COMPLETE;
                        progress.clientOut.println("STORE_COMPLETE");
                        controller.suspendedAcks.remove(filename);
                        System.out.println("[Controller] Store complete for " + filename);
                    }
                }
            }
        }
        else if ((line.startsWith("REMOVE_ACK ") || line.startsWith("ERROR_FILE_DOES_NOT_EXIST"))) {
            String filename = line.split(" ")[1];
            synchronized (controller) {
                PendingOperation progress = controller.suspendedAcks.get(filename);
                if (progress != null) {
                    progress.waitingFor.remove((Integer) dstorePort);
                    if (progress.waitingFor.isEmpty()) {
                        if (progress.timeoutTimer != null) {
                            progress.timeoutTimer.cancel();
                        }

                        // Remove the file from the index
                        controller.index.remove(filename);
                        // Notify client
                        progress.clientOut.println("REMOVE_COMPLETE");
                        controller.suspendedAcks.remove(filename);
                        System.out.println("[Controller] Remove complete for " + filename);
                    }
                }
            }
        }
        else if(line.startsWith("REBALANCE_COMPLETE")){
            System.out.println("[DStoreHandler] DStore " + dstorePort + " completed rebalance.");
            synchronized (controller) {
                if (controller.waitingForRebalanceAcks.contains(dstorePort)) {
                    controller.waitingForRebalanceAcks.remove(dstorePort);
                    System.out.println("[Rebalancer] Received REBALANCE_COMPLETE from " + dstorePort);

                    if (controller.waitingForRebalanceAcks.isEmpty()) {
                        controller.rebalanceTimeoutTimer.cancel();
                        System.out.println("[Rebalancer] All DStores completed rebalance.");
                        controller.rebalanceInProgress = false;
                        controller.processQueuedRequests();
                    }
                }
            }
        }
        else if(line.startsWith("LIST")){
            synchronized (controller) {
                controller.rebalanceListResponses.put(dstorePort, line);
                controller.waitingForListResponses.remove(dstorePort);

                // If we've received all responses, process them
                if (controller.waitingForListResponses.isEmpty()) {
                    if (controller.listResponseTimer != null) {
                        controller.listResponseTimer.cancel();
                    }
                    controller.processListResponses();
                }
            }
        }
    }
}



/**
 * Handles connection between a single client and the controller
 */
class ClientControllerHandler implements Runnable {

    private final Socket socket;
    private final String initialLine;   // The first line we already read
    private final Controller controller;
    private final Map<String, List<Integer>> loadTriedPorts = new HashMap<>(); //Keep track of which DStores we've tried to laod from

    public ClientControllerHandler(Socket socket, String initialLine, Controller controller) {
        this.socket = socket;
        this.initialLine = initialLine;
        this.controller = controller;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            // first handle the line we already read
            handleCommand(initialLine, out);

            // then read further commands from the client
            String line;
            while ((line = in.readLine()) != null) {
                handleCommand(line, out);
            }
            System.out.println("[ClientControllerHandler] Client closed the connection or sent no more data.");
        } catch (IOException e) {
            System.out.println("[ClientControllerHandler] Client disconnected or error: " + e.getMessage());
        } finally {
            try {
                if (!socket.isClosed()) {
                    socket.close();
                    System.out.println("[ClientControllerHandler] Socket closed in finally block.");
                }
            } catch (IOException ignored) {
                System.out.println("[ClientControllerHandler] Error closing socket in finally block.");
            }
        }
    }

    protected void handleCommand(String line, PrintWriter out) {
        System.out.println("[ClientControllerHandler] Received from client: " + line);

        synchronized (controller) {
            // If rebalance is in progress, queue the request (except for RELOAD commands)
            if (controller.rebalanceInProgress && !line.startsWith("RELOAD")) {
                controller.clientRequestQueue.add(new PendingClientRequest(socket, line, out));
                System.out.println("[ClientControllerHandler] Queued request during rebalance: " + line);
                return;
            }
        }

        if (line.startsWith("STORE ")) {
            loadTriedPorts.clear();
            System.out.println("Storing");
            String[] parts = line.split(" ");
            if (parts.length != 3) {
                out.println("ERROR: INVALID COMMAND");
                return;
            }

            String fileName = parts[1];
            int fileSize = Integer.parseInt(parts[2]);

            synchronized (controller) {
                if (controller.activeDStores.size() < controller.numberOfReplications) {
                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                    return;
                }

                if (controller.index.containsKey(fileName)) {
                    out.println("ERROR_FILE_ALREADY_EXISTS");
                    return;
                }

                FileInfo newFile = new FileInfo(fileSize);
                controller.index.put(fileName, newFile);

                List<Socket> dstores = pickDstoresForStore();
                List<Integer> ports = new ArrayList<>();
                for (Socket s : dstores) {
                    Integer listeningPort = controller.dstorePorts.get(s);
                    if (listeningPort != null) {
                        ports.add(listeningPort);
                    }
                }
                controller.index.get(fileName).dstores.addAll(ports);

                StringBuilder sb = new StringBuilder("STORE_TO");
                for (int port : ports) {
                    sb.append(" ").append(port);
                }
                out.println(sb.toString());

                PendingOperation pending = new PendingOperation(out, ports);
                controller.suspendedAcks.put(fileName, pending);

                Timer timer = new Timer();
                pending.timeoutTimer = timer;
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        synchronized (controller) {
                            if (controller.suspendedAcks.containsKey(fileName)) {
                                controller.suspendedAcks.remove(fileName);
                                controller.index.remove(fileName);
                                System.err.println("[Controller] Store timeout for " + fileName);
                            }
                        }
                    }
                }, controller.timeout);
            }
        }

        else if(line.equals("LIST")) {
            loadTriedPorts.clear();
            synchronized (controller) {
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, FileInfo> entry : controller.index.entrySet()) {
                    if (entry.getValue().state == FileInfo.State.STORE_COMPLETE) {
                        sb.append(entry.getKey()).append(" ");
                    }
                }
                String response = sb.toString().trim();
                if (response.isEmpty()) {
                    out.println("LIST");
                } else {
                    out.println("LIST " + response);
                }
            }
        }
        else if(line.startsWith("LOAD")){
            loadTriedPorts.clear();
            String[] parts = line.split(" ");
            if (parts.length != 2) {
                out.println("ERROR: INVALID COMMAND");
                return;
            }

            String filename = parts[1];

            synchronized (controller) {
                if (controller.activeDStores.size() < controller.numberOfReplications) {
                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                    return;
                }
                FileInfo fileInfo = controller.index.get(filename);
                if (fileInfo == null || fileInfo.state != FileInfo.State.STORE_COMPLETE) {
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                    return;
                }


                List<Integer> ports = fileInfo.dstores;
                if (ports.isEmpty()) {
                    out.println("ERROR_LOAD");
                    return;
                }
                //Choose a random port
                int randomIndex = (int)(Math.random() * ports.size());
                int chosenPort = ports.get(randomIndex);
                loadTriedPorts.putIfAbsent(filename, new ArrayList<>());
                loadTriedPorts.get(filename).add(chosenPort);

                out.println("LOAD_FROM " + chosenPort + " " + fileInfo.size);
            }
        }
        else if(line.startsWith("RELOAD")){
            String filename = line.split(" ")[1];
            synchronized (controller) {
                if (!loadTriedPorts.containsKey(filename)) {
                    out.println("ERROR_LOAD");
                    return;
                }
                FileInfo fileInfo = controller.index.get(filename);
                if (fileInfo == null || fileInfo.state != FileInfo.State.STORE_COMPLETE) {
                    // File does not exist on this DStore
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                    return;
                }


                List<Integer> fileDstores = fileInfo.dstores;
                List<Integer> tried = loadTriedPorts.get(filename);

                // Find the first Dstore port that we haven't yet tried
                Integer newPort = null;
                for (int portCandidate : fileDstores) {
                    if (!tried.contains(portCandidate)) {
                        newPort = portCandidate;
                        break;
                    }
                }

                if (newPort == null) {
                    // We have tried them all
                    out.println("ERROR_LOAD");
                } else {
                    // Found an untried Dstore
                    tried.add(newPort);
                    out.println("LOAD_FROM " + newPort + " " + fileInfo.size);
                }
            }
        }
        else if(line.startsWith("REMOVE")){
            loadTriedPorts.clear();
            String[] parts = line.split(" ");
            if (parts.length != 2) {
                out.println("ERROR: INVALID COMMAND");
                return;
            }
            String filename = parts[1];
            synchronized (controller) {
                if (controller.activeDStores.size() < controller.numberOfReplications) {
                    out.println("ERROR_NOT_ENOUGH_DSTORES");
                    return;
                }

                FileInfo fileInfo = controller.index.get(filename);
                if (fileInfo == null || fileInfo.state != FileInfo.State.STORE_COMPLETE) {
                    out.println("ERROR_FILE_DOES_NOT_EXIST");
                    return;
                }

                fileInfo.state = FileInfo.State.REMOVE_IN_PROGRESS;

                List<Integer> dstoresWithFile = new ArrayList<>(fileInfo.dstores);

                PendingOperation pending = new PendingOperation(out, dstoresWithFile);
                controller.suspendedAcks.put(filename, pending);

                Timer timer = new Timer();
                pending.timeoutTimer = timer;
                timer.schedule(new TimerTask() {
                    @Override
                    public void run() {
                        synchronized (controller) {
                            if (controller.suspendedAcks.containsKey(filename)) {
                                controller.suspendedAcks.remove(filename);
                                // Leave file in index as REMOVE_IN_PROGRESS
                                System.err.println("[Controller] Remove timeout for " + filename);
                            }
                        }
                    }
                }, controller.timeout);

                for (Map.Entry<Socket, Integer> entry : controller.dstorePorts.entrySet()) {
                    if (dstoresWithFile.contains(entry.getValue())) {
                        SafeWriter sw = controller.writers.get(entry.getKey());
                        sw.sendSafeMessage("REMOVE " + filename);
                    }
                }
            }


        }
        else{
            out.println("ERROR: INVALID COMMAND");
        }
    }

    private List<Socket> pickDstoresForStore() {
        // Count how many files are on each DStore

        //Initialize list with all ports
        Map<Socket, Integer> fileCounts = new HashMap<>();
        synchronized(controller) {
            for (Socket s : controller.activeDStores) {
                fileCounts.put(s, 0);
            }

            //Increment the count for each file
            for (Map.Entry<String, FileInfo> e : controller.index.entrySet()) {
                FileInfo fi = e.getValue();
                // We'll treat both STORE_IN_PROGRESS and STORE_COMPLETE as occupying storage
                if (fi.state != FileInfo.State.REMOVE_IN_PROGRESS) {
                    for (int port : fi.dstores) {
                        // Find which socket has that port
                        Socket sock = getDstoreSocketByPort(port);
                        if (sock != null && fileCounts.containsKey(sock)) {
                            fileCounts.put(sock, fileCounts.get(sock) + 1);
                        }
                    }
                }
            }

            // Now sort the activeDStores by ascending file count
            List<Socket> sorted = new ArrayList<>(controller.activeDStores);
            sorted.sort(Comparator.comparing(fileCounts::get));

            // Return the first R (or fewer if there are not enough Dstores)
            int limit = Math.min(controller.numberOfReplications, sorted.size());
            return sorted.subList(0, limit);
        }
    }

    /**
     * Helper to find the Socket corresponding to a given Dstore port.
     */
    private Socket getDstoreSocketByPort(int port) {
        synchronized (controller) {
            for (Socket s : controller.activeDStores) {
                Integer p = controller.dstorePorts.get(s);
                if (p != null && p == port) return s;
            }
            return null;
        }
    }
}

/**
 * Fileinfo for the controller's index to store
 */
class FileInfo {
    enum State { STORE_IN_PROGRESS, STORE_COMPLETE, REMOVE_IN_PROGRESS }
    State state;
    int size;
    final CopyOnWriteArrayList<Integer> dstores = new CopyOnWriteArrayList<>(); // ports or IDs of Dstores storing the file

    FileInfo(int size) {
        this.size = size;
        this.state = State.STORE_IN_PROGRESS;
    }
}

/**
 * Info for the controller's suspended ACKs map, which allows us to track whether an operation has been completed
 */
class PendingOperation {
    PrintWriter clientOut; //Client we need to reply to once all ACKs are received
    List<Integer> waitingFor; //Ports which we are waiting to receive ACKs from
    Timer timeoutTimer;

    PendingOperation(PrintWriter out, List<Integer> dstores) {
        this.clientOut = out;
        this.waitingFor = new ArrayList<>(dstores);
    }
}


/**
 * Represents the DStores to which a DStore must send a file as part of the rebalance operation
 */
class RebalanceInstruction{
    String filename;
    List<Integer> targetPorts;

    RebalanceInstruction(String filename, List<Integer> targetPorts) {
        this.filename = filename;
        this.targetPorts = targetPorts;
    }
}

/**
 * Class to store pending requests
 */
class PendingClientRequest {
    Socket clientSocket;
    String command;
    PrintWriter out;

    PendingClientRequest(Socket socket, String command, PrintWriter out) {
        this.clientSocket = socket;
        this.command = command;
        this.out = out;
    }
}

class SafeWriter {
    private final PrintWriter out;
    private final Object lock = new Object();
    SafeWriter(Socket s) throws IOException {
        this.out = new PrintWriter(s.getOutputStream(), true);
    }
    void sendSafeMessage(String msg) {
        synchronized (lock) {
            out.println(msg);
        }
    }
}
