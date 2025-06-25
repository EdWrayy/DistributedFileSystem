import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Dstore {

    int port; //Port to listen for incoming client requests
    int cport; //Controller's port
    int timeout;
    String fileFolder;

    private Socket controllerSocket;
    private PrintWriter ctrlOut;

    private final Object ctrlOutLock = new Object();

    public Dstore(int port, int cport, int timeout, String fileFolder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: java Dstore <port> <cport> <timeout> <file_folder>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String fileFolder = args[3];

        Dstore store = new Dstore(port, cport, timeout, fileFolder);
        store.start();
    }

    private void start() {
        try {
            //Clear the file folder on startup
            File folder = new File(fileFolder);
            for (File file : folder.listFiles()) {
                if (file.isFile()) file.delete();
            }

            //Send join to the controller, then begin listening to it on a single socket
            controllerSocket = new Socket("localhost", cport);
            ctrlOut = new PrintWriter(controllerSocket.getOutputStream(), true);
            sendToController("JOIN " + port);
            System.out.println("[Dstore] Sent JOIN " + port);

            BufferedReader ctrlIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
            new Thread(new ControllerHandler(ctrlIn, fileFolder, this)).start();


            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("[Dstore] Listening for client/Dstore requests on port " + port);


            //Begin forming connections to clients
            for (;;) {
                Socket clientSocket = serverSocket.accept();

                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String firstLine = in.readLine();
                if (firstLine == null) {
                    clientSocket.close();
                    continue;
                }

                if (firstLine.startsWith("REBALANCE_STORE ")) {
                    new Thread(new DStoreRebalanceHandler(clientSocket, firstLine, fileFolder)).start();
                } else {
                    // Must be a client
                    new Thread(new ClientStoreHandler(clientSocket, fileFolder, firstLine, this)).start();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendToController(String msg) {
        synchronized (ctrlOutLock) {
            ctrlOut.println(msg);
        }
    }
}


/**
 * Handles communication between a singular DStore and the controller
 */
class ControllerHandler implements Runnable {
    private final BufferedReader controllerIn;
    private final String fileFolder;
    private final Dstore dstore;

    public ControllerHandler(BufferedReader controllerIn, String fileFolder, Dstore dstore) {
        this.controllerIn = controllerIn;
        this.fileFolder = fileFolder;
        this.dstore = dstore;
    }

    @Override
    public void run() {
        try {
            String message;
            while ((message = controllerIn.readLine()) != null) {
                handleMessage(message);
            }
        } catch (IOException e) {
            System.err.println("[ControllerHandler] Lost connection to Controller.");
        }
    }

    private void handleMessage(String message) {
        System.out.println("[ControllerHandler] Received from Controller: " + message);

        if (message.startsWith("REMOVE ")) {
            String[] parts = message.split(" ");
            if (parts.length != 2) {
                System.err.println("[ControllerHandler] Malformed REMOVE command.");
                return;
            }

            String filename = parts[1];
            File file = new File(fileFolder, filename);

            if (file.exists()) {
                if (file.delete()) {
                    System.out.println("[Dstore] File " + filename + " deleted successfully.");
                } else {
                    System.err.println("[Dstore] Failed to delete file: " + filename);
                    return;
                }
            } else {
                dstore.sendToController("ERROR_FILE_DOES_NOT_EXIST " + filename);
                return;
            }

            dstore.sendToController("REMOVE_ACK " + filename);
            System.out.println("[Dstore] Sent REMOVE_ACK " + filename);
        }
        else if (message.startsWith("REBALANCE ")) {
            new Thread(() -> handleRebalance(message)).start();
        }
        else if(message.startsWith("LIST")){
            File folder = new File(fileFolder);
            File[] files = folder.listFiles();

            StringBuilder response = new StringBuilder("LIST");
            if (files != null) {
                for (File f : files) {
                    if (f.isFile() && isFileComplete(f)) {
                        response.append(" ").append(f.getName());
                    }
                }
            }

            dstore.sendToController(response.toString());
            System.out.println("[Dstore] Sent LIST response: " + response);
        }
    }

    /**
     * Ensures we don't LIST a file that is still being written
     * @param file
     * @return boolean
     */
    private boolean isFileComplete(File file) {
        long lastModified = file.lastModified();
        long now = System.currentTimeMillis();
        return (now - lastModified) > 1000;
    }

    private void handleRebalance(String message) {
        try {
            System.out.println("[Dstore] Handling REBALANCE: " + message);

            String[] parts = message.split(" ");
            int i = 1;

            int numberOfFilesToSend = Integer.parseInt(parts[i++]);

            //Count which DStores to send each file to
            Map<String, List<Integer>> filesToSend = new HashMap<>();
            for (int j = 0; j < numberOfFilesToSend; j++) {
                String filename = parts[i++];
                int numTargets = Integer.parseInt(parts[i++]);
                List<Integer> targets = new ArrayList<>();
                for (int k = 0; k < numTargets; k++) {
                    targets.add(Integer.parseInt(parts[i++]));
                }
                filesToSend.put(filename, targets);
            }

            int numFilesToRemove = Integer.parseInt(parts[i++]);
            List<String> filesToRemove = new ArrayList<>();
            for (int j = 0; j < numFilesToRemove; j++) {
                filesToRemove.add(parts[i++]);
            }

            // Send files
            for (Map.Entry<String, List<Integer>> entry : filesToSend.entrySet()) {
                String filename = entry.getKey();
                File file = new File(fileFolder, filename);
                if (!file.exists()) continue;

                byte[] fileBytes = java.nio.file.Files.readAllBytes(file.toPath());
                int size = fileBytes.length;

                for (int port : entry.getValue()) {
                    try (
                            Socket socket = new Socket("localhost", port);
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            OutputStream binaryOut = socket.getOutputStream()
                    ) {
                        out.println("REBALANCE_STORE " + filename + " " + size);
                        String ack = in.readLine();
                        if ("ACK".equals(ack)) {
                            binaryOut.write(fileBytes);
                            binaryOut.flush();
                            System.out.println("[Dstore] Rebalance: Sent " + filename + " to port " + port);
                        } else {
                            System.err.println("[Dstore] Rebalance: No ACK from port " + port);
                        }
                    } catch (IOException e) {
                        System.err.println("[Dstore] Failed to send " + filename + " to port " + port);
                    }
                }
            }

            // Delete files
            for (String filename : filesToRemove) {
                File file = new File(fileFolder, filename);
                if (file.exists() && file.delete()) {
                    System.out.println("[Dstore] Rebalance: Deleted " + filename);
                }
            }

            // Tell Controller this DStore is finished
            dstore.sendToController("REBALANCE_COMPLETE");
            System.out.println("[Dstore] Sent REBALANCE_COMPLETE");

        } catch (Exception e) {
            System.err.println("[Dstore] Rebalance handling error: " + e.getMessage());
        }
    }
}


/**
 * Handles communication between a singular DStore and a singular client
 */
class ClientStoreHandler implements Runnable {
    private final Socket clientSocket;
    private final String fileFolder;
    private final String firstLine;
    private final Dstore dStore;

    public ClientStoreHandler(Socket socket, String fileFolder, String firstLine, Dstore dStore) {
        this.clientSocket = socket;
        this.fileFolder = fileFolder;
        this.firstLine = firstLine;
        this.dStore = dStore;
    }

    @Override
    public void run() {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                InputStream binaryIn = clientSocket.getInputStream()
        ) {
            // Handle the first line that was already read
            if (firstLine != null) {
                handleMessage(firstLine, out, binaryIn);
            }
            //Continue handling further messages
            String message;
            while ((message = in.readLine()) != null) {
                handleMessage(message, out, binaryIn);
            }
        } catch (IOException e) {
            System.err.println("[DSTORE ClientHandler] Error handling client.");
        }
    }

    private void handleMessage(String message, PrintWriter out, InputStream binaryIn) {
        System.out.println("[DSTORE ClientHandler] Received: " + message);

        if (message == null) {
            System.err.println("[DSTORE ClientHandler] Invalid or missing STORE message.");
            return;
        }
        if(message.startsWith("STORE ")){
            String[] parts = message.split(" ");
            if (parts.length != 3) {
                System.err.println("[DSTORE ClientHandler] Malformed STORE command.");
                return;
            }
            String filename = parts[1];
            int filesize = Integer.parseInt(parts[2]);

            try {
                System.out.println("[Dstore] Storing file: " + filename + " (" + filesize + " bytes)");

                //Send ACK to the client
                out.println("ACK");


                byte[] data = binaryIn.readNBytes(filesize);


                File file = new File(fileFolder, filename);
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    fos.write(data);
                }

                System.out.println("[Dstore] Stored file: " + filename);

                // Notify controller
                dStore.sendToController("STORE_ACK " + filename);
                System.out.println("[Dstore] Sent STORE_ACK " + filename);

            } catch (IOException e) {
                System.err.println("[DSTORE ClientHandler] Error while storing file: " + e.getMessage());
            }
        }
        else if(message.startsWith("LOAD_DATA ")){
            String[] parts = message.split(" ");
            if (parts.length != 2) {
                System.err.println("[DSTORE ClientHandler] Malformed LOAD_DATA command.");
                return;
            }

            String filename = parts[1];
            File file = new File(fileFolder, filename);

            if (!file.exists()) {
                System.err.println("[DSTORE ClientHandler] Requested file not found, closing connection.");
                try {
                    clientSocket.close(); // Clean shutdown as per spec
                } catch (IOException e) {
                    System.err.println("[DSTORE ClientHandler] Error closing socket: " + e.getMessage());
                }
                return;
            }

            try (OutputStream binaryOut = clientSocket.getOutputStream();
                 FileInputStream fis = new FileInputStream(file)){

                byte[] fileBytes = fis.readAllBytes();
                binaryOut.write(fileBytes);
                binaryOut.flush();
                System.out.println("[Dstore] Sent file " + filename + " (" + fileBytes.length + " bytes)");

            } catch (IOException e) {
                System.err.println("[DSTORE ClientHandler] Error sending file: " + e.getMessage());
            }
        }

        else {
            System.err.println("[DSTORE ClientHandler] Unknown command: " + message);
        }
    }
}

class DStoreRebalanceHandler implements Runnable {
    private final Socket socket;
    private final String firstLine;
    private final String fileFolder;

    public DStoreRebalanceHandler(Socket socket, String firstLine, String fileFolder) {
        this.socket = socket;
        this.firstLine = firstLine;
        this.fileFolder = fileFolder;
    }

    @Override
    public void run() {
        try (
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                InputStream in = socket.getInputStream()
        ) {
            String[] parts = firstLine.split(" ");
            if (parts.length != 3) {
                System.err.println("[DStoreReceiver] Malformed REBALANCE_STORE command.");
                return;
            }

            String filename = parts[1];
            int filesize = Integer.parseInt(parts[2]);

            out.println("ACK");

            byte[] bytes = in.readNBytes(filesize);
            File file = new File(fileFolder, filename);
            try (FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(bytes);
                System.out.println("[DStoreReceiver] Received and stored " + filename);
            }

        } catch (IOException e) {
            System.err.println("[DStoreReceiver] Error handling rebalance store: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }
}



