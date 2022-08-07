import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static java.nio.file.StandardCopyOption.*;

public class MerkleReplica implements Runnable {

    private MerkleState state;
    public int id;
    public String replicaPath;
    public Queue<MerkleRequest> requests = new LinkedList<>(); // A queue contains requests to modify the replica
    private final Object requestMutex = new Object(); // Provides mutual exclusion to access of the queue

    public MerkleReplica(final String startingPath, final int id) throws Exception {
        this.id = id;
        this.state = MerkleState.INIT;
        this.replicaPath = initDirectory(startingPath); // Copy content from the original source directory
        this.state = MerkleState.READY;
    }

    public synchronized MerkleState getState() { return state; }

    private synchronized void updateState(MerkleState state) { this.state = state; }

    private synchronized String initDirectory(final String src) throws IOException {
        final String destName = "/replica-" + id;
        final Path srcDir = Paths.get(src);
        final String destPath = srcDir.getParent() + destName;
        final Path destDir = Paths.get(destPath);

        // Delete the exisitng directory if it has the same path before copying
        if (Files.exists(destDir)) {
            deleteWhenExists(destDir);
        }
        copyFolder(srcDir, destDir);
        return destPath;
    }

    private void deleteWhenExists(Path dir) throws IOException {
        // Collect all the files within the directory
        // If the directory contains directories with files, the children are deleted first for proper clean up
        final List<Path> pathsToDelete = Files.walk(dir)
                                        .sorted(Comparator.reverseOrder())
                                        .collect(Collectors.toList());
        for(Path path : pathsToDelete) {
            Files.deleteIfExists(path);
        }
    }

    private void copyFolder(Path src, Path dest) throws IOException {
        try (Stream<Path> stream = Files.walk(src)) {
            stream.forEach(source -> copy(source, dest.resolve(src.relativize(source))));
        }
    }

    private void copy(Path source, Path dest) {
        try {
            Files.copy(source, dest, REPLACE_EXISTING);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String computeReplicaMerkleHash() {
        // This function compute the MerkleHash of the current replica
        // Calls the static function from MerkleHash to compute the root
        // If the replica is deleted during computation, it returns the id of replica,
        // since it is unique to each replica and this avoids majority of the same message
        try {
            final Path replicaDir = Paths.get(replicaPath);
            return MerkleHash.getRootHash(replicaDir);
        } catch (final Exception e) {
            updateState(MerkleState.INVALID);
            return String.valueOf(id);
        }
    }

    public void addRequest(MerkleRequest request) {
        synchronized (requestMutex) {
            // Change the state from READY to PROCESSING, indicating that replica is modifying and storage should not try to compute its MerkleHash at this point
            updateState(MerkleState.PROCESSING);
            requests.add(request);
        }
    }

    private void processRequest(MerkleRequest request)  {
        Path src = request.src;
        if (request.requestType == RequestType.ADD){
            try {
                // Add file provided by src to the current replica
                String dest = Paths.get(replicaPath) + "/" + src.getFileName().toString();
                Files.deleteIfExists(Paths.get(dest));
                Files.copy(src, Paths.get(dest), REPLACE_EXISTING);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else if (request.requestType == RequestType.DELETE){
            try {
                // Delete the file specified by src in the current replica
                Files.deleteIfExists(src);
            }  catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        // Main MerkleReplica thread function
        // This function continuously check the state of the replica, and react to it accordingly
        while (true) {
            // State is changed to PROCESSING when a new request is added to the queue
            if (getState() == MerkleState.PROCESSING) {
                MerkleRequest request = null;
                synchronized (requestMutex) {
                    // If queue is not empty, pop a request
                    // If it is, no more requests to process, update the replica's state to READY
                    if (!requests.isEmpty())
                        request = requests.remove();
                    else
                        updateState(MerkleState.READY);
                }

                // Process request
                if (request != null) {
                    // If storage requests to kill the replica,
                    // set the state to INVALID, and return this run function, killing the thread
                    if (request.requestType == RequestType.STOP) {
                        Logger.info(String.valueOf(id), "Received Stop Command. Terminating replica.");
                        updateState(MerkleState.INVALID);
                        return;
                    } else {
                        // Create a new thread to execute the add/delete task
                        // Use Callable object as it returns a Future
                        // Future is used to check on the status of the task
                        // Update the state of replica to PENDING before it process the request
                        // If the task is completed, update the state of replica to PROCESSING
                        final MerkleRequest finalRequest = request;
                        ExecutorService executor = Executors.newSingleThreadExecutor();
                        Callable<Boolean> proccessRequest = () -> {
                            updateState(MerkleState.PENDING);
                            processRequest(finalRequest);
                            return true;
                        };
                        Future<Boolean> future = executor.submit(proccessRequest);
                        while (true) {
                            try {
                                future.get(100, TimeUnit.MILLISECONDS);
                                updateState(MerkleState.PROCESSING);
                                break;
                            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                                // when task is not completed, simply catch the exception and do nothing
                            }
                        }
                    }
                }
            } else if (getState() == MerkleState.INVALID){
                // The state could be set to INVALID when computeReplicaMerkleHash caught the exception
                // This mean the replica is non-existent, thus the thread representing it should be terminated as well
                return;
            }
            // Run the state checking every 100 ms
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } //end while
    }
}
