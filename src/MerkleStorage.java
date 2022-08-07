import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class MerkleStorage implements Runnable {
    // MerkleStorage class represents the whole storage of replicas
    // It checks on the states of all replicas, write/delete files to all replicas,
    // and ensures appropriate recovery from data corruption in any of the replicas

    private int numReplicas = 21; // The number of replicas created from the original source
    // (must be an odd number because the storage needs to have majority).
    private MerkleState state = MerkleState.INIT;
    private List<MerkleReplica> replicas = new ArrayList<>(); // Store all MerkleReplica objects
    private Executor executor = Executors.newFixedThreadPool(numReplicas); // Manages all MerkleReplica threads
    private Object mutex = new Object(); // Provides mutual exclusion for getState() and updateState() called by both main() and MerkleStorage itself

    public MerkleState getState() {
        synchronized (mutex ) {
            return state;
        }
    }

    private synchronized void updateState(MerkleState state) {
        synchronized (mutex) {
            this.state = state;
        }
    }

    public synchronized void addFile(Path src) throws IOException {
            // Update the storage's state to PENDING so that main() cannot add or delete file until it is READY again
            updateState(MerkleState.PENDING);
            // Update the changes in original source directory
            String dest = Paths.get("./all/sample") + "/" + src.getFileName().toString();
            Files.copy(src, Paths.get(dest), REPLACE_EXISTING);

            // Update each replica by submitting a MerkleRequest with ADD request and path of the source file
            for (MerkleReplica replica : replicas) {
                replica.addRequest(new MerkleRequest(RequestType.ADD, src));
            }
    }

    public synchronized void deleteFile(String fileName) throws IOException {
        updateState(MerkleState.PENDING);
        String mainFileToDelete = "./all/sample/" +fileName;
        Files.deleteIfExists(Paths.get(mainFileToDelete));

        for (MerkleReplica replica : replicas) {
            String replicaFileToDelete = replica.replicaPath + "/" + fileName;
            replica.addRequest(new MerkleRequest(RequestType.DELETE, Paths.get(replicaFileToDelete)));
        }
    }

    @Override
    public void run() {
        // Main MerkleStorage thread function
        // This function first create all the necessary MerkleReplica, and populate the replicas with the content of the original source

        try {
            // Each replica is added to the List<> for monitoring, and to the executor to run its corresponding thread
            // Update the state of storage to READY
            for (int i=0; i<numReplicas; i++) {
                MerkleReplica replica = new MerkleReplica("./all/sample", i);
                replicas.add(replica);
                executor.execute(replica);
            }
            updateState(MerkleState.READY);

            Object storageMutex = new Object();


            // This while loop continuously check the storage's state and react to state changing events accordingly
            while (true) {
                synchronized (storageMutex) {
                    // If state is not READY, check the state of all replicas.
                    // If more than half of the replicas are READY, change the storage's state to READY
                    // The remaining non-READY replicas will continue its execution and change state to READY,
                    // or terminate and recover itself
                    if (getState() != MerkleState.READY) {
                        int numReady = 0;
                        for (MerkleReplica replica : replicas) {
                            if (replica.getState() == MerkleState.READY) {
                                numReady += 1;
                            }
                        }
                        if (numReady > numReplicas / 2) {
                            updateState(MerkleState.READY);
                        }
                    }

                    // If the state of storage is READY, compute the hash of each replica and determine if there is a majority
                    // Use HashMap to map the hash and its frequency
                    // If frequency of a hash is more than half, the majority of the replicas are in the same state
                    if (getState() == MerkleState.READY) {
                        String majority = null;
                        final Map<String, Integer> hashCount = new HashMap<>();
                        for (MerkleReplica replica : replicas) {
                            // Only compute the replica's hash when it is in READY state
                            if (replica.getState() == MerkleState.READY) {
                                final String hash = replica.computeReplicaMerkleHash();
                                if (hashCount.containsKey(hash)) {
                                    int current = hashCount.get(hash);
                                    hashCount.put(hash, current + 1);
                                    if (current + 1 > numReplicas / 2) {
                                        majority = hash;
                                    }
                                } else {
                                    hashCount.put(hash, 1);
                                }
                            }
                        }

                        // If the majority hash is not found, the loop continues and check again
                        if (majority == null) {
                            updateState(MerkleState.INVALID);
                            continue;
                        } else {
                            // The majority of the replicas are in READY state and they have the same hash
                            // There is possibility that some are also in READY state, but does not have the majority hash
                            // This mean that there is a data corruption
                            // When the replica is READY and its hash does not match majority,
                            // kill the replica thread, re-create the replica and add to the executor

                            List<MerkleReplica> invalidReplicas = new ArrayList<>();
                            List<MerkleReplica> validReplicas = new ArrayList<>();

                            for (MerkleReplica replica : replicas) {
                                // Only compute the replica's hash when it is in READY state
                                if (replica.getState() == MerkleState.READY){
                                    final String hash = replica.computeReplicaMerkleHash();
                                    if (!hash.equals(majority)) {
                                        invalidReplicas.add(replica);
                                        // Send request to terminate the replica's thread
                                        replica.addRequest(new MerkleRequest(RequestType.STOP, null));
                                    } else {
                                        validReplicas.add(replica);
                                    }
                                } else if (replica.getState() == MerkleState.INVALID) {
                                    // A replica can be deleted during hash computation (see MerkleReplica.computeReplicaMerkleHash())
                                    // That sets the state of the replica to INVALID
                                    invalidReplicas.add(replica);
                                } else {
                                    validReplicas.add(replica);
                                }
                            }
                            for (MerkleReplica replica : invalidReplicas) {
                                // Recreate the replicas from the original source with the invalid replicas' id
                                // Assumption: the original source is the ground truth; any corrupted replica can create a copy from it
                                MerkleReplica newReplica = null;
                                while (newReplica == null){
                                    try{
                                        newReplica = new MerkleReplica("./all/sample", replica.id);
                                        break;
                                    } catch (Exception e){
                                        continue;
                                    }
                                }
                                Logger.info("Storage", "Recovering replica " + newReplica.id);
                                validReplicas.add(newReplica);
                                executor.execute(newReplica);
                            }
                            // Update the List<> with current versions of replicas
                            replicas = validReplicas;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
