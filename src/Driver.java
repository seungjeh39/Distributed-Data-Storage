import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.*;

public class Driver {

    private final static String ID = "Driver";
    private final static int THREAD_POOL_INTERVAL = 1000;

    public static void main(String[] args) throws InterruptedException, IOException {
        //  Logger Object is used to output messages to Console
        Logger.info(ID, "Starting up Merkle Storage.");

        // Use ExecutorService to manage threads
        // This executor calls the 'run' function of the MerkleStorage storage,
        // which continuously checking the state of the MerkleStorage by managing the states of all MerkleReplicas
        ExecutorService executor = Executors.newSingleThreadExecutor();
        MerkleStorage storage = new MerkleStorage();
        executor.submit(storage);

        // Pre-populate the files to add and delete for the storage
        ArrayList<String> filesToAdd = new ArrayList<>();
        filesToAdd.add("./all/test.txt");
        filesToAdd.add("./all/test2.txt");
        filesToAdd.add("./all/test3.txt");
        filesToAdd.add("./all/test4.txt");

        ArrayList<String> filesToDelete = new ArrayList<>();
        filesToDelete.add("test.txt");
        filesToDelete.add("test2.txt");
        filesToDelete.add("test3.txt");
        filesToDelete.add("test4.txt");

        int i = 0;
//        This loop will check the state of storage every 1 second.
//        If the state is READY, a file is added to the storage
        while (true) {
            Thread.sleep(THREAD_POOL_INTERVAL);
            MerkleState state = storage.getState();
            Logger.info(ID, "Merkle Storage State: " + state);

            if (state == MerkleState.READY) {
                Logger.info(ID, "Adding file: " + filesToAdd.get(i));
                storage.addFile(Paths.get(filesToAdd.get(i)));
                i += 1;
                if (i == 4) {
                    break;
                }
            }
        }
//        This loop will check the state of storage every 1 second.
//        If the state is READY, a file is added to the storage
        i = 0;
        while(true) {
            Thread.sleep(THREAD_POOL_INTERVAL);
            MerkleState state = storage.getState();
            Logger.info(ID, "Merkle Storage State: " + state);

            if (storage.getState() == MerkleState.READY) {
                Logger.info(ID, "Deleting file: " + filesToDelete.get(i));
                storage.deleteFile(filesToDelete.get(i));
                i += 1;
                if (i == 4) {
                    break;
                }
            }
        }
//        This loop checks and output the state of the storage every 1 second.
        while (true) {
            Thread.sleep(THREAD_POOL_INTERVAL);
            MerkleState state = storage.getState();
            Logger.info(ID, "Merkle Storage State: " + state);
        }
    }
}
