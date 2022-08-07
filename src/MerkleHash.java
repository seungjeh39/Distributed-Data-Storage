import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class MerkleHash {
    // This class provides one static main function (getRootHash) which returns the hash of one directory given its path
    // This implements the logic of the Merkle tree in an ArrayList<>

    public static String getRootHash(Path dir) throws IOException, NoSuchAlgorithmException {
        // Collect all the files from the directory
        final List<Path> pathsInDir = Files.walk(dir)
                                    .sorted(Comparator.reverseOrder())
                                    .collect(Collectors.toList());

        // Compute the hash for each file
        ArrayList<String> fileHash = buildTree(pathsInDir, dir);

        // Return the rootHash from the List<> of hashes
        return computeRootHash(fileHash);
    }

    private static ArrayList<String> buildTree(List<Path> pathsInDir, Path rootDir) throws NoSuchAlgorithmException, IOException {
        // This function returns a List<String> where each element is the hash of each file in the directory
        final ArrayList<String> hashStrings = new ArrayList<>();
        for(Path path : pathsInDir) {
            // Only computes the hashes of children files.
            // The rootDir (the replica file) is different for each replica, computing it will not lead to majority
            if (path.equals(rootDir))
                continue;

            // If it is a regular file, combine the binary of its content and its name,
            // then create the hash of that binary stream
            if (Files.isRegularFile(path)) {
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                byte[] bytes = Files.readAllBytes(path);
                outputStream.write(bytes);
                outputStream.write(path.getFileName().toString().getBytes());
                final String fileMD5 = Hash.computeHash(outputStream.toByteArray());
                hashStrings.add(fileMD5);
            } else {
                // If it is not a regular file (i.e. a directory),
                // compute the hash from the file name's binary
                final byte[] bytes = path.getFileName().toString().getBytes();
                final String nameMD5 = Hash.computeHash(bytes);
                hashStrings.add(nameMD5);
            }
        }
        return hashStrings;
    }

    private static String computeRootHash(ArrayList<String> fileHash) throws NoSuchAlgorithmException {
        // Given the List<String> of hash for each file,
        // this function implements the Merkle tree logic,
        // where two adjacent elements are hashed repeatedly until only one hash remains,
        // which is the hash of the directory

        List<String> newList = fileHash;
        int size = fileHash.size();
        if (size == 0) return "";
        while (size > 1) {
            List<String> resultList = new ArrayList<>();
            boolean odd = (size % 2 != 0);
            int numLoop = size;
            if (odd)
                numLoop = size - 1;

            for (int i = 0; i < numLoop; i += 2) {
                resultList.add(mergeHash(newList.get(i), newList.get(i + 1)));
            }

            // If the List<> has odd number of elements, the last element is kept untouched
            if (odd)
                resultList.add(mergeHash(newList.get(size - 1), newList.get(size - 1)));

            newList = resultList;
            size = newList.size();
        }
        return newList.get(0);
    }

    private static String mergeHash(String hash1, String hash2) throws NoSuchAlgorithmException {
        // This function builds the non-leaf nodes of the Merkle tree as it create has from two existing hashes
        return Hash.merge(hash1 + hash2);
    }

}