import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

//Reference: https://www.tutorialspoint.com/java_cryptography/java_cryptography_message_digest.htm
public class Hash {
    // This class compute the hash given binary representation of a file or a string
    // Acts as a helper class to MerkleHash class

    public static String computeHash(byte[] binary) throws NoSuchAlgorithmException {

        //Creating the MessageDigest object
        MessageDigest md = MessageDigest.getInstance("MD5");

        //Passing data to the created MessageDigest Object
        md.update(binary);

        //Compute the message digest
        byte[] digest = md.digest();

        //Converting the byte array in to HexString format
        StringBuffer hexString = new StringBuffer();

        for (int i = 0;i<digest.length;i++) {
            hexString.append(Integer.toHexString(0xFF & digest[i]));
        }

        return hexString.toString();
    }

    public static String merge(String input) throws NoSuchAlgorithmException {
        //Convert the string into byte[] and compute hash
        byte[] binary = input.getBytes();
        return computeHash(binary);
    }
}