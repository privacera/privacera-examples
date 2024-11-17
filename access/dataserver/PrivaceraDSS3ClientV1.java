import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

/**
 * This class demonstrates how to use the Privacera Dataserver S3 client to upload an object to S3 Using the AWS SDK V1
 */
public class PrivaceraDSS3ClientV1 {

    private static String dsUrl = "<replace-with-ds-url>";
    private static String pAccessKey = "<replace-with-privacera-access-key>";
    private static String pSecretKey = "<replace-with-privacera-secret-key>";
    private static String bucketName = "<replace-with-s3-bucket>";
    private static String keyName = "<replace-with-s3--key>";
    private static String region = "<replace-with-region>";
    private static AmazonS3 s3Client;

    public static void main(String[] args) throws Throwable {
        System.out.println("Initializing PrivaceraDSS3ClientV1");
        uploadTest();
    }

    private static void uploadTest() throws Throwable {
        System.out.println("PrivaceraDSS3ClientV1 uploadTest called");

        try {
            createS3Client();
            uploadObject();
        } catch (Throwable thr) {
            System.err.println("Failed to run test, error=" + thr.getMessage());
            thr.printStackTrace();
        } finally {
            closeS3();
        }
    }

    private static void createS3Client() throws Exception {
        System.out.println("Creating s3 client, dsUrl=" + dsUrl + ", pAccessKey=" + pAccessKey);

        // credentials
        AWSStaticCredentialsProvider credential = new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(pAccessKey, pSecretKey));

        // path style access
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(credential);
        amazonS3ClientBuilder.withPathStyleAccessEnabled(true);

        // endpoint configuration
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(dsUrl,
                                                                                                                  region);
        amazonS3ClientBuilder.withEndpointConfiguration(endpointConfiguration);

        // ssl configuration
        SSLContextBuilder builder = new SSLContextBuilder();
        builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.getApacheHttpClientConfig().setSslSocketFactory(sslsf);
        amazonS3ClientBuilder.withClientConfiguration(clientConfig);

        s3Client = amazonS3ClientBuilder.build();
    }

    private static void closeS3() {
        if (s3Client != null) {
            s3Client.shutdown();
            s3Client = null;
        }
    }

    private static void uploadObject() throws IOException {
        ByteArrayInputStream bais = null;

        try {
            System.out.println("Uploading object, bucketName=" + bucketName + ", keyName=" + keyName);

            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH-mm-ss-SSS");
            LocalDateTime now = LocalDateTime.now();
            String time = dtf.format(now);

            ObjectMetadata objectMeta = new ObjectMetadata();
            objectMeta.addUserMetadata("x-amz-custom-meta", "privacera-ds-client");

            String content = "DataServer Client - Uploaded Using AWS SDK V1\nTime=" + time;
            bais = new ByteArrayInputStream(content.getBytes("UTF-8"));

            PutObjectRequest putObj = new PutObjectRequest(bucketName, keyName, bais, objectMeta);
            PutObjectResult result = s3Client.putObject(putObj);

            if (result != null) {
                System.out.println("Upload successful, ETag=" + result.getETag());
            } else {
                System.err.println("Failed to upload file, result is null");
            }

        } catch (AmazonS3Exception ex) {
            System.err.println("Failed to upload file, error=" + ex.getErrorMessage());
            ex.printStackTrace();
        } catch (Exception ex) {
            System.err.println("Failed to upload file, error=" + ex.getMessage());
            ex.printStackTrace();
        } finally {
            if (bais != null) {
                bais.close();
            }
        }
    }
}
