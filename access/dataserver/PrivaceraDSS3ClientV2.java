import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * This class demonstrates how to use the Privacera Dataserver S3 client to upload an object to S3 Using the AWS SDK V2
 */
public class PrivaceraDSS3ClientV2 {

    private static String dsUrl = "<replace-with-ds-url>";
    private static String pAccessKey = "<replace-with-privacera-access-key>";
    private static String pSecretKey = "<replace-with-privacera-secret-key>";
    private static String bucketName = "<replace-with-s3-bucket>";
    private static String keyName = "<replace-with-s3--key>";
    private static String region = "<replace-with-region>";
    private static S3Client s3Client;

    public static void main(String[] args) throws Throwable {
        System.out.println("Initializing PrivaceraDSS3ClientV2");
        uploadTest();
    }

    private static void uploadTest() throws Throwable {
        System.out.println("PrivaceraDSS3ClientV2 uploadTest called");

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
        System.out.println("Creating S3 client, dsUrl=" + dsUrl + ", pAccessKey=" + pAccessKey);

        // credentials
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(pAccessKey, pSecretKey);
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(awsCreds);

        // path style access
        S3Configuration s3Configuration = S3Configuration.builder().pathStyleAccessEnabled(true).build();

        // ssl configuration
        SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
        sslContextBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
        SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContextBuilder.build());

        // http client
        SdkHttpClient httpClient = ApacheHttpClient.builder().socketFactory(sslSocketFactory).build();

        s3Client = S3Client.builder().credentialsProvider(credentialsProvider).endpointOverride(URI.create(dsUrl))
                .region(Region.of(region)).httpClient(httpClient).serviceConfiguration(s3Configuration).build();
    }

    private static void closeS3() {
        if (s3Client != null) {
            s3Client.close();
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

            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-custom-meta", "privacera-ds-client");

            String content = "DataServer Client - Uploaded Using AWS SDK V2\nTime=" + time;
            bais = new ByteArrayInputStream(content.getBytes("UTF-8"));

            PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName).key(keyName).metadata(metadata)
                    .build();

            PutObjectResponse response = s3Client.putObject(putObjectRequest,
                                                            RequestBody.fromBytes(content.getBytes("UTF-8")));

            if (response != null) {
                System.out.println("Upload successful, ETag=" + response.eTag());
            } else {
                System.err.println("Failed to upload file, response is null");
            }

        } catch (S3Exception ex) {
            System.err.println("Failed to upload file, error=" + ex.awsErrorDetails().errorMessage());
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
