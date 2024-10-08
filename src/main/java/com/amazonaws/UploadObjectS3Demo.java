package com.amazonaws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.core.signer.NoOpSigner;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
// import software.amazon.awssdk.http.auth.aws.internal.signer.DefaultAwsV4HttpSigner;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
// import software.amazon.awssdk.services.s3.model.RequestOverrideConfiguration;
import software.amazon.awssdk.core.RequestOverrideConfiguration;
import software.amazon.awssdk.core.signer.Signer;
// import software.amazon.awssdk.core.signer.SignerOverride;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.Map;


public class UploadObjectS3Demo {

    public static Long getFileSize(AmazonS3 s3Client, String bucketName, String key) {

         return s3Client.getObject(bucketName, key).getObjectMetadata().getInstanceLength();
    }

    public static void main(String[] args) throws Exception {
        S3Client s3Client = initClient();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD");
        // // Create a RequestOverrideConfiguration
        // RequestOverrideConfiguration requestOverrideConfig = RequestOverrideConfiguration.builder()
        //         .putHeader("Custom-Header", "Value")
        //         .build();
        // Create an initial AwsRequestOverrideConfiguration
        AwsRequestOverrideConfiguration initialConfig = AwsRequestOverrideConfiguration.builder()
                .putHeader("Initial-Header", "InitialValue2")
                .build();

        // // Create a new AwsRequestOverrideConfiguration from the initial one
        AwsRequestOverrideConfiguration newConfig = AwsRequestOverrideConfiguration.from(initialConfig)
                .toBuilder()
                .putHeader("X-Amz-Content-Sha256", "required")
                // .putHeader("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                .build();

        // // Create a custom signer (for demonstration purposes, using the default signer)
        // Signer customSigner = SignerOverride.create("AWSS3V4SignerType");

        // // Create an AwsRequestOverrideConfiguration with the custom signer
        // AwsRequestOverrideConfiguration overrideConfig = AwsRequestOverrideConfiguration.builder()
        //         .signer(customSigner)
        //         .build();


        File file = new File("/root/go/src/deeproute/s3-java-demo/xxxxx");
        // InputStream fis = new FileInputStream(file);
        InputStream fis = Files.newInputStream(file.toPath());
        PutObjectResponse putObjectResponse = s3Client.putObject(PutObjectRequest.builder()
                        // .bucket("stg-data-1") // s3 bucket
                        // .bucket("zhengliang") // s3 bucket
                        .bucket("test") // s3 bucket
                        .key("jjjjjj")
                        // .putHeader(metadata)
                        // .overrideConfiguration(newConfig)
                        // .overrideConfiguration(overrideConfig)
                        // .metadata(metadata)
                        .contentLength(file.length())
                        .build(),
                // RequestBody.fromContentProvider(ContentStreamProvider.fromInputStream(fis, file.length()), "")); // NOTE: create a empty file first
                RequestBody.fromInputStream(fis, file.length())); // NOTE: create a empty file first
                // RequestBody.fromFile(file)); // NOTE: create a empty file first
        System.out.println(putObjectResponse.eTag());
    }

    public static S3Client initClient() throws Exception{
        return generateS3Client(S3Config.builder()
                // .endpoint("http://10.3.11.81:80") // ceph pacific v18.2.4 will fail
                // .endpoint("http://10.3.8.201:80") // ceph pacific v18.2.4 will fail
                // .endpoint("http://10.3.9.141:80") // ceph pacific v16.2.13 will fail
                // .endpoint("http://10.9.8.72:80") // ceph pacific v16.2.15 will fail
                .endpoint("http://10.9.8.95:80") // ceph pacific v16.2.14 will fail
                // .endpoint("http://10.9.8.102:80") // ceph pacific v16.2.14 is OK
                .accessKey("testy") // ak 
                .secretKey("testy") // sk
                .pathStyleAccessEnabled(true)
                // .chunkedEncodingEnabled(false)
                .build());
    }

    public static S3Client generateS3Client(S3Config config)throws Exception {
        S3ClientBuilder s3ClientBuilder = S3Client.builder();
        // Default value
        Boolean pathStyleAccessEnabled = Optional.ofNullable(config.getPathStyleAccessEnabled())
                .orElse(S3ConfigConst.DEFAULT_PATH_STYLE_ACCESS_ENABLED);
        String region = Optional.ofNullable(config.getRegion()).orElse(S3ConfigConst.DEFAULT_REGION.id());
        Long socketTimeout = Optional.ofNullable(config.getReadTimeout()).orElse(S3ConfigConst.DEFAULT_READ_TIMEOUT);
        Long connectionTimeout = Optional.ofNullable(config.getConnectionTimeout()).orElse(S3ConfigConst.DEFAULT_CONNECTION_TIMEOUT);
        Integer maxConnections = Optional.ofNullable(config.getMaxConnections()).orElse(S3ConfigConst.DEFAULT_MAX_HTTP_CONNECTION_POOL_SIZE);

        
        // // Aws4Signer defaultSigner = Aws4Signer.create();
        // ClientOverrideConfiguration overrideConfig =
        //         ClientOverrideConfiguration.builder()
        //                 .putAdvancedOption(SdkAdvancedClientOption.SIGNER, NoOpSigner.class)
        //                 .build();

        // Set base config
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey());
        s3ClientBuilder.credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials));
        s3ClientBuilder.endpointOverride(new URI(config.getEndpoint()));
        s3ClientBuilder.region(Region.of(region));
        s3ClientBuilder.serviceConfiguration(builder -> {
            // builder.pathStyleAccessEnabled(pathStyleAccessEnabled);
            builder.chunkedEncodingEnabled(false);
        });

        // Retry
        ClientOverrideConfiguration.Builder clientOverrideConfigurationBuilder = ClientOverrideConfiguration.builder();
        s3ClientBuilder.overrideConfiguration(clientOverrideConfigurationBuilder.build());

        // s3ClientBuilder.overrideConfiguration(overrideConfig);

        // Set http client config
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();
        httpClientBuilder.socketTimeout(Duration.ofMillis(socketTimeout));
        httpClientBuilder.connectionTimeout(Duration.ofMillis(connectionTimeout));
        httpClientBuilder.maxConnections(maxConnections);
        s3ClientBuilder.httpClient(httpClientBuilder.build());
        return s3ClientBuilder.build();
    }

    public static class S3ConfigConst {
        /**
         * Default value for max http connection pool size
         */
        public static final Integer DEFAULT_MAX_HTTP_CONNECTION_POOL_SIZE = 1000;

        /**
         * Default value for minimum part size in bytes
         */
        public static final Long DEFAULT_MINIMUM_PART_SIZE_IN_BYTES = 30 * 1024 * 1024L;

        /**
         * Default value for region
         */
        public static final Region DEFAULT_REGION = Region.US_EAST_1;

        /**
         * Default value for path style access
         */
        public static final Boolean DEFAULT_PATH_STYLE_ACCESS_ENABLED = true;

        /**
         * Default value for throughput in Gbps
         */
        public static final Double DEFAULT_THROUGHPUT_IN_GBPS = 10d;

        /**
         * Default value for connection timeout
         */
        public static final Long DEFAULT_CONNECTION_TIMEOUT = 3000L;

        /**
         * Default value for netty write time out
         */
        public static final Long DEFAULT_WRITE_TIMEOUT = 10000L;

        /**
         * Default value for socket timeout
         */
        public static final Long DEFAULT_READ_TIMEOUT = 5000L;

        /**
         * The default value for name of the bucket
         */
        public static final String NOT_EXISTED_BUCKET = "NOT_EXISTED_BUCKET";

        /**
         * The default value for enable retry
         */
        public static final Boolean DEFAULT_ENABLE_RETRY = false;

        /**
         * The default value for max retry times.<br/>
         * Retry interval: 100ms, random(0,100ms*2^1), random(0,100ms*2^2), random(0,100ms*2^3), random(0,100ms*2^4)...<br/>
         */
        public static final Integer DEFAULT_MAX_RETRY_TIMES = 3;
    }


    @Data
    @SuperBuilder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class S3Config {
        private String accessKey;

        private String secretKey;

        private String endpoint;

        /**
         * The region.
         */
        private String region;

        /**
         * The millions for connection timeout time
         */
        private Long connectionTimeout;

        /**
         * The millions for read timeout time, this value is just for S3Client
         */
        private Long readTimeout;
        /**
         * The amount of millions time to wait for a write on a socket before an exception is thrown. Specify null or 0 to disable.
         */
        private Long writeTimeout;

        /**
         * The max connections for S3Client or S3AsyncClient
         */
        private Integer maxConnections;

        /**
         * If use path style access
         */
        private Boolean pathStyleAccessEnabled;

        /**
         * If use chunked encoding
         */
        private Boolean chunkedEncodingEnabled;

        /**
         * If retry
         */
        private Boolean enableRetry;

        /**
         * The max retry times
         */
        private Integer maxRetryTimes;
        /**
         * 客户端线程配置
         */
        private ClientExecutor clientExecutor = new ClientExecutor();

        /**
         * 客户端线程池相关配置
         */
        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class ClientExecutor {
            private Integer corSize = 5;
            private String threadNamePrefix = "s3-call";
        }
    }
}
