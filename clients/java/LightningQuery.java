// Minimal Lightning Queries REST client. Java 17+, no build tool, no libraries.
//
// Run:   java clients/java/LightningQuery.java "SELECT * FROM lq_orders LIMIT 2"
// Needs: the environment variables LQ_ENV_ID, LQ_CLUSTER_ID, LQ_API_KEY, LQ_API_SECRET
//        (Global API key), optional LQ_REGION (default eu-central-1).
// Prints the HTTP status, the elapsed milliseconds and the raw JSON body.

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class LightningQuery {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: java LightningQuery.java \"SELECT ...\"");
            System.exit(1);
        }
        List<String> missing = new ArrayList<>();
        String envId = setting("LQ_ENV_ID", missing);
        String clusterId = setting("LQ_CLUSTER_ID", missing);
        String key = setting("LQ_API_KEY", missing);
        String secret = setting("LQ_API_SECRET", missing);
        if (!missing.isEmpty()) {
            System.err.println("Missing environment variables: " + String.join(", ", missing));
            System.exit(1);
        }
        String region = System.getenv().getOrDefault("LQ_REGION", "eu-central-1");

        String url = "https://sql.%s.aws.confluent.cloud/query/v1".formatted(region);
        String auth = Base64.getEncoder().encodeToString(
                (key.strip() + ":" + secret.strip()).getBytes(StandardCharsets.UTF_8));
        String body = """
                {"catalog_name": "%s", "database_name": "%s", "query": "%s",
                 "options": {"max_result_rows": 200, "result_format": "JSON"}}"""  // 200 keeps the answer inline (no 202)
                .formatted(envId, clusterId, jsonEscape(args[0]));

        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(60))
                .header("Authorization", "Basic " + auth)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        long start = System.nanoTime();
        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());
        long ms = (System.nanoTime() - start) / 1_000_000;

        System.out.println("HTTP " + response.statusCode() + " in " + ms + " ms");
        System.out.println(response.body());
        System.exit(response.statusCode() == 200 ? 0 : 1);
    }

    private static String setting(String name, List<String> missing) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            missing.add(name);
            return "";
        }
        return value;
    }

    // Escape the SQL for a JSON string: backslash, double quote, control characters.
    private static String jsonEscape(String s) {
        StringBuilder out = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        return out.toString();
    }
}
