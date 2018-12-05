package ru.maybe;


import com.google.gson.Gson;
import org.apache.http.client.HttpResponseException;
import spark.Request;
import spark.Response;
import spark.Spark;

class HttpRequestHandler {

    private HttpRequestHandler() {

    }

    public static void processRequests(int port) {
        Spark.port(port);
        Spark.after((Request request, Response response) -> {
            response.type("application/json");
        });
        Spark.get("/average", (request, response) -> CafeData.getAverageCafeVisits(), JsonUtil::toJson);
    }

    public interface ResponseTransformer {
        String render(Object model) throws HttpResponseException;
    }

    public static class JsonUtil {
        private JsonUtil() {
        }

        public static String toJson(Object object) {
            return new Gson().toJson(object);
        }

        public static ResponseTransformer json() {
            return JsonUtil::toJson;
        }
    }
}