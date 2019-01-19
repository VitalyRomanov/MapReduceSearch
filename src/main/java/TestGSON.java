import com.google.gson.Gson;

public class TestGSON {
    public static void main(String[] args) {
        Gson gson = new Gson();
        int [] a = new int[100];

        String json = gson.toJson(a);
        System.out.println(json);

        int [] b = gson.fromJson(json, int[].class);
        System.out.println(b.length);
    }
}