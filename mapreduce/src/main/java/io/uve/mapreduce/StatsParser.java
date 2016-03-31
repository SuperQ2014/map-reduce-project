package io.uve.mapreduce;

import org.json.*;

public class StatsParser {
    public static final String L1S = "\\x1A";
    public static final String L1CS = "\\x1C";
    public static final String L2S = "\\x01";
    public static final String L2CS = "\\x1D";
    public static final String L3S = "\\x1E";
    public static final String L3CS = "\\x1F";

    private final String log;
    private JSONObject json = new JSONObject();
    private int l2Count = 0;
    private int l3Count = 0;
    private int availablePos = 0;

    public StatsParser(String log) {
        this.log = log;
        this.parse();
    }

    public int getL2Count() {
        return this.l2Count;
    }

    public int getL3Count() {
        return this.l3Count;
    }
    
    public int getAvailablePos() {
    	return this.availablePos;
    }

    private void parse() {
        JSONArray arr = this.doParse(this.log, L1S, L1CS);
        this.json = arr.getJSONObject(0);
    }

    private JSONArray doParse(String log, String S, String CS) {
        String[] parts = log.split(S);
        JSONArray arr = new JSONArray();
        for (String s : parts) {
            JSONObject obj = new JSONObject();
            String[] xParts = s.split(CS);
            for (String kv : xParts) {
                int c = kv.indexOf(":");
                if (c < 1) {
                    continue;
                }
                String k = kv.substring(0, c);
                String v = kv.substring(c + 1);
                if (k.compareTo("tmeta_l2") == 0) {
                    JSONArray l2 = this.doParse(v, L2S, L2CS);
                    this.l2Count += l2.length();
                    obj.put(k, l2);
                } else if (k.compareTo("tmeta_l3") == 0) {
                    JSONArray l3 = this.doParse(v, L3S, L3CS);
                    this.l3Count += l3.length();
                    obj.put(k, l3);
                } else {
                    obj.put(k, v);
                }
            }
            arr.put(obj);
        }
        return arr;
    }

    public JSONObject getJSONObject() {
        return this.json;
    }

    /*public static void main(String[] args) {
        String file = "stats_log_sample.log";

        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String log = null;
            while ((log = reader.readLine()) != null) {
                StatsParser p = new StatsParser(log);
                JSONObject json = p.getJSONObject();
                System.out.print(json.getString("uid"));
                int available_pos_num = json.getInt("available_pos");
                System.out.println(", " + available_pos_num);
                System.out.print(", L2: " + p.getL2Count());
                System.out.println(", L3: " + p.getL3Count());
            }
        } catch (Exception ex) {
            System.out.println(ex);
        }
    }*/
}
