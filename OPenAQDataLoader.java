import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OPenAQDataLoader {

    static Data data;

    public static class Node {
        public String location;
        public float parameter_o3;
        public float parameter_no2;
        public float parameter_pm10;
        public float parameter_so2;
        public float latitude;
        public float longitude;

        public void setlonglat(float latitude, float longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }

    public static class Data {
        public List<Node> nodes;
        public List<int[]> edges;

        public Data(List<Node> nodes, List<int[]> edges) {
            this.nodes = nodes;
            this.edges = edges;
        }

        public List<Node> getNodes() {
            return nodes;
        }

        public List<int[]> getEdges() {
            return edges;
        }
    }

    public static List<CSVRecord> loadLocationData(String csvFile) throws IOException {
        FileReader reader = new FileReader(csvFile);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
        List<CSVRecord> records = csvParser.getRecords();
        csvParser.close();
        return records;
    }

    public static Data createConnectedGraph(List<CSVRecord> records) {
        List<Node> nodes = new ArrayList<>();
        List<int[]> edges = new ArrayList<>();

        for (CSVRecord record : records) {
            float latitude = Float.parseFloat(record.get("latitude"));
            float longitude = Float.parseFloat(record.get("longitude"));
            // nodes.add(new float[]{latitude, longitude});
            Node node = new Node();
            node.setlonglat(latitude, longitude);
            nodes.add(node);
        }

        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                Node node1 = nodes.get(i);
                Node node2 = nodes.get(j);
                double distance = Math.sqrt(
                        Math.pow(node1.latitude - node2.latitude, 2) + Math.pow(node1.longitude - node2.longitude, 2));
                if (distance < 0.36){
                    edges.add(new int[] { i, j });
                    edges.add(new int[] { j, i });
                }
            }
        }

        System.out.println("Creating connected graph");
        Data data = new Data(nodes, edges);
        return data;
    }

    // Assign values to the nodes
    public static void assignValuesToNodes(List<Node> nodes, List<CSVRecord> records, int iteration) {
        // for (int j = 0; j < records.size(); j++) {
        CSVRecord record = records.get(iteration);
        for (int i = 0; i < nodes.size(); i++) {
            Node node = nodes.get(i);
            node.location = record.get("location_name_Loc_" + (i + 1));
            node.parameter_o3 = Float.parseFloat(record.get("parameter_o3_Loc_" + (i + 1)));
            node.parameter_no2 = Float.parseFloat(record.get("parameter_no2_Loc_" + (i + 1)));
            node.parameter_pm10 = Float.parseFloat(record.get("parameter_pm10_Loc_" + (i + 1)));
            node.parameter_so2 = Float.parseFloat(record.get("parameter_so2_Loc_" + (i + 1)));
            nodes.set(i, node);
        }
        // }
    }

    // message passing function
    /*
     * public static void messagePassing(Data data) {
     * for (int i = 0; i < data.getNodes().size(); i++) {
     * Node node = data.getNodes().get(i);
     * for (int j = 0; j < data.getEdges().size(); j++) {
     * int[] edge = data.getEdges().get(j);
     * Node neighbor = data.getNodes().get(edge[1]);
     * //implement message passing function
     * 
     * }
     * }
     * }
     */

    // message passing function
    public static void messagePassing(Data data) {
        // Create a temporary list to hold updated parameters for each node
        List<Node> updatedNodes = new ArrayList<>(data.getNodes());

        for (int i = 0; i < data.getNodes().size(); i++) {
            Node node = data.getNodes().get(i);

            float sumO3 = node.parameter_o3;
            float sumNO2 = node.parameter_no2;
            float sumPM10 = node.parameter_pm10;
            float sumSO2 = node.parameter_so2;
            int count = 1; // Count includes the node itself

            // Iterate through edges to get neighbors
            for (int j = 0; j < data.getEdges().size(); j++) {
                int[] edge = data.getEdges().get(j);
                if (edge[0] == i) { // only consider outgoing edges
                    Node neighbor = data.getNodes().get(edge[1]);
                    sumO3 += neighbor.parameter_o3;
                    sumNO2 += neighbor.parameter_no2;
                    sumPM10 += neighbor.parameter_pm10;
                    sumSO2 += neighbor.parameter_so2;
                    count++;
                }
            }

            // Update the parameters of the node with the average of its neighbors
            updatedNodes.get(i).parameter_o3 = sumO3 / count;
            updatedNodes.get(i).parameter_no2 = sumNO2 / count;
            updatedNodes.get(i).parameter_pm10 = sumPM10 / count;
            updatedNodes.get(i).parameter_so2 = sumSO2 / count;
        }

        // Update original nodes with the new values
        for (int i = 0; i < updatedNodes.size(); i++) {
            data.getNodes().set(i, updatedNodes.get(i));
        }
    }

    // Function to run multiple iterations of message passing
    public static void runGNN(Data data, int iterations) {
        for (int iter = 0; iter < iterations; iter++) {
            messagePassing(data);
        }
    }

    

    public static void main(String[] args) {
        String csvFile = "locations.csv";
        System.out.println("Loading Location Data");
        try {
            List<CSVRecord> records = loadLocationData(csvFile);
            System.out.println("Loading Location Data - Complete");
            data = createConnectedGraph(records);
            System.out.println("Nodes: " + data.getNodes().size());
            System.out.println("Edges: " + data.getEdges().size());
            // implement graph iteration using gnn formula for a certian
        } catch (IOException e) {
            e.printStackTrace();
        }
        csvFile = "city_data_red.csv";
        System.out.println("Assigning Node values");
        try {
            List<CSVRecord> records = loadLocationData(csvFile);
            assignValuesToNodes(data.getNodes(), records, 0);
            System.out.println("Assigning Node values - Complete");
        } catch (Exception e) {
            System.out.println("Error in assigning values to nodes");
            e.printStackTrace();
        }
        // print the values of the nodes
        for (Node node : data.getNodes()) {
            System.out.println();
            System.out.println("Location: " + node.location);
            System.out.println("O3: " + node.parameter_o3);
            System.out.println("NO2: " + node.parameter_no2);
            System.out.println("PM10: " + node.parameter_pm10);
            System.out.println("SO2: " + node.parameter_so2);
            System.out.println("Latitude: " + node.latitude);
            System.out.println("Longitude: " + node.longitude);
            System.out.println();
        }
        // Run GNN
        runGNN(data, 2);
        // print the values of the nodes
        for (Node node : data.getNodes()) {
            System.out.println();
            System.out.println("Location: " + node.location);
            System.out.println("O3: " + node.parameter_o3);
            System.out.println("NO2: " + node.parameter_no2);
            System.out.println("PM10: " + node.parameter_pm10);
            System.out.println("SO2: " + node.parameter_so2);
            System.out.println("Latitude: " + node.latitude);
            System.out.println("Longitude: " + node.longitude);
            System.out.println();
        }
    }
}