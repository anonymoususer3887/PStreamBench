package com.kom.dsp.machineOutlier;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kom.dsp.machineOutlier.BFPRTAlgorithm.bfprt;

public class MachineOutlier2Bolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;
    private static final Logger LOG = LoggerFactory.getLogger(MachineOutlierBolt.class);
    private final double threshold;

    public MachineOutlier2Bolt(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> tuples = tupleWindow.get();
        Map<String, List<MachineUsage>> allMachines = new HashMap<>();
        Map<String, List<Long>> allE2eTimestamps = new HashMap<>();
        Map<String, List<Long>> allProcessingTimestamps = new HashMap<>();

        for(Tuple tuple : tuples){
            MachineUsage machineUsage = (MachineUsage) tuple.getValueByField("machineUsage");
            String id = machineUsage.getMachineId();

            List<MachineUsage> machineUsages = allMachines.getOrDefault(id, new ArrayList<MachineUsage>());
            machineUsages.add(machineUsage);
            allMachines.put(id, machineUsages);

            List<Long> e2eTimestamps = allE2eTimestamps.getOrDefault(id, new ArrayList<Long>());
            e2eTimestamps.add(tuple.getLongByField("e2eTimestamp"));
            allE2eTimestamps.put(id, e2eTimestamps);

            List<Long> processingTimestamps = allProcessingTimestamps.getOrDefault(id, new ArrayList<Long>());
            processingTimestamps.add(tuple.getLongByField("processingTimestamp"));
            allProcessingTimestamps.put(id, processingTimestamps);
        }

        for (String id : allMachines.keySet()) {
            List<MachineUsage> machineUsages = allMachines.get(id);
            List<Long> e2eTimestamps = allE2eTimestamps.get(id);
            List<Long> processingTimestamps = allProcessingTimestamps.get(id);

            // Apply the BFPRT algorithm to detect abnormal readings
            double threshold = 0.5;
            // Check if the input list is empty or contains only one element
            if (machineUsages.isEmpty() || machineUsages.size() == 1) {
                // No outliers can be detected
                return;
            }

            // Convert the list of MachineUsage objects to an array for easy manipulation
            MachineUsage[] arr = machineUsages.toArray(new MachineUsage[0]);

            // Find the kth element using the BFPRT algorithm
            int k = (int) Math.ceil(arr.length * 0.75);  // Choose the value of k as desired (e.g., 75%)
            MachineUsage kthElement = bfprt(arr, 0, arr.length - 1, k);

            // Iterate over the array and collect the abnormal readings
            for (int i = 0; i < arr.length; i++) {
                MachineUsage machineUsage = arr[i];
                LOG.info("threshold value euclidean" + machineUsage.getEuclideanDistance(kthElement));
                if (machineUsage.getEuclideanDistance(kthElement) > threshold) {
                    // Emit the outlier as a string
                    outputCollector.emit(new Values(
                            machineUsage.getMachineId(),
                            machineUsage.getTimestamp(),
                            machineUsage.getCpuUtilPercentage(),
                            machineUsage.getMemUtilPercentage(),
                            machineUsage.getMissPerThousandInstructions(),
                            machineUsage.getNetIn(),
                            machineUsage.getNetOut(),
                            machineUsage.getDiskIO(),
                            e2eTimestamps.get(i),
                            processingTimestamps.get(i))
                    );
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                "machineId",
                "timestamp",
                "cpuUtilPercentage",
                "memUtilPercentage",
                "missPerThousandInstructions",
                "netIn",
                "netOut",
                "diskIO",
                "e2eTimestamp",
                "processingTimestamp"
        ));
    }
}
