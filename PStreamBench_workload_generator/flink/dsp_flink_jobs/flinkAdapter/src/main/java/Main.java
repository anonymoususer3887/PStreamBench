import SpikeDetection.SpikeDetection;
import io.netty.handler.timeout.TimeoutException;

//import SyntheticQueryGenerator.StorePrintStream;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    static ExecutorService myExecutor = Executors.newCachedThreadPool(); //or whatever
    public static void main(String[] args) throws Exception {
        //StorePrintStream storePrintStream = new StorePrintStream(System.out);
        //System.setOut(storePrintStream);
        //a(storePrintStream);
        LOG.info("Inside Main.main");
        
        //System.out.println("after spike");
        
        CompletableFuture<Void> handle = CompletableFuture.runAsync(() -> {
            try {
            	SpikeDetection sd = new SpikeDetection();
                sd.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        try {
            handle.get(2000, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            handle.cancel(true); // this will interrupt the job execution thread, cancel and close the job
        }
    }

    /**public static void a (StorePrintStream storePrintStream){
        myExecutor.execute(new Runnable() {
            public void run() {
                System.out.println("[run] Inside run.");
                String prefix = "Job has been submitted with JobID ";
                String jobId = null;
                while(true){
                    if(StorePrintStream.printList.size() > 0) {
                        String lastLine = StorePrintStream.printList.getLast();
                        //System.out.println("[run] Last line: " + lastLine);
                        //System.out.println(StringUtils.containsIgnoreCase("[run] Insde run.", " Insde run."));
                        if (StringUtils.containsIgnoreCase(lastLine, prefix)) {
                            System.out.println("[run] Got JobID.");
                            jobId = lastLine.substring(prefix.length());
                            System.out.println(jobId);
                            try {
                                TimeUnit.SECONDS.sleep(60);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }

                            break;
                        }
                    }
                }
            }
        });
    }*/
}
