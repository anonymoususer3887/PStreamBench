import requests
import time
job_query_mapping = {
    #"Word Count": [1],
    #"Sentiment Analysis": [1],
    #"TPCH": [1],
    #
    #"Smart Grid": [1],
    #"Log Processing":[1],
    #"Spike Detection": [1],
    #"Bargain Index": [1],
    #"Machine Outlier": [1],
    #"Traffic Monitoring": [1],
    #"Click Analytics": [1],
    #"Trending Topics": [1],

    "Ad Analytics": [1],

    #"Click Analytics": [1,2],
    #"Log Processing":[1,2],
    #"Smart Grid": [1,2],
    #
    #"Linear Road": [1,2,3,4],
    #"Google cloud Monitoring":[1,2],
}
count = 0
#parallelism_degrees = ['5', '10', '15', '20', '25', '30', '35', '40'] # Have to change this , '32', '8','2'
event_rates = [1000000]#,50000, 20000, 10000, 5000, 1000]# Have to change this , 20000, 10000, 5000, 1000
#cluster_id = '71d4b11b-9582-408f-80a7-218b6218cefd' # storm # Have to change this
cluster_id = 'ae774125-69ac-4238-a652-2bfb8baa4e97' # flink # Have to change this

count=0

for job_name in job_query_mapping:
    queries = job_query_mapping[job_name]
    for query in queries:
        #for parallelism_degree in parallelism_degrees:
            for event_rate in event_rates:
                data_sent = {
                    'job_enumeration_strategy': 'Custom',
                    #'job_custom_strategy': "5,5,5,5,5,5,5\n10,10,10,10,10,10,10\n15,15,15,15,15,15,15\n20,20,20,20,20,20,20\n25,25,25,25,25,25,25\n30,30,30,30,30,30,30\n35,35,35,35,35,35,35\n40,40,40,40,40,40,40\n45,45,45,45,45,45,45\n50,50,50,50,50,50,50\n55,55,55,55,55,55,55\n60,60,60,60,60,60,60\n65,65,65,65,65,65,65\n70,70,70,70,70,70,70\n75,75,75,75,75,75,75\n80,80,80,80,80,80,80",
                    'job_custom_strategy': "5,5,5,5,5,5,5\n10,10,10,10,10,10,10\n15,15,15,15,15,15,15\n20,20,20,20,20,20,20\n25,25,25,25,25,25,25\n30,30,30,30,30,30,30\n35,35,35,35,35,35,35\n40,40,40,40,40,40,40",
                    'job_class': job_name,
                    'job_class_input_type': 'Kafka',
                    'job_class_input': event_rate,
                    'job_query_name': query,
                    #'job_pds': [parallelism_degree, parallelism_degree, parallelism_degree, parallelism_degree, parallelism_degree, parallelism_degree, parallelism_degree],
                    'job_pds': [],
                    'job_window_size': '10',
                    'job_window_slide_size': '10',
                    'job_run_time': 6,
                    'job_google_lateness': 0,
                    'job_threshold': 0,
                    'num_of_times_job_run': 1,
                    'job_pds_steps': [],
                    'job_class_input_max': event_rate,
                    'job_class_input_min': event_rate,
                    'job_window_size_max': 10,
                    'job_window_size_min': 10,
                    'job_window_slide_size_max': 10,
                    'job_window_slide_size_min': 10,
                    'job_iterations': 0,
                    'job_iteration_strategy': 'From Top',
                }
                print('##################################################################\n\n\n\n')
                print("Data sending...\n")
                print(data_sent)
                requests.post("http://localhost:8000/infra/jobcreate/"+cluster_id, json=data_sent)
                #count+=1
                #print((count*4)/60)
                print('DONE')
                print('##################################################################\n\n\n\n')
    time.sleep(60)
