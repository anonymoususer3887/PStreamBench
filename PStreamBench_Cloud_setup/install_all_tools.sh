# Copy custom_producer.py
cp /local/repository/custom_producer.py /home/playground/

# Copy smartgrid data file
#cp /local/repository/smart-grid.xlsx /home/playground/
#copy adAnalytics data file
#cp /local/repository/adAnalytics.dat /home/playground/
#copy googleCloudMonitoring.csv file

#cp /local/repository/googleCloudMonitoring.csv /home/playground
#
##copy sentiment analysis  file
#
#cp /local/repository/sentimentAnalysis_tweetstream.jsonl /home/playground
#
##copy spike detection file
#
#cp /local/repository/spikeDetection_sensors.dat /home/playground
##wget -P /home/playground/  https://code.jquery.com/jquery-3.6.0.min.js
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/click-stream.json?inline=false -O click-stream.json
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/http.log?inline=false -O http.log
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/outlier.csv?inline=false -O outlier.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/stocks.csv?inline=false -O stocks.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/ttopic.csv?inline=false -O ttopic.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/lrb.csv?inline=false -O lrb.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/taxi-traces.csv?inline=false -O taxi-traces.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/tpc_h.csv?inline=false -O tpc_h.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/adAnalytics.dat?inline=false -O adAnalytics.dat
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/googleCloudMonitoring.csv?inline=false -O googleCloudMonitoring.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/sentimentAnalysis_tweetstream.json?inline=false -O sentimentAnalysis_tweetstream.json
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/smart-grid.csv?inline=false -O smart-grid.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/spikeDetection_sensors.dat?inline=false -O spikeDetection_sensors.dat
#
#cp /click-stream.json /home/playground
#cp /http.log /home/playground
#cp /outlier.csv /home/playground
#cp /stocks.csv /home/playground
#cp /ttopic.csv /home/playground
#cp /lrb.csv /home/playground
#cp /taxi-traces.csv /home/playground
#cp /tpc_h.csv /home/playground
#cp /adAnalytics.dat /home/playground
#cp /googleCloudMonitoring.csv /home/playground
#cp /sentimentAnalysis_tweetstream.json /home/playground
#cp /smart-grid.csv /home/playground
#cp /spikeDetection_sensors.dat /home/playground
# cp /local/repository/click-stream.json /home/playground
# cp /local/repository/http.log /home/playground
# cp /local/repository/outlier.csv /home/playground
# cp /local/repository/stocks.csv /home/playground
# cp /local/repository/ttopic.csv /home/playground


# Downloading flink 
#curl -L https://dlcdn.apache.org/flink/flink-1.16.2/flink-1.16.2-bin-scala_2.12.tgz > /home/playground/zip/flink.tgz
curl -0 https://archive.apache.org/dist/flink/flink-1.16.2/flink-1.16.2-bin-scala_2.12.tgz --output /home/playground/zip/flink.tgz
# Unzip flink to playgrounds directory
tar zxf /home/playground/zip/flink.tgz -C /home/playground/

# Downloading storm
curl -0 https://archive.apache.org/dist/storm/apache-storm-2.6.2/apache-storm-2.6.2.tar.gz --output /home/playground/zip/storm.tar.gz
# Unzip flink to playgrounds directory
tar zxf /home/playground/zip/storm.tar.gz -C /home/playground/

# Copy oshi,jna and jna_platform
cp /local/repository/jna-platform-5.10.0.jar /home/playground/flink-1.16.2/lib
cp /local/repository/jna-5.10.0.jar /home/playground/flink-1.16.2/lib
cp /local/repository/oshi-core-6.1.5.jar /home/playground/flink-1.16.2/lib

# Copy dsp_jobs files
cp /local/repository/dsp_jobs-1.0-SNAPSHOT.jar /home/playground/flink-1.16.2/bin

# Copy roads.geojson file to flink/bin

cp /local/repository/roads.geojson /home/playground/flink-1.16.2/bin 

# Downloading prometheus 
curl -L https://github.com/prometheus/prometheus/releases/download/v2.42.0/prometheus-2.42.0.linux-amd64.tar.gz > /home/playground/zip/prometheus.tar.gz
# Unzip prometheus to playgrounds directory
tar zxf /home/playground/zip/prometheus.tar.gz -C /home/playground/

# Setup storm exporter
sudo pip install prometheus_client
cp /local/repository/storm_exporter.py /home/playground/apache-storm-2.6.2/bin

# Download grafana
curl -L https://dl.grafana.com/enterprise/release/grafana-enterprise-9.3.6.linux-amd64.tar.gz > /home/playground/zip/grafana.tar.gz
# Unzip grafana to playgrounds directory
tar zxf /home/playground/zip/grafana.tar.gz -C /home/playground/


# Download kafka
curl -L https://downloads.apache.org/kafka/3.8.0/kafka_2.12-3.8.0.tgz > /home/playground/zip/kafka.tgz

# Unzip kafka to playgrounds directory
tar zxf /home/playground/zip/kafka.tgz -C /home/playground/

mv /home/playground/kafka_2.12-3.8.0 /home/playground/kafka_2.12

# removing default kafka property
rm /home/playground/kafka_2.12/config/server.properties

# Copying the server.properties to /home/playground
cp /local/repository/server.properties /home/playground/kafka_2.12/config

# Copy prometheus exporter library
cp /local/repository/jmx_prometheus_javaagent.jar /home/playground/kafka_2.12/libs/jmx_prometheus_javaagent.jar

# Copy prometheus exporter config
cp /local/repository/jmx_exporter.yml /home/playground/kafka_2.12/config/jmx_exporter.yml

#rm -r /local/repository

#sudo rm -r /home/playground/zip

