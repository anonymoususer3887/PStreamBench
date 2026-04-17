<template>
    <div>
            <v-card class="my-6 pa-0" color="white" elevation="24px">
                <v-card-title>
                    <v-sheet color="primary" elevation="24"
                        class="justify-center mt-n12 pl-16 pr-16 pt-6 pb-6 rounded  white--text ">
                        <h3>ML Data Visualization</h3>
                    </v-sheet>
                </v-card-title>
                <v-card-subtitle class="text-right">
                    <v-btn v-if="array_of_individual_user_selections.length>=2" color="primary" class="ma-4" elevation="3"
                        @click="showComparisonOptions()">
                        <v-icon>mdi-plus</v-icon><b>Compare</b>
                    </v-btn>
                </v-card-subtitle>
                <v-card-text>
                    <v-form ref="formingMetrics">
                        <v-row class="mt-6">
                            <v-col cols="3">
                                <v-select background-color="white" color="black" v-model="selected_file"
                                    :items="list_of_files" label="Select Inference Parameter"
                                    outlined @change="onSelectFile">
                                    <template v-slot:selection="item">
                                        {{ mapParameter(item.item) }}
                                    </template>
                                    <template v-slot:item="item">
                                        {{ mapParameter(item.item) }}
                                    </template>
                                </v-select>
                            </v-col>
                        </v-row>
                        <!-- TODO: show dynamic input fields based on options -->
                        <v-row class="mt-6">
                            <v-col cols="3" v-for="(options, name) in list_of_options" :key="name">
                                <v-select background-color="white"
                                          color="black"
                                          v-model="list_of_selected_options[name]"
                                          :items="options"
                                          :label="mapValue(name)"
                                          outlined>
                                </v-select>
                            </v-col>
                        </v-row>
                    </v-form>
                </v-card-text>

                <v-card-actions>
                    <v-spacer></v-spacer>
                    <v-btn :disabled="isBtnDisabled" color="primary" class="ma-4" elevation="3"
                        @click="plotIndividualGraph()">
                        <v-icon>mdi-plus</v-icon><b>Plot Graph</b>
                    </v-btn>
                    <!-- <v-btn :disabled="isBtnDisabled" color="primary" class="ma-4" elevation="3"
                        @click="showComparisonOptions()">
                        <v-icon>mdi-plus</v-icon><b>Compare</b>
                    </v-btn> -->
                </v-card-actions>
            </v-card>

        <!-- <v-card>
            <div id="chartContainer18"></div>
        </v-card> -->
        <v-card class="my-12 pa-0" color="white" elevation="24">
            <v-card-title>
                <v-sheet color="primary" elevation="24"
                        class="justify-center mt-n12 pl-16 pr-16 pt-6 pb-6 rounded  white--text ">
                    <h3>Individual charts</h3>
                </v-sheet>
            </v-card-title>
            <v-card-text>
                <v-row>
                    <v-col v-for="chart,index in array_of_individual_user_selections" :key="index">
                        <div class="pa-3" :id="'chartContainer'+index"></div>
                    </v-col>
                </v-row>
            </v-card-text>
        </v-card> 
        <v-card class="my-12 pa-0" color="white" elevation="24">
            <v-card-title>
                <v-sheet color="primary" elevation="24"
                        class="justify-center mt-n12 pl-16 pr-16 pt-6 pb-6 rounded  white--text ">
                    <h3>Compare charts</h3>
                </v-sheet>
            </v-card-title>
            <v-card-text>
                <v-row>
                    <v-col v-for="chart,index in array_of_compare_user_selection" :key="index">
                        <div class ="pa-3" :id="'comparechartContainer'+index"></div>
                    </v-col>
                </v-row>
            </v-card-text>
        </v-card>

        <v-dialog 
            v-model="showCompareDialog"
            persistent
            max-width="500"
            class="primary">
            <v-card>
                <v-card-title>
                    <span class="text-h5">Compare Metrics</span>
                </v-card-title>
                <!-- <v-card-subtitle>
                    {{ selected }}
                </v-card-subtitle> -->
                <v-card-text> 
                    <div v-for="selectInput,index in array_of_individual_user_selections" :key="index">
                        <v-checkbox v-model="selected" :value="selectInput"
                            :disabled="selectInput.file != selected_file">
                            <template v-slot:label>
                                <v-card class="w-full" dark>
                                    <v-card-text>
                                        <span v-for="(value, key) in selectInput" :key="key">
                                            {{ key }}: {{ value }}<br />
                                        </span>
                                        <span v-if="selectInput.file != selected_file" style="color: red">Mixing parameters is not possible</span>
                                    </v-card-text>
                                </v-card>
                            </template>
                        </v-checkbox>
                    </div>
                </v-card-text>
                <v-card-actions>
                    <v-spacer />
                    <v-btn class="error mr-2" small @click="showCompareDialog=false"> Close </v-btn>
                    <v-btn class="success mr-2" small @click="finishCompareInputs() "> Finish </v-btn>
                </v-card-actions>
            </v-card>
        </v-dialog>
    </div>
</template>
<script>
import axios from 'axios';
import * as d3 from 'd3';

export default {
    name: 'MLDataVis',
    data() {
        return {
            selected: [],
            showCompareDialog: false,
            url: process.env.VUE_APP_URL,

            selected_file: null,
            list_of_files: [],
            list_of_selected_options: {},
            list_of_options: [],
            selected_query: null,
            list_of_queries: [
                'Query Types',
                'Tuple Width',
                'Event Rate',
                'Window Duration',
                'Window Length',
                'Amount of Workers',
                'Parallelism Category',
            ],

            array_of_individual_user_selections: [],
            array_of_compare_user_selection:[],
            graphs: [],
        }
    },
    computed: {
        snackbar: {
            get() {
                return this.$store.state.snackbar;
            },
            set(value) {
                this.$store.commit('setSnackbar', value);
            },
        },
        isBtnDisabled() {
            return this.chosen_query_metrics === null && this.chosen_operator_metrics === null && this.query_option === null;
        }
    },
    methods: {
        mapParameter(item) {
            if (item == "predictions_benchmarking_queries.csv") {
                return "Queries";
            } else if (item == "predictions_unseen_cluster_size.csv") {
                return "Cluster Size";
            } else if (item == "predictions_unseen_event_rates.csv") {
                return "Event Rates";
            } else if (item == "predictions_unseen_hardware.csv") {
                return "Hardware";
            } else if (item == "predictions_unseen_test_query_template.csv") {
                return "Test Query Template";
            } else if (item == "predictions_unseen_tuple_widths.csv") {
                return "Tuple Widths";
            } else if (item == "predictions_unseen_window_durations.csv") {
                return "Window Durations";
            } else if (item == "predictions_unseen_window_lengths.csv") {
                return "Window Lengths";
            } else {
                return item;
            }
        },
        mapValue(item) {
            if (item == "enumeration_strategy") {
                return "Enumeration Strategy";
            } else if (item == "model_template_name") {
                return "Model Template Name";
            } else if (item == "evaluation_metric") {
                return "Evaluation Metric";
            } else if (item == "test_template_name") {
                return "Test Template Name";
            } else if (item == "amount_of_workers") {
                return "Amount of Workers";
            } else if (item == "unseen") {
                return "Unseen";
            } else if (item == "event_rate") {
                return "Event Rate";
            } else if (item == "hardware_configuration") {
                return "Hardware Configuration";
            } else if (item == "tuple_width") {
                return "Tuple Width";
            } else if (item == "window_duration") {
                return "Window Duration";
            } else if (item == "window_length") {
                return "Window Length";
            } else {
                return item;
            }
        },
        async showComparisonOptions(){
            this.showCompareDialog = true;
        },
        async finishCompareInputs() {
            this.array_of_compare_user_selection.push(this.selected)
            this.showCompareDialog=false;

            await axios
                .post(this.url + ':8000/report/getCompareGraphDataInference', this.selected)
                .then(async (resp) => {
                    await resp.data;
                    console.log("data from backend for comparison plot");
                    console.log(resp.data);
                    var container = "#comparechartContainer" + (this.array_of_compare_user_selection.length-1)
                    var title = resp.data.file;

                    this.plotClusterHistogram(container, title, resp.data["values"], "Quantile", "Q-Error", resp.data["clusters"])
                    return
                })
        },
        async refresh() {
            await axios
                .get(this.url + ":8000/report/getInferenceOptions")
                .then((resp) => {
                    this.list_of_files = JSON.parse(resp.data);
                })
        },
        async onSelectFile() {
            console.log("Selected file: ", this.selected_file);
            this.list_of_selected_options = {}
            this.array_of_compare_user_selection = [];
            this.selected = [];
            await axios
                .post(this.url + ":8000/report/getInferenceOptions", {"file": this.selected_file})
                .then((resp) => {
                    console.log(resp)
                    this.list_of_options = JSON.parse(resp.data)
                })
        },
        async onSelectQuery() {
            this.list_of_active_query_options = this.list_of_query_options[this.selected_query];
            this.selected_query_option = null;
        },
        async plotIndividualGraph() {
            let form_data = {...this.list_of_selected_options};
            form_data['file'] = this.selected_file;
            //form_data['query'] = this.selected_query;
            //form_data['query_option'] = this.selected_query_option;
            //form_data['enumeration_strategy'] = this.selected_enumeration_strategy;
            //form_data['test_data'] = this.selected_test_data;
            //form_data['metric'] = this.selected_metric;
            //form_data['test_template'] = this.selected_test_template;

            this.array_of_individual_user_selections.push(form_data)
            console.log(this.array_of_individual_user_selections)

            await axios
                .post(this.url + ':8000/report/getIndividualInferenceGraphData', form_data)
                .then(async (resp) => {
                    await resp.data;
                    var data = JSON.parse(resp.data);

                    var container = "#chartContainer" + (this.array_of_individual_user_selections.length-1);
                    var chartTitle = data.file;
                    var chartSubtitle = data.title;
                    var yLabel = "Q-Error" + (data.average ? ' (average)' : '')
                    this.plotHistogram(container, chartTitle, chartSubtitle, data.values, "Quantile", yLabel);
                });
        },
        plotHistogram(container, title, subtitle, data, x_axis_name, y_axis_name) {
            let maxValue = Number.NEGATIVE_INFINITY;

            for (const key in data) {
            const value = parseFloat(data[key]);
            maxValue = Math.max(maxValue, value);
            }

            maxValue = Math.ceil(maxValue);
            console.log('The maximum value is:', maxValue);


            const margin = { top: 50, right: 50, bottom: 100, left: 50 };
            const width = 480 - margin.left - margin.right;
            const height = 400 - margin.top - margin.bottom;

            let svg = d3.select(container).select('svg');

            // Create a new SVG element if not already present
            svg = d3
              .select(container)
              .append('svg')
              .attr('width', width + margin.right + margin.right + margin.right + margin.right)
              .attr('height', height + margin.bottom + margin.bottom + margin.bottom)
              .append('g')
              .attr('transform', `translate(${2 * margin.left}, ${margin.top})`);

            svg.append("text")
              .attr("x", width / 2)
              .attr("y", 0 - (margin.top / 2))
              .attr("text-anchor", "middle")
              .style("font-size", "16px")
              .style("fill", "white")
              .attr("dy", -5)
              .text(title);

            svg.append("text")
              .attr("x", width / 2)
              .attr("y", 0 - (margin.top / 2))
              .attr("text-anchor", "middle")
              .style("font-size", "16px")
              .style("fill", "white")
              .attr("dy", 15)
              .text(subtitle);

            const xScale = d3
              .scaleBand()
              .range([0, width])
              .padding(0.1)
              .domain(Object.keys(data));
            //d3.max(Object.values(data))
            const yScale = d3
              .scaleLinear()
              .range([height, 0])
              .domain([0, 1.5 * maxValue])
              .nice();

            svg.append("g")
              .attr("transform", `translate(0, ${height})`)
              .call(d3.axisBottom(xScale)
                .tickSizeOuter(0))
              .selectAll("text")
              .style("fill", "white")
              .attr("transform", "rotate(-45)") // Rotate the x-axis labels by -45 degrees
              .style("text-anchor", "end");

            // Add Y Axis
            svg.append("g")
              .call(d3.axisLeft(yScale)
                .tickSizeOuter(0))
              .selectAll("text")
              .style("fill", "white");

            // Adding x and y gridlines
            svg.append("g")
              .attr("class", "grid")
              .attr("transform", "translate(0," + height + ")")
              .call(d3.axisBottom(xScale)
              .tickSize(-height)
              .tickFormat("")
            );

            svg.append("g")
              .attr("class", "grid")
              .call(d3.axisLeft(yScale)
              .tickSize(-width)
              .tickFormat("")
            );

            // Add the x axis label
            svg.append("text")
              .attr("class", "x-axis-label")
              .attr("x", width / 2)
              .attr("y", height + margin.bottom + 40)
              .attr("text-anchor", "middle")
              .style("fill", "#FFFDD0")
              .attr("dy", "0.5em")
              .text(x_axis_name);

            // Add the y axis label
            svg.append("text")
              .attr("class", "y-axis-label")
              .attr("x", 0 - (height / 2))
              .attr("y", 0 - margin.left - (margin.left / 2))
              .attr("text-anchor", "middle")
              .attr("transform", "rotate(-90)")
              .attr("dy", "0.5em")
              .style("fill", "#FFFDD0")
              .text(y_axis_name);

            const colorScale = d3.scaleOrdinal()
             .domain(Object.keys(data))
             .range(["#ffb3ba", "#baffc9", "#ffffba", "#bae1ff", "#9467bd", "#8c564b", "#e377c2"]);


            svg.selectAll(".bar")
              .data(Object.entries(data))
              .enter().append("rect")
              .attr("class", "bar") // Added class "bar" to the rect elements for easier selection
              .attr("x", d => xScale(d[0])) // Changed x position to use the xScale with the key of the data object
              .attr("y", d => yScale(d[1])) // Changed y position to use the yScale with the value of the data object
              .attr("width", xScale.bandwidth()) // Changed width to use the bandwidth of the xScale
              .attr("height", d => height - yScale(d[1])) // Changed height to be the difference between the height and the yScale value
              .style("fill", (i) => colorScale(i));
            svg.selectAll(".label")
              .data(Object.entries(data))
              .enter().append("text")
              .attr("class", "label")
              .attr("id", d => `label-${d[0]}`) // Set unique ID for each label
              .attr("x", d => xScale(d[0]) + xScale.bandwidth() / 2)
              .attr("y", d => yScale(d[1]) - 25)
              .attr("text-anchor", "middle")
              .style("fill", "white")
              .text(d => parseFloat(d[1]).toFixed(2))
              .style("visibility", "visible");
            },

        // data = {"group1": {"cluster1": 1}}
        plotClusterHistogram(container, title, data, x_axis_name, y_axis_name, clusters) {
            let maxValue = Number.NEGATIVE_INFINITY;

            for (const key in data) {
                const value = parseFloat(data[key]);
                maxValue = Math.max(maxValue, value);
            }

            maxValue = Math.ceil(maxValue);
            console.log('The maximum value is:', maxValue);

            const margin = { top: 50, right: 50, bottom: 100, left: 50 };
            const width = 480 - margin.left - margin.right;
            const height = 400 - margin.top - margin.bottom;

            let svg = d3.select(container).select('svg');

            // Create a new SVG element if not already present
            svg = d3
              .select(container)
              .append('svg')
              .attr('width', width + margin.right + margin.right + margin.right + margin.right)
              .attr('height', height + margin.bottom + margin.bottom + margin.bottom)
              .append('g')
              .attr('transform', `translate(${2 * margin.left}, ${margin.top})`);

            svg.append("text")
              .attr("x", width / 2)
              .attr("y", 0 - (margin.top / 2))
              .attr("text-anchor", "middle")
              .style("font-size", "16px")
              .style("fill", "white")
              .attr("dy", 10)
              .text(title);

            const xScale = d3
              .scaleBand()
              .range([0, width])
              .padding(0.1)
              .domain(Object.keys(data));
            //d3.max(Object.values(data))
            const yScale = d3
              .scaleLinear()
              .range([height, 0])
              .domain([0, 1.5 * maxValue])
              .nice();

            svg.append("g")
              .attr("transform", `translate(0, ${height})`)
              .call(d3.axisBottom(xScale)
                .tickSizeOuter(0))
              .selectAll("text")
              .style("fill", "white")
              .attr("transform", "rotate(-45)") // Rotate the x-axis labels by -45 degrees
              .style("text-anchor", "end");

            // Add Y Axis
            svg.append("g")
              .call(d3.axisLeft(yScale)
                .tickSizeOuter(0))
              .selectAll("text")
              .style("fill", "white");

            // Adding x and y gridlines
            svg.append("g")
              .attr("class", "grid")
              .attr("transform", "translate(0," + height + ")")
              .call(d3.axisBottom(xScale)
              .tickSize(-height)
              .tickFormat("")
            );

            svg.append("g")
              .attr("class", "grid")
              .call(d3.axisLeft(yScale)
              .tickSize(-width)
              .tickFormat("")
            );

            // Add the x axis label
            svg.append("text")
              .attr("class", "x-axis-label")
              .attr("x", width / 2)
              .attr("y", height + margin.bottom + 40)
              .attr("text-anchor", "middle")
              .style("fill", "#FFFDD0")
              .attr("dy", "0.5em")
              .text(x_axis_name);

            // Add the y axis label
            svg.append("text")
              .attr("class", "y-axis-label")
              .attr("x", 0 - (height / 2))
              .attr("y", 0 - margin.left - (margin.left / 2))
              .attr("text-anchor", "middle")
              .attr("transform", "rotate(-90)")
              .attr("dy", "0.5em")
              .style("fill", "#FFFDD0")
              .text(y_axis_name);

            const colorScale = d3.scaleOrdinal()
             .domain(Object.keys(data))
             .range(["#ffb3ba", "#baffc9", "#ffffba", "#bae1ff", "#9467bd", "#8c564b", "#e377c2"]
                 .flatMap(i => Array.from({ length: clusters.length }).fill(i)));

            svg.selectAll(".bar")
                .data(Object.entries(data))
                .enter().append("rect")
                .attr("class", "bar") // Added class "bar" to the rect elements for easier selection
                .attr("x", d => xScale(d[0])) // Changed x position to use the xScale with the key of the data object
                .attr("y", d => yScale(d[1])) // Changed y position to use the yScale with the value of the data object
                .attr("width", xScale.bandwidth()) // Changed width to use the bandwidth of the xScale
                .attr("height", d => height - yScale(d[1])) // Changed height to be the difference between the height and the yScale value
                .style("fill", (i) => colorScale(i));
            svg.selectAll(".label")
                .data(Object.entries(data))
                .enter().append("text")
                .attr("class", "label")
                .attr("id", d => `label-${d[0]}`) // Set unique ID for each label
                .attr("x", d => xScale(d[0]) + xScale.bandwidth() / 2)
                .attr("y", d => yScale(d[1]) - 25)
                .attr("text-anchor", "middle")
                .style("fill", "white")
                .text(d => parseFloat(d[1]).toFixed(2))
                .style("visibility", "visible");

            var legendContainer = svg.append("g") // This creates a container for the legend
                .attr("class", "legend-container")
                //.attr("transform", "translate(" + (width + margin.right - 40) + "," + (margin.top - 60) + ")")
                .attr("transform", "translate(5,5)")
                .style("overflow-y", "auto") // Enable vertical scrolling
                .attr("height", 100);

            var legend = legendContainer.selectAll(".legend")
                .data(clusters)
                .enter().append("g")
                .attr("class", "legend")
                .attr("transform", function(d, i) { return "translate(0," + i * 15 + ")"; })
                .style("fill", "#FFFDD0");

            legend.append("text")
                .attr("x", 15)
                .attr("y", 5)
                .attr("dy", ".35em")
                .style("text-anchor", "start")
                .style("fill", "#FFFDD0")
                .style("font-size", "10px")
                .text(function(d, i) { return i + ": " + d; });
        },

        drawLineGraph(container_id, title, data, x_axis_name, y_axis_name, max_value) {
            var margin = {top: 50, right: 50, bottom: 50, left: 50},
            width = 480 - margin.left - margin.right,
            height = 400 - margin.top - margin.bottom;

            // Create the SVG container
            var svg = d3.select(container_id)
              .append("svg")
              .style("background-color", "black")
              .attr("width", width + margin.left + margin.right + margin.right + margin.right)
              .attr("height", height + margin.bottom + margin.bottom + margin.bottom)
              .append("g")
              .attr("transform", "translate(" + (2 * margin.left) + "," + margin.top + ")");

            // Set up the x and y scales
            var xScale = d3.scaleTime()
              .range([0, width]);
            var yScale = d3.scaleLinear()
              .range([height, 0]);

            // Set up the x and y axes
            var xAxis = d3.axisBottom(xScale).tickSizeOuter(0);
            var yAxis = d3.axisLeft(yScale).tickSizeOuter(0);

            // Update the x and y domains based on the data
            var allData = [];
            data.forEach(function(lineData) {
              allData = allData.concat(lineData.values);
            });

            console.log(allData);
            
            xScale.domain(d3.extent(allData, function(d) { return new Date(d[0] * 1000); }));
            yScale.domain([0, 2 * (max_value)]);
            //yScale.domain([0, d3.max(allData, function(d) { return d[1]; })]);
            // Add the x and y axes to the SVG container
            svg.append("g")
              .attr("class", "x-axis")
              .attr("transform", "translate(0," + height + ")")
              .style("fill", "#FFFDD0")

              .call(xAxis);
            svg.append("g")
              .attr("class", "y-axis")
              .style("fill", "#FFFDD0")

              .call(yAxis);
        
            //Add the x and y gridlines
            svg.append("g")
              .attr("class", "grid")
              .attr("transform", "translate(0," + height + ")")
              .call(d3.axisBottom(xScale)
                .tickSize(-height)
                .tickFormat("")
              );

            svg.append("g")
              .attr("class", "grid")
              .call(d3.axisLeft(yScale)
                .tickSize(-width)
                .tickFormat("")
              );

              // Add the title to the graph
              svg.append("text")
                .attr("class", "title")
                .attr("x", (width / 2))
                .attr("y", 0 - (margin.top / 2))
                .attr("text-anchor", "middle")
                .style("fill", "#FFFDD0") 
                .text(title);

              // Add the x axis label
              svg.append("text")
                .attr("class", "x-axis-label")
                .attr("x", width / 2)
                .attr("y", height + margin.bottom - 10)
                .attr("text-anchor", "middle")
                .style("fill", "#FFFDD0") 
                .text(x_axis_name);

              // Add the y axis label
              svg.append("text")
                .attr("class", "y-axis-label")
                .attr("x", 0 - (height / 2))
                .attr("y", 0 - margin.left - (margin.left / 2)- 10)
                .attr("text-anchor", "middle")
                .attr("transform", "rotate(-90)")
                .attr("dy", "0.5em")
                .style("fill", "#FFFDD0") 
                .text(y_axis_name);
              svg.selectAll(".x-axis text")      
                .style("fill", "#FFFDD0");
              svg.selectAll(".y-axis text")
                .attr("dx", "0.5em")
                .style("fill", "#FFFDD0");
          
                // Define a color scale for lines
                var contrastColors = ["#FF5733", "#2ECC71", "#3498DB", "#E74C3C", "#1ABC9C", "#F39C12", "#9B59B6", "#27AE60"];

                var colorSelected = []
                // Loop through the data and draw the line for each line in the graph
                data.forEach(function(lineData, i) {
                  var line = d3.line()
                    .x(function(d) { return xScale(new Date(d[0] * 1000)); })
                  .y(function(d) { return yScale(d[1]); });
                //var color_line = "#" + Math.floor(Math.random()*16777215).toString(16)
                var color_line = contrastColors[i];
                colorSelected.push(color_line)
                svg.append("path")
                  .datum(lineData.values)
                  .attr("class", "line")
                  .attr("d", line)
                  .style("stroke", color_line) // Set the stroke color based on index
                  .style("fill", "none");

                  
              });
             

                var legendContainer = svg.append("g") // This creates a container for the legend
    .attr("class", "legend-container")
    .attr("transform", "translate(" + (width + margin.right - 40) + "," + (margin.top - 60) + ")")
    .style("overflow-y", "auto") // Enable vertical scrolling
    .attr("height", height - margin.top - margin.bottom)
    .attr("height", height);

var legend = legendContainer.selectAll(".legend")
    .data(data.slice(0, 21))
    .enter().append("g")
    .attr("class", "legend")
    .attr("transform", function(d, i) { return "translate(0," + i * 15 + ")"; })
    .style("fill", "#FFFDD0");

legend.append("rect")
    .attr("x", 0)
    .attr("width", 5)
    .attr("height", 5)
    .style("fill", function(d, i) { return colorSelected[i]; });

legend.append("text")
    .attr("x", 15)
    .attr("y", 5)
    .attr("dy", ".35em")
    .style("text-anchor", "start")
    .style("fill", "#FFFDD0")
    .style("font-size", "10px")
    .text(function(d) { return (d.metric["host"]).slice(0 , 5); });

        },
    },  
    async mounted() {
        this.refresh();
    }
}
</script>
<style>
.legend{
  padding: 2%;
  max-height: 150px;
  overflow-x: scroll;
  border: 1px solid blue;
}

</style>
