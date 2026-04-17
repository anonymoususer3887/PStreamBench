<h1>PStreamBench: Benchmark Data Extraction & Plotting Scripts</h1>

<p>
This repository contains a small pipeline to 
(1) extract benchmark data from PStreamBench artifacts,
(2) optionally generate operator-level metrics, and 
(3) generate plots for query-level and operator-level analysis.
</p>

<hr/>

<h2>Contents</h2>
<ul>
  <li><code>1_data_extractor.py</code> — extracts query-level metrics into a CSV</li>
  <li><code>2_operator_build_data.py</code> — extracts operator-level metrics and writes per-query CSVs</li>
  <li><code>3_plot_pdsp_query_metrics.py</code> — main plotting utility (many plot modes)</li>
  <li><code>4_plot_pdsp_low_high_parallelism.py</code> — low/high parallelism comparison plotting</li>
  <li><code>5_plot_pdsp_operator_metrics.py</code> — operator-metrics plotting (reads <code>operator_metrics_wide/</code>)</li>
  <li><code>6_make_all_plots.sh</code> — batch plot runner</li>
</ul>

<hr/>

<h2>Prerequisites</h2>
<ul>
  <li>Python 3.9+ (recommended: 3.10+)</li>
  <li><code>venv</code> (Python virtual environments)</li>
  <li>Linux/macOS shell (for the batch script). Windows users can run commands manually.</li>
</ul>

<p><b>Optional</b></p>
<ul>
  <li>LaTeX installation (only if you enable LaTeX text rendering in Matplotlib)</li>
</ul>

<hr/>

<h2>Setup (virtual environment)</h2>

<pre><code class="language-bash"># 1) Create venv
python3 -m venv .venv

# 2) Activate venv
source .venv/bin/activate

# 3) Upgrade pip
python -m pip install --upgrade pip

# 4) Install dependencies
pip install numpy pandas matplotlib scienceplots 
or 
pip install requirements.txt
</code></pre>

<p>
If you want reproducible installs, create a <code>requirements.txt</code> with the same packages and run
<code>pip install -r requirements.txt</code>.
</p>

<hr/>

<h2>Input data layout</h2>

<p>
Both extraction scripts expect an <code>artifacts/</code> directory containing PStreamBench JSON files such as:
</p>

<ul>
  <li><code>artifacts/metaInfo-*.json</code></li>
  <li><code>artifacts/evaluationMetrics-*.json</code></li>
</ul>

<pre><code class="language-text">your-repo/
  artifacts/
    metaInfo-...json
    evaluationMetrics-...json
  1_data_extractor.py
  2_operator_build_data.py
  3_plot_pdsp_query_metrics.py
  ...
</code></pre>

<hr/>

<h2>Step-by-step pipeline</h2>

<h3>Step 1 — Extract job-level metrics (CSV)</h3>
<p>
This generates a consolidated CSV (default output: <code>data_with_query_type_throughput.csv</code>).
</p>

<pre><code class="language-bash">python 1_data_extractor.py
</code></pre>

<p>
After this step, you should have:
</p>
<ul>
  <li><code>query_performance_data.csv</code></li>
</ul>

<hr/>

<h3>Step 2 — (Optional) Build operator-level datasets</h3>
<p>
This creates per-query operator metric files under <code>operator_metrics_wide/</code>, and also writes a cleaned CSV.
</p>

<pre><code class="language-bash">python 2_operator_build_data.py
</code></pre>

<p>Expected outputs include:</p>
<ul>
  <li><code>operator_metrics_wide/operator_metrics_&lt;query&gt;.csv</code></li>
  <li><code>data/operator_build_data_cleaned.csv</code></li>
</ul>

<hr/>

<h3>Step 3 — Generate plots (main plotting script)</h3>

<p>
The main plotting tool is:
</p>
<ul>
  <li><code>3_plot_pdsp_query_metrics.py</code></li>
</ul>

<p><b>Tip:</b> If you prefer a stable name, you can copy it once:</p>
<pre><code class="language-bash">cp 3_plot_pdsp_query_metrics.py plot_pdsp.py
</code></pre>

<p>
Below are example commands using the original filename. Replace placeholders like <code>QUERY_NAME</code>.
</p>

<h4>Example A — Compare Flink vs Storm scaling over DoP</h4>
<pre><code class="language-bash">python 3_plot_pdsp_query_metrics.py \
  --csv data_with_query_type_throughput.csv \
  --plot scaling_compare_fw \
  --query "QUERY_NAME" \
  --hardware_type c6525-25g \
  --producer_rate 1000000 \
  --metric throughput_50 \
  --save plots/scaling_compare_fw_QUERY_NAME
</code></pre>

<h4>Example B — Dual-axis plot (throughput bars + latency lines)</h4>
<pre><code class="language-bash">python 3_plot_pdsp_query_metrics.py \
  --csv data_with_query_type_throughput.csv \
  --plot dual_axis_tp_latency \
  --query "QUERY_NAME" \
  --hardware_type c6525-25g \
  --producer_rate 1000000 \
  --x-dim parallelism_avg \
  --tp-metric throughput_50 \
  --lat-metric endtoend_latency \
  --save plots/dual_axis_QUERY_NAME
</code></pre>

<h4>Example C — Low/Medium/High grouping plots</h4>
<p>
These plots require a mapping JSON for each hardware tier. Example:
</p>

<pre><code class="language-bash">python 3_plot_pdsp_query_metrics.py \
  --csv data_with_query_type_throughput.csv \
  --plot low_med_high_parallelism_across_hw \
  --query "QUERY_NAME" \
  --metric throughput_50 \
  --producer_rate 1000000 \
  --hardware-types m510 c6525-25g c6525-100g \
  --frameworks flink storm \
  --par-map-json '{"m510":[2,16,40],"c6525-25g":[2,16,40],"c6525-100g":[2,16,40]}' \
  --save plots/lmh_parallelism_QUERY_NAME
</code></pre>

<hr/>

<h3>Step 4 — Operator metrics plots</h3>
<p>
If you ran <b>Step 2</b>, you can plot operator metrics from <code>operator_metrics_wide/</code>:
</p>

<pre><code class="language-bash">python 5_plot_pdsp_operator_metrics.py \
  --input_dir operator_metrics_wide \
  --output_dir plots/operator_metrics \
  --plot operator_bars \
  --operator "Sink (Op)" \
  --metric avg_cpu_pct
</code></pre>

<p>
Use <code>--plot list_operators</code> or <code>--plot list_metrics</code> (if supported by your script version) to discover valid names.
</p>

<hr/>

<h2>Batch plotting (one command)</h2>

<p>
To generate a full set of plots using the provided shell script:
</p>

<pre><code class="language-bash">bash make_all_plots.sh
</code></pre>

<p>
The script contains editable variables near the top (CSV path, output directory, plot script path).
Open it and adjust as needed.
</p>

<hr/>

<details>
  <summary><b>Troubleshooting</b></summary>

  <h3>“No data left after filtering”</h3>
  <ul>
    <li>Check that <code>--csv</code> points to the correct file.</li>
    <li>Verify that your <code>--query</code>, <code>--hardware_type</code>, and <code>--producer_rate</code> values exist in the CSV.</li>
  </ul>

  <h3>Plots look empty</h3>
  <ul>
    <li>Try removing filters (e.g., omit <code>--hardware_type</code> or <code>--producer_rate</code>) to confirm data exists.</li>
    <li>Check that the CSV columns match expected metric names (e.g., <code>throughput_50</code>, <code>endtoend_latency</code>).</li>
  </ul>
</details>
