<div align="center">
  
  <h1>End-to-End NYC Taxi Data Lakehouse 🚕</h1>
  
  <p>
    An end-to-end data engineering pipeline built with PySpark, AWS S3, and Delta Lake, following a modern Bronze-Silver-Gold "Medallion" architecture.
  </p>

  <p>
    <img src="https://img.shields.io/badge/Python-3.10+-blue.svg?logo=python&logoColor=white" alt="Python 3.10+">
    <img src="https://img.shields.io/badge/Apache_Spark-4.0.1-E25A1C.svg?logo=apachespark&logoColor=white" alt="Spark 4.0.1">
    <img src="https://img.shields.io/badge/AWS_S3-Storage-569A31.svg?logo=amazons3&logoColor=white" alt="AWS S3">
    <img src="https://img.shields.io/badge/Delta_Lake-Warehouse-0063B5.svg?logo=databricks&logoColor=white" alt="Delta Lake">
  </p>

</div>

---

<p>This project demonstrates a professional, cloud-based workflow. The pipeline ingests raw NYC Taxi data from AWS S3, processes it using a <strong>local PySpark</strong> job, and builds a multi-layered <strong>Data Lakehouse</strong> in <strong>AWS S3</strong> using the <strong>Delta Lake</strong> format.</p>

<p>The core of this project is a robust, modular PySpark application that implements the <strong>Bronze-Silver-Gold Medallion Architecture</strong>. This separates the pipeline into distinct, decoupled stages.</p>

<h2>🏛️ Project Architecture: The Medallion Model</h2>

<p>This pipeline is built in three distinct layers, showing a clear separation of concerns.</p>

<ul>
  <li>
    <strong>🥉 Bronze Layer (AWS S3):</strong>
    <ul>
      <li><strong>What:</strong> The raw, untouched Parquet and CSV files.</li>
      <li><strong>Why:</strong> Provides a permanent, immutable archive of the source data for future re-processing.</li>
    </ul>
  </li>
  <li>
    <strong>🥈 Silver Layer (AWS S3 + Delta Lake):</strong>
    <ul>
      <li><strong>What:</strong> A <strong>single, clean, 1-to-1 table</strong> (<code>silver_trips</code>).</li>
      <li><strong>Why:</strong> This table is the "single source of truth" for all cleaned data. It has fixed data types, corrected values (no negative fees), and imputed nulls. All downstream "Gold" products are built from this layer.</li>
    </ul>
  </li>
  <li>
    <strong>🥇 Gold Layer (AWS S3 + Delta Lake):</strong>
    <ul>
      <li><strong>What:</strong> The final "business-ready" data products. This layer contains <strong>two separate pipelines</strong> that run from the Silver Layer:</li>
      <li><strong>1. The Data Warehouse:</strong> A fully normalized <strong>Star Schema</strong> (<code>fact_trips</code>, <code>dim_location</code>, etc.) built for BI tools and fast analytical SQL queries.</li>
      <li><strong>2. The ML Feature Table:</strong> A wide, "denormalized" table (<code>ml_features</code>) containing complex, pre-calculated features (e.g., <code>speed_vs_avg_ratio</code>, <code>zone_pagerank</code>) for machine learning models.</li>
    </ul>
  </li>
</ul>

<h2>🚀 Key Skills & Technologies</h2>

<ul>
  <li><strong>Data Processing:</strong> Apache Spark (PySpark 4.0.1)</li>
  <li><strong>Cloud Storage:</strong> AWS S3</li>
  <li><strong>Warehouse Format:</strong> Delta Lake</li>
  <li><strong>Language:</strong> Python 3.11</li>
  <li><strong>Data Modeling:</strong> Star Schema & Deterministic Hash Keys</li>
  <li><strong>Feature Engineering:</strong> Advanced transformations (Window Functions, Aggregate-and-Join).</li>
  <li><strong>Dependency Management:</strong> Spark <code>--packages</code> for handling S3 & Delta dependencies.</li>
  <li><strong>Configuration:</strong> Securely managed with <code>.env</code> files and <code>python-dotenv</code>.</li>
</ul>

<h2>🛠️ Core Engineering Features</h2>

<p>This project demonstrates a practical, end-to-end understanding of a modern data stack.</p>

<ul>
  <li><strong>Data Cleaning:</strong> Robust functions to handle real-world messy data (negative fees, null values, 0-distance trips) in the Silver pipeline.</li>
  <li><strong>Timezone Standardization:</strong> All timestamps are converted from <code>America/New_York</code> to <code>UTC</code> in the Silver Layer to prevent analytics errors.</li>
  <li><strong>Idempotency (Reliability):</strong> The pipeline is fully re-runnable. <strong>Deterministic hash keys</strong> (<code>md5</code>, <code>concat_ws</code>) are generated for all primary keys in the Gold Layer warehouse.</li>
  <li><strong>Performance Optimization:</strong>
    <ul>
      <li><strong><code>partitionBy()</code>:</strong> The Silver and Gold tables are partitioned by <code>pickup_date</code>, enabling <strong>partition pruning</strong> for fast queries.</li>
      <li><strong><code>broadcast()</code>:</strong> Broadcast joins are used in the Gold Layer to join facts to dimensions, preventing costly shuffles.</li>
    </ul>
  </li>
</ul>

<h2>🏃‍♂️ How to Run This Project</h2>

<p>This project runs on your local machine and connects to your AWS S3 bucket.</p>

<ol>
  <li>
    <strong>Clone the Repository</strong>
    <pre><code>git clone https://github.com/mladbago/NYC-Data-Engineering-Pipeline.git</code></pre>
  </li>
  <li>
    <strong>Install Dependencies</strong>
    <ul>
      <li>Install Apache Spark 4.0.1+ and Java 17+.</li>
      <li>Create a Python 3.11 virtual environment and activate it.</li>
      <li>Install all Python packages:</li>
    </ul>
    <pre><code>pip install -r requirements.txt</code></pre>
  </li>
  <li>
    <strong>Configure Environment</strong>
    <ul>
      <li>Create a <code>.env</code> file in the project root.</li>
      <li>Add your AWS S3 credentials and bucket name:</li>
    </ul>
    <pre><code>AWS_ACCESS_KEY_ID="YOUR_KEY"
AWS_SECRET_ACCESS_KEY="YOUR_SECRET"
S3_BUCKET_NAME="your-s3-bucket-name"</code></pre>
  </li>
  <li>
    <strong>Upload Bronze Data</strong>
    <ul>
      <li>Go to your S3 bucket and create a <code>bronze/</code> "folder."</li>
      <li>Upload your raw <code>yellow_tripdata_...parquet</code> files and the <code>taxi_zone_lookup.csv</code> into this <code>bronze/</code> folder.</li>
    </ul>
  </li>
  <li>
    <strong>Run the ETL Pipeline</strong>
    <ul>
      <li>This pipeline <strong>must</strong> be run using <code>spark-submit</code> to correctly load all required Java package dependencies.</li>
      <li>From your project's root directory, run:</li>
    </ul>
    <pre><code>spark-submit \
--master local[*] \
--packages "io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.367,software.amazon.awssdk:s3:2.20.18" \
src/main.py</code></pre>
  </li>
</ol>