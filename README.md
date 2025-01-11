# Taxi Trip Data Batch Processing and Analysis

## Overview

This project analyzes Yellow and Green Taxi trip datasets from March 2021 to extract meaningful insights. Using Apache Spark, we perform data cleaning, enrichment, and analysis to uncover trends and patterns in trip data.

---

# Repository Structure

**HW_2\_BatchProcessing/**

**├── scripts/ \# Python scripts for data processing and analysis**

**│ └── HW_2\_BatchProcessing.py \# Main Python script for data analysis
functions**

**├── data/ \# Folder for input datasets**

**│ ├── yellow_tripdata_2021-03.parquet \# Yellow Taxi trip data**

**│ ├── green_tripdata_2021-03.parquet \# Green Taxi trip data**

**│ ├── taxi+\_zone_lookup.csv \# Lookup table for location zones**

**├── outputs.zip/ \# Generated visualizations and analysis results**

**│ ├── yellow_taxi_hourly_counts.png \# Hourly pickup/drop-off graph
(Yellow Taxi)**

**│ ├── green_taxi_hourly_counts.png \# Hourly pickup/drop-off graph
(Green Taxi)**

**│ ├── yellow_taxi_tip_correlations.png \# Tip correlation graph
(Yellow Taxi)**

**│ ├── green_taxi_tip_correlations.png \# Tip correlation graph (Green
Taxi)**

**├── README.md \# Documentation for the project**

**└── .gitignore \# Files and folders to exclude from version control**



## Features

- **Data Cleaning**: Remove invalid or extreme values to ensure data quality.
- **Data Enrichment**: Join taxi datasets with a lookup table to add location details (zones and boroughs).
- **Analysis**:
  - Identify the most expensive routes.
  - Determine the top 5 busiest pickup zones.
  - Find the longest trips based on distance.
  - Analyze crowded pickup and drop-off zones by hour.
  - Calculate correlations between tips and other trip attributes.
- **Visualizations**:
  - Hourly pickup and drop-off counts.
  - Correlations between tip amounts and numerical columns.

---

## Getting Started

### Prerequisites

Ensure the following are installed:

- Python 3.8+
- Apache Spark
- Pandas
- Matplotlib

### Installation

- Clone the repository:
   ```bash
   git clone https://github.com/yourusername/HW_2_BatchProcessing.git
   cd HW_2_BatchProcessing


- Install Python dependencies:

  - pip install -r requirements.txt
  - Place the input datasets (yellow_tripdata_2021-03.parquet, green_tripdata_2021-03.parquet, and taxi+_zone_lookup.csv) in the data/ folder.

- Place the input datasets (yellow_tripdata_2021-03.parquet, green_tripdata_2021-03.parquet, and taxi+_zone_lookup.csv) in the data/ folder.

# Usage
- Run the main script:
  - python scripts/HW_2_BatchProcessing.py
- Outputs will be saved in the outputs/ directory.

# Visualizations

- Hourly Pickup and Drop-off Counts

- Tip Correlations

# Technologies Used

- [Apache Spark] -Scalable data processing framework.
- [Python] - Programming language for analysis and visualization.
- [Matplotlib] - Visualization library for generating plots.

  

# License
This project is licensed under the MIT License. See the LICENSE file for more details.


# Contact
- For questions, feedback, or collaboration opportunities, please reach out: Muhammed Ali Sezer
- **Email**: muhammedalisezer44@gmail.com
