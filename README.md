# Healthcare ETL and Dashboard Project

This project is a complete ETL (Extract, Transform, Load) pipeline and data visualization dashboard for analyzing U.S. hospital readmission data. The entire application is containerized using Docker and orchestrated with Docker Compose, making it easy to set up and run.

## 📜 Table of Contents
* [Project Overview](#-project-overview)
* [Features](#-features)
* [Technology Stack](#-technology-stack)
* [Project Structure](#-project-structure)
* [Setup and Installation](#-setup-and-installation)
* [How to Use](#-how-to-use)
* [ETL Process Details](#-etl-process-details)
* [Dashboard Details](#-dashboard-details)
* [Configuration](#-configuration)

---

## 🧐 Project Overview

The goal of this project is to process raw hospital data, load it into a structured database, and provide an interactive web dashboard for analysis. Specifically, it analyzes the "Hospital Readmission Reduction Program" (HRRP) data, focusing on heart failure (`READM-30-HF-HRRP`) readmission rates.

The pipeline automates the following steps:
1.  **Extract**: Reads hospital information and readmission data from local CSV files.
2.  **Transform**: Cleans, merges, and filters the data to create a final, analysis-ready dataset.
3.  **Load**: Ingests the transformed data into a PostgreSQL database.
4.  **Visualize**: A Streamlit dashboard queries the database and presents the data through interactive charts, maps, and tables.

---

## ✨ Features

* **Automated ETL Pipeline**: The data processing pipeline runs automatically when the services are started.
* **Containerized Services**: All components (ETL script, database, dashboard) are isolated in Docker containers for consistency and portability.
* **Interactive Dashboard**: A user-friendly web interface to explore the data without needing to write any code.
* **Scalable Architecture**: The use of Docker and a relational database allows for easy scaling and modification.

---

## 💻 Technology Stack

* **Backend & ETL**: Python 3.9
* **Data Manipulation**: Pandas
* **Database**: PostgreSQL
* **Dashboard**: Streamlit
* **Data Visualization**: Plotly
* **Containerization**: Docker & Docker Compose

---

## 📂 Project Structure

The project is organized into distinct directories for each component:

```
healthcare_etl/
├── data/
│   ├── readmissions.csv      # Raw readmission data
│   └── hospital_info.csv       # Raw hospital metadata
├── dashboard_app/
│   ├── app.py                # Streamlit dashboard script
│   ├── Dockerfile            # Docker instructions for the dashboard
│   └── requirements.txt      # Python dependencies for the dashboard
├── etl_app/
│   ├── etl_script.py         # The main ETL processing script
│   ├── Dockerfile            # Docker instructions for the ETL app
│   └── requirements.txt      # Python dependencies for the ETL script
└── docker-compose.yml        # Orchestrates all the services
```

---

## 🚀 Setup and Installation

### Prerequisites

* [Docker](https://docs.docker.com/get-docker/)
* [Docker Compose](https://docs.docker.com/compose/install/)

### Running the Application

1.  **Clone the repository** (or ensure all the files are in the structure shown above).

2.  **Navigate to the project root directory**:
    ```bash
    cd healthcare_etl
    ```

3.  **Build and run the services using Docker Compose**:
    ```bash
    docker-compose up --build
    ```
    * This command will build the Docker images for the `etl_service` and `dashboard_service`, and then start all three services (`postgres_db`, `etl_service`, `dashboard_service`).
    * The ETL script will run once the database is ready. You will see logs in your terminal as it extracts, transforms, and loads the data.

---

## 🌐 How to Use

Once the containers are running, you can access the interactive dashboard.

1.  Open your web browser.
2.  Navigate to **[http://localhost:8501](http://localhost:8501)**.

The dashboard will load and display the visualizations based on the data processed by the ETL pipeline.

To stop all the running containers, press `Ctrl + C` in the terminal where `docker-compose` is running, and then run:
```bash
docker-compose down
```

---

## ⚙️ ETL Process Details

The `etl_app/etl_script.py` performs the following transformations:
1.  **Reads Data**: Loads `readmissions.csv` and `hospital_info.csv` into Pandas DataFrames.
2.  **Cleans Column Names**: Standardizes column names by converting them to lowercase and replacing spaces with underscores.
3.  **Filters Data**: Isolates the records relevant to heart failure readmissions by filtering for `Measure Name == 'READM-30-HF-HRRP'`.
4.  **Selects Columns**: Subsets the `hospital_info` data to include only essential columns like facility ID, name, location, and ownership type.
5.  **Merges Datasets**: Joins the readmission and hospital info datasets on the `facility_id`.
6.  **Loads to Database**: Connects to the PostgreSQL database and loads the final, clean DataFrame into a table named `heart_failure_readmissions`.

---

## 📊 Dashboard Details

The `dashboard_app/app.py` creates a multi-faceted view of the data:
* **Key Metrics**: Displays high-level statistics, including the total number of hospitals analyzed and the average readmission ratio.
* **Geographic Map**: A choropleth map of the USA showing the average readmission ratio by state. 
* **Readmission by Ownership**: A bar chart comparing the average readmission ratio across different types of hospital ownership (e.g., government, proprietary, non-profit).
* **Interactive Data Table**: A sortable and searchable table that allows users to find the hospitals with the highest or lowest readmission ratios.

---

## 🔧 Configuration

All service configurations are managed within the `docker-compose.yml` file.
* **Database Credentials**: The PostgreSQL username, password, and database name are set as environment variables for the `postgres_db`, `etl_service`, and `dashboard_service`.
* **Ports**:
    * The database is accessible on port `5432`.
    * The Streamlit dashboard is accessible on port `8501`.
* **Volumes**:
    * A named volume `postgres_volume` is used to persist the database data.
    * The `./data` directory is mounted into the `etl_service` container so the script can access the CSV files.