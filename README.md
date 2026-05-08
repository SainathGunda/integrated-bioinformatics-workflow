# Integrated Bioinformatics Workflow for Evolutionary Analysis

Dockerized Apache Airflow workflows for automating evolutionary bioinformatics analyses, including **PSMC**, **ABBA-BABA**, and **Species Distribution Modeling**.

This project converts multiple standalone bioinformatics workflows into automated, reproducible, and configurable pipelines. Each pipeline is managed through **Apache Airflow**, packaged inside a **Docker** environment, and controlled using simple YAML configuration files.

---

## Project Summary

Evolutionary bioinformatics analyses often require many manual steps, multiple software tools, and careful file handling. This project solves that problem by creating an automated workflow system that can run complex pipelines in a consistent and organized way.

The project includes three workflows:

1. **PSMC Pipeline**  
   Estimates historical population size changes from genomic data.

2. **ABBA-BABA Pipeline**  
   Detects introgression and gene flow between species using Dsuite.

3. **Species Distribution Modeling Pipeline**  
   Predicts habitat suitability using GBIF occurrence records, IUCN range maps, WorldClim climate variables, and MaxEnt modeling.

---

## Why This Project Matters

Running bioinformatics tools manually can be difficult because each tool has different inputs, outputs, dependencies, and command-line steps. Small mistakes in file paths, parameters, or tool versions can affect reproducibility.

This project improves the workflow by:

- Automating each analysis using Apache Airflow DAGs
- Keeping pipeline settings in YAML configuration files
- Running all tools inside a Dockerized environment
- Organizing outputs into clear folders
- Sending email notifications when pipelines finish
- Making the workflow easier to reproduce and share

---

## Repository Structure

```text
integrated-bioinformatics-workflow/
│
├── config/
│   ├── psmc_config.yaml
│   ├── abba_baba_config.yaml
│   └── sdm_config.yaml
│
├── dags/
│   ├── psmc.py
│   ├── abba_baba.py
│   └── sdm.py
│
├── inputs/
│   ├── psmc/
│   └── sdm/
│   └── abba-baba/
│
├── outputs/
│   ├── psmc/
│   ├── abba-baba/
│   └── sdm/
│
├── Dockerfile
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Technologies Used

### Workflow and Automation

- Apache Airflow
- Docker
- YAML configuration files

### Genomic Analysis Tools

- samtools
- bcftools
- PSMC
- Dsuite

### Species Distribution Modeling libraries in Python

- geopandas
- shapely
- rioxarray
- rasterio
- pygbif
- elapid


---

## Docker Image

A prebuilt Docker image is available on Docker Hub.

```bash
docker pull sainathgunda99/evolutionary-analysis:1
```

Docker Hub repository:

```text
https://hub.docker.com/repository/docker/sainathgunda99/evolutionary-analysis
```

---

## How to Run the Project

### Option 1: Run Using Docker Hub Image

Pull the image:

```bash
docker pull sainathgunda99/evolutionary-analysis:1
```

Run the container:

```bash
docker run -it -p 8080:8080 -v D:\project:/app sainathgunda99/evolutionary-analysis:1
```

Then open Airflow in your browser:

```text
http://localhost:8080
```

---

### Option 2: Build the Docker Image Locally

Clone the repository:

```bash
git clone https://github.com/SainathGunda/integrated-bioinformatics-workflow.git
cd integrated-bioinformatics-workflow
```

Build the Docker image:

```bash
docker build -t integrated-bioinformatics-workflow .
```

Run the container:

```bash
docker run -it -p 8080:8080 -v D:\project:/app integrated-bioinformatics-workflow
```

Open Airflow:

```text
http://localhost:8080
```

---

## Pipeline 1: PSMC Analysis

### Purpose

The **PSMC** pipeline estimates changes in effective population size over time using whole-genome sequence data.

This is useful for understanding demographic history, such as population expansion, decline, or bottlenecks.

## Pipeline 2: ABBA-BABA Analysis

### Purpose

The **ABBA-BABA** pipeline is used to detect introgression or gene flow between species.

This workflow uses **Dsuite Dtrios**.

## Pipeline 3: Species Distribution Modeling

### Purpose

The **Species Distribution Modeling** pipeline predicts habitat suitability for a selected species.

This pipeline combines:

- GBIF occurrence records
- IUCN species range polygons
- WorldClim bioclimatic variables
- Background point generation
- MaxEnt modeling
- Habitat suitability map generation

## Input Data Requirements

Large input files are not included in this repository. Before running the workflows, place the required files in the correct input folders.


## Configuration Files

Each pipeline is controlled by a YAML file in the `config/` folder.

```text
config/
├── psmc_config.yaml
├── abba_baba_config.yaml
└── sdm_config.yaml
```

### Why YAML Files Are Used

YAML files make the workflow easier to modify without changing the Python DAG code.

You can update:

- Species ID
- Species name
- Input file paths
- Output directory
- PSMC parameters
- Tree topology
- Number of GBIF records
- Number of background points
- Selected bioclimatic variables
- Notification email

---

## Airflow DAGs

The project contains three Airflow DAGs:

| DAG File | DAG ID | Purpose |
|---|---|---|
| `dags/psmc.py` | `psmc_pipeline` | Runs demographic history analysis using PSMC. |
| `dags/abba_baba.py` | `abba_baba_pipeline` | Runs introgression analysis using Dsuite. |
| `dags/sdm.py` | `sdm_pipeline` | Runs species distribution modeling using MaxEnt. |

Each DAG is manually triggered from the Airflow UI.

---

## Running a Pipeline in Airflow

1. Start the Docker container.
2. Open Airflow at:

```text
http://localhost:8080
```

3. Log in using the credentials shown by Airflow.
4. Find the DAG you want to run:
   - `psmc_pipeline`
   - `abba_baba_pipeline`
   - `sdm_pipeline`
5. Turn on the DAG.
6. Click **Trigger DAG**.
7. Monitor task progress in the Airflow UI.
8. Check the output folder after the DAG finishes.

---

## Email Notifications

Each pipeline includes an email notification task at the end.

The DAGs use the Airflow SMTP connection:

```text
smtp_default
```

To enable email notifications, configure SMTP settings in Airflow and update the `notify_to` value in each YAML config file.

Example:

```yaml
notify_to: "your_email@example.com"
```

---

## Notes

- The workflows are designed to run inside Docker.
- Large genomic, raster, and shapefile datasets should be stored in the input directories before running the DAGs.
- File paths in the YAML files should match the paths inside the Docker container.
- The pipelines are manually triggered from the Airflow UI.
- Output files are saved in the `outputs/` directory.
- Email notifications require SMTP configuration in Airflow.

---


## Author

**Sainath Gunda**  
Graduate Student  
University of Arizona

---

## Mentor

**Dr. Kunal Arekar**

---

## Project Purpose

This project was developed as part of a capstone project to automate evolutionary bioinformatics workflows. The main goal is to improve reproducibility, reduce manual execution errors, and make complex analyses easier to run through a Dockerized Apache Airflow environment.

---

