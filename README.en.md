[🇷🇺 Русский](./README.md) | [🇬🇧 English](./README.en.md)

# 📘 NASA EPIC Data Pipeline

This project is a comprehensive data pipeline for processing data from the NASA EPIC API (DSCOVR satellite). The pipeline is built following the principles of the Data Engineering Lifecycle, as described in the book "Fundamentals of Data Engineering" by Joe Reis and Matt Housley.

## 📦 Tech Stack

- **Apache Airflow 3.0:** Orchestration of workflows.
- **PostgreSQL:** Storage for raw data.
- **MongoDB:** Storage for aggregated data (daily coordinate averages).
- **FastAPI:** REST API to access the data.
- **Docker / Docker Compose:** Containerization of the application.
- **Selectel S3:** Cloud storage for exported CSV data.
- **Pandas:** Data transformation and processing.
- **PySpark:** Data Lake processing for CSV data from S3.

## 🔧 How the Pipeline Works

### DAG: `nasa_data_pipeline`

- Runs daily in the evening.
- Fetches the previous day's data from the NASA API.
- Stores full data in PostgreSQL.
- Calculates average coordinates and stores them in MongoDB.

### DAG: `yearly_export_dag`

- Runs at the end of the year (December 31).
- Extracts yearly data from PostgreSQL.
- Saves it to a CSV file and uploads it to Selectel S3.
- Processes the CSV file using PySpark.

## 🌐 API

The FastAPI application provides access to aggregated data:

- `GET /` — API health check.
- `GET /records` — Fetch all aggregated records.
- `GET /records/{id}` — Fetch a record by ObjectId.

Swagger documentation: [http://localhost:8000/docs](http://localhost:8000/docs)

## 🐳 Quick Start

1. **Clone the repository:**

```bash
git clone https://github.com/georgiymironenko/data-nasa-pipeline.git
cd data-nasa-pipeline
```

2. **Configure the environment:**

- Create a `.env` file based on `.env.example`.
- Set your NASA API key:

```env
NASA_API_KEY=your_nasa_api_key_here
```

3. **Launch the project:**

```bash
docker-compose up --build
```

4. **Configure Airflow Connections:**

Go to the Airflow UI: [http://localhost:8080](http://localhost:8080)

- Navigate to `Admin > Connections > Add Connection`

Create a MongoDB connection:
- Conn ID: `mongo_default`
- Conn Type: `Mongo`
- Host: `mongodb`
- Login: `airflow`
- Password: `root`
- Port: `27017`

Create a PostgreSQL connection:
- Conn ID: `data_nasa_base`
- Conn Type: `Postgres`
- Host: `postgres`
- Login: `airflow`
- Password: `airflow`
- Port: `5432`
- Schema: `postgres`

5. **Access Services:**

| Service        | URL                          | Login / Password     |
|----------------|-------------------------------|----------------------|
| Airflow        | http://localhost:8080         | airflow / airflow    |
| FastAPI        | http://localhost:8000/docs    | —                    |
| Mongo Express  | http://localhost:8081         | admin / root         |

## 🗂 S3 + Airflow Integration

1. **Set Airflow Variables:**

- `data_nasa_api` — NASA API key.
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — Selectel S3 credentials.

2. **Ensure `boto3` is installed:**

If needed, add `boto3` to Airflow's `requirements.txt` and rebuild the Docker image.

## 📘 Data Catalog

Data comes from the NASA EPIC API:

```
https://api.nasa.gov/EPIC/api/natural/date/2019-05-30?api_key=DEMO_KEY
```

Sample data:

```json
"date": "2025-05-23 01:27:51",
"dscovr_j2000_position": {
  "x": 934134.275255,
  "y": 1072103.780608,
  "z": 512460.978068
},
```

Each record includes a link to an Earth photo taken by the satellite:

```
https://api.nasa.gov/EPIC/archive/natural/2019/05/30/png/epic_1b_20190530011359.png?api_key=DEMO_KEY
```

## 🧠 Author

**Georgiy Mironenko**, 18 years old  
Project inspired by *"Fundamentals of Data Engineering"*

## 📝 License

This project is licensed under the **MIT License**.
