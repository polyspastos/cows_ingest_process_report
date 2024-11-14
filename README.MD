A flaskapi implementation of a parquet ingestion - data processing - reporting pipeline

## Setup

python 3.12

1. Clone the repository:
   ```
   git clone git@github.com:polyspastos/cows_ingest_process_report.git
   cd nn_ha
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   ```

3. Activate the virtual environment:
   - On Windows:
     ```
     venv\Scripts\activate
     ```
   - On macOS and Linux:
     ```
     source venv/bin/activate
     ```

4. Install dependencies:
   ```
   pip install --upgrade pip setuptools wheel
   pip install -r requirements.txt
   ```

## Usage

1. Start the API server:
   ```
   uvicorn app.main:app --reload
   ```

2. Ingest data from .parquet files:
   ```
   python -m app.ingestion
   ```

3. Generate a report:
   ```
   python -m app.reporting
   ```

   or use the api endpoint
   ```
   /cows/report
   ```

4. Access the API documentation:
   Open a web browser and go to `http://localhost:8000/docs` to view the Swagger UI for API documentation.

## Running Tests

To run the tests, use the following command:

```
pytest
```
