
## Setup

python 3.12

1. Clone the repository:
   ```
   git clone https://github.com/your-username/cowshed-automation.git
   cd cowshed-automation
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

4. Access the API documentation:
   Open a web browser and go to `http://localhost:8000/docs` to view the Swagger UI for API documentation.

## Running Tests

To run the tests, use the following command:

```
pytest
```
