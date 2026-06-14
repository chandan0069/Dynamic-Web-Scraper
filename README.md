# Dynamic Web Scraper

scrapes 5 websites concurrently - Hacker News, Reddit, Quotes to Scrape, Books to Scrape, Wikipedia Recent Changes.
## Follow these easy steps to run it on your machine:


Step 1: Create a Virtual Environment

Step 2: Install the packages

```bash
pip install -r requirements.txt
playwright install
```


## How to Run the Project!
1. Seed the database 
```bash
python src/main.py seed
```
2. Run the FastAPI Server 
```bash
python src/main.py serve
```

3. Run the Scraper pipeline for 30 minutes
```bash
python src/main.py run --duration 1800 --output output/pipeline_run.json
```

4. Trigger mid run failure
```bash
curl -X PATCH http://localhost:8000/api/sources/quotes_to_scrape \
  -H "Content-Type: application/json" \
  -d '{"enabled": false}'
```

## API

```
GET    /sources
POST   /sources
GET    /sources/{id}
PUT    /sources/{id}
DELETE /sources/{id}
POST   /sources/{id}/dry-run
GET    /health
```