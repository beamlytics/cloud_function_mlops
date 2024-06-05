from google.cloud import bigquery

def train_model(request):
    client = bigquery.Client()
    query = """
    CREATE OR REPLACE MODEL `Retail_Store.your_model`
    OPTIONS(model_type='linear_reg', input_label_cols=['label']) AS
    SELECT
      feature1,
      feature2,
      label
    FROM
      `Retail_Store.clean_inventory_data`;
    """
    query_job = client.query(query)
    query_job.result()  

    return ('Model trained successfully.', 200)
