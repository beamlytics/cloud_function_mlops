import os
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import functions
from google.cloud.exceptions import NotFound



bq_client = bigquery.Client()

storage_client = storage.Client()

functions_client = functions.CloudFunctionsServiceClient()

def new_count(table_id,count_id,stream_data):

    table = bq_client.get_table(table_id)
    
    query_1= f"""
    SELECT row_count
    FROM `{count_id}`
    WHERE table_name = '{stream_data}'
    """
    query_job = bq_client.query(query_1)
    # Get the result
    result = query_job.result()
    row_count = None
    for row in result:
        row_count = row.row_count
    # Get the total number of rows in the table
    current_rows = table.num_rows
    prev_no_rows = row_count
    new_row_added = current_rows - prev_no_rows
    new_data_percentage = (new_row_added/ prev_no_rows) * 100
    #new_data_percentage = 12
    print("New_data_Percentage",new_data_percentage)

    return new_data_percentage



def export_model_to_gcs(client, project_id, dataset_id, model_id, bucket_name):
    destination_uri = f"gs://{bucket_name}/{model_id}/"
    query = f"""
    EXPORT MODEL `{project_id}.{dataset_id}.{model_id}`
    OPTIONS(uri='{destination_uri}');
    """
    query_job = client.query(query)
    result  = query_job.result()  # Wait for the job to complete
    print(f"Model `{model_id}` exported to `{destination_uri}` successfully.")
    return result


def retrain_model(client, project_id, dataset_id, model_id, table_id, count_id, stream_data, threshold):
    new_data_percentage = new_count(table_id, count_id, stream_data)
    if new_data_percentage > threshold:
        try:
            # Step 5: Create the Churn Prediction Model
            create_model_query = f"""
            CREATE OR REPLACE MODEL `{project_id}.{dataset_id}.{model_id}`
            OPTIONS(
                model_type='logistic_reg', 
                input_label_cols=['churn_label']
            ) AS
            SELECT
              ccd.*,
              cd.churn_label AS churn_label
            FROM
              `{project_id}.{dataset_id}.encoded_clickstream_data_encoded` AS ccd
            JOIN
              `{project_id}.{dataset_id}.churn_data` AS cd
            ON
              ccd.user_id = cd.user_id
              AND ccd.client_id = cd.client_id;
            """

            # Execute the query to create or replace the model
            create_model_job = client.query(create_model_query)
            create_model_job.result()  # Waits for the query to complete

            # Step 6: Evaluate the Churn Prediction Model
            evaluate_model_query = f"""
            SELECT
              *
            FROM
              ML.EVALUATE(MODEL `{project_id}.{dataset_id}.{model_id}`);
            """

            # Execute the query to evaluate the model
            evaluate_model_job = client.query(evaluate_model_query)
            evaluation_results = evaluate_model_job.result()  # Waits for the query to complete

            # Print evaluation results
            for row in evaluation_results:
                print(row)

            print(f"Model {model_id} has been retrained and evaluated successfully.")

        except Exception as e:
            print(f"Error retraining model {model_id}: {e}")





def automate_model_training(request):
    bq_client = bigquery.Client()
    request_json = request.get_json(silent=True)
    if request_json and 'project_id' in request_json:
        project_id = request_json['project_id']
    else: 
        project_id = "retail-pipeline-beamlytics1"
    if request_json and 'dataset_id' in request_json:
        dataset_id = request_json['dataset_id']
    else: 
        dataset_id = "Retail_Store1"
    if request_json and 'model_id' in request_json:
        model_id =  request_json['model_id']
    else: 
        model_id = "customer_churn_model"
    if request_json and 'bucket_name' in request_json:
        bucket_name = request_json['bucket_name']
    else: 
        bucket_name = "automation_bucket4"

    if request_json and 'function_name' in request_json:
        function_name = request_json['function_name']
    else:
        function_name = "Customer_Churn_Automation_Model"
    
    if request_json and 'location' in request_json:
        location = request_json['location']
    else:
        location = "us-east1"
  
    if request_json and 'count_id' in request_json:
        count_id = request_json['count_id']
    else:
        count_id = "retail-pipeline-beamlytics.Retail_Store.row_count"
  
    if request_json and 'table_id' in request_json:
        table_id = request_json['table_id']
    else:
        table_id = "retail-pipeline-beamlytics.Retail_Store.clean_clickstream_data"
   
    if request_json and 'stream_data' in request_json:
        stream_data = request_json['stream_data']
    else:
        stream_data = "clean_clickstream_data"

    if request_json and 'threshold' in request_json:
        threshold = request_json['threshold']
    else:
        threshold = 10

            

    # Retrain the model
    retrain_model(bq_client, project_id, dataset_id, model_id,table_id,count_id,stream_data,threshold)

    # Export the trained model to Cloud Storage
    return (export_model_to_gcs(bq_client, project_id, dataset_id, model_id, bucket_name))

    # Update the Cloud Function with the new model
    #model_path = f"gs://{bucket_name}/{model_id}/"
    #update_cloud_function(project_id, "us-east1", function_name, model_path)


def predict(request):
    # Set up BigQuery client
    client = bigquery.Client()

    # Get the input data from the request
    request_json = request.get_json(silent=True)


    if request_json and 'project_id' in request_json:
        project_id = request_json['project_id']
    else: 
        project_id = "retail-pipeline-beamlytics1"
    if request_json and 'dataset_id' in request_json:
        dataset_id = request_json['dataset_id']
    else: 
        dataset_id = "Retail_Store1"
    if request_json and 'model_id' in request_json:
        model_id =  request_json['model_id']
    else: 
        model_id = "customer_churn_model"
    
    if request_json and 'user_id' in request_json:
        user_id = request_json['user_id']
    else: 
        user_id = 74657
    
    if request_json and 'client_id' in request_json:
        client_id = request_json['client_id']
    else: 
        client_id = "52393647"

    if request_json and 'event' in request_json:
        event = request_json['event']
    else:
        event = "browse"

    if request_json and 'page' in request_json:
        page = request_json['page']
    else:
        page = "P_1"

    if request_json and 'page_previous' in request_json:
        page_previous = request_json['page_previous']
    else:
        page_previous = "P_5"

    if request_json and 'ecommerce_items_index' in request_json:
        ecommerce_items_index = request_json['ecommerce_items_index']
    else:
        ecommerce_items_index = 9

    if request_json and 'ecommerce_items_item_name' in request_json:
        ecommerce_items_item_name = request_json['ecommerce_items_item_name']
    else:
        ecommerce_items_item_name = "Voltas 1.5 Ton, 5 Star, Inverter Split AC(Copper, 4-in-1 Adjustable Mode, Anti-dust Filter, 2023 Model, 185V DAZJ, White)"

    if request_json and 'ecommerce_items_item_id' in request_json:
        ecommerce_items_item_id = request_json['ecommerce_items_item_id']
    else:
        ecommerce_items_item_id = 9

    if request_json and 'ecommerce_items_price' in request_json:
        ecommerce_items_price = request_json['ecommerce_items_price']
    else:
        ecommerce_items_price = 73990.0

    if request_json and 'ecommerce_items_item_brand' in request_json:
        ecommerce_items_item_brand = request_json['ecommerce_items_item_brand']
    else:
        ecommerce_items_item_brand = "Amazon"

    if request_json and 'event_unknown' in request_json:
        event_unknown = request_json['event_unknown']
    else:
        event_unknown = 0

    if request_json and 'event_add_to_cart' in request_json:
        event_add_to_cart = request_json['event_add_to_cart']
    else:
        event_add_to_cart = 0

    if request_json and 'event_purchase' in request_json:
        event_purchase = request_json['event_purchase']
    else:
        event_purchase = 0

    if request_json and 'page_unknown' in request_json:
        page_unknown = request_json['page_unknown']
    else:
        page_unknown = 0

    if request_json and 'page_homepage' in request_json:
        page_homepage = request_json['page_homepage']
    else:
        page_homepage = 0

    if request_json and 'page_product_page' in request_json:
        page_product_page = request_json['page_product_page']
    else:
        page_product_page = 0

    if request_json and 'page_cart' in request_json:
        page_cart = request_json['page_cart']
    else:
        page_cart = 0

    if request_json and 'page_checkout' in request_json:
        page_checkout = request_json['page_checkout']
    else:
        page_checkout = 0

    if request_json and 'page_previous_unknown' in request_json:
        page_previous_unknown = request_json['page_previous_unknown']
    else:
        page_previous_unknown = 0

    if request_json and 'page_previous_homepage' in request_json:
        page_previous_homepage = request_json['page_previous_homepage']
    else:
        page_previous_homepage = 0

    if request_json and 'page_previous_product_page' in request_json:
        page_previous_product_page = request_json['page_previous_product_page']
    else:
        page_previous_product_page = 0

    if request_json and 'page_previous_cart' in request_json:
        page_previous_cart = request_json['page_previous_cart']
    else:
        page_previous_cart = 0

    if request_json and 'page_previous_checkout' in request_json:
        page_previous_checkout = request_json['page_previous_checkout']
    else:
        page_previous_checkout = 0

    if request_json and 'ecommerce_items_item_category' in request_json:
        ecommerce_items_item_category = request_json['ecommerce_items_item_category']
    else:
        ecommerce_items_item_category = "Electronics"

    if request_json and 'ecommerce_items_item_category_2' in request_json:
        ecommerce_items_item_category_2 = request_json['ecommerce_items_item_category_2']
    else:
        ecommerce_items_item_category_2 = "appliances"

    if request_json and 'ecommerce_items_item_category_3' in request_json:
        ecommerce_items_item_category_3 = request_json['ecommerce_items_item_category_3']
    else:
        ecommerce_items_item_category_3 = "Air Conditioners"

    if request_json and 'ecommerce_items_item_variant' in request_json:
        ecommerce_items_item_variant = request_json['ecommerce_items_item_variant']
    else:
        ecommerce_items_item_variant = "White"

    if request_json and 'ecommerce_items_item_list_name' in request_json:
        ecommerce_items_item_list_name = request_json['ecommerce_items_item_list_name']
    else:
        ecommerce_items_item_list_name = "Voltas 1.5 Ton, 5 Star, Inverter Split AC(Copper, 4-in-1 Adjustable Mode, Anti-dust Filter, 2023 Model, 185V DAZJ, White)"

    if request_json and 'ecommerce_items_item_list_id' in request_json:
        ecommerce_items_item_list_id = request_json['ecommerce_items_item_list_id']
    else:
        ecommerce_items_item_list_id = "SR9"

    if request_json and 'ecommerce_items_quantity' in request_json:
        ecommerce_items_quantity = request_json['ecommerce_items_quantity']
    else:
        ecommerce_items_quantity = 1

    if request_json and 'ecommerce_items_item_category_4' in request_json:
        ecommerce_items_item_category_4 = request_json['ecommerce_items_item_category_4']
    else:
        ecommerce_items_item_category_4 = "Electronics"




    # Create a temporary table
    temp_table_id = f"{project_id}.{dataset_id}.churn_prediction_table"
    
    # Define the schema for the temporary table
    schema = [
        bigquery.SchemaField("user_id", "INT64"),
        bigquery.SchemaField("client_id", "STRING"),
        bigquery.SchemaField("event", "STRING"),
        bigquery.SchemaField("page", "STRING"),
        bigquery.SchemaField("page_previous", "STRING"),
        bigquery.SchemaField("ecommerce_items_index", "INT64"),
        bigquery.SchemaField("ecommerce_items_item_name", "STRING"),
        bigquery.SchemaField("ecommerce_items_item_id", "INT64"),
        bigquery.SchemaField("ecommerce_items_price", "FLOAT64"),
        bigquery.SchemaField("ecommerce_items_item_brand", "STRING"),
        bigquery.SchemaField("event_unknown", "INT64"),
        bigquery.SchemaField("event_add_to_cart", "INT64"),
        bigquery.SchemaField("event_purchase", "INT64"),
        bigquery.SchemaField("page_unknown", "INT64"),
        bigquery.SchemaField("page_homepage", "INT64"),
        bigquery.SchemaField("page_product_page", "INT64"),
        bigquery.SchemaField("page_cart", "INT64"),
        bigquery.SchemaField("page_checkout", "INT64"),
        bigquery.SchemaField("page_previous_unknown", "INT64"),
        bigquery.SchemaField("page_previous_homepage", "INT64"),
        bigquery.SchemaField("page_previous_product_page", "INT64"),
        bigquery.SchemaField("page_previous_cart", "INT64"),
        bigquery.SchemaField("page_previous_checkout", "INT64"),
        bigquery.SchemaField("ecommerce_items_item_category", "STRING"),
        bigquery.SchemaField("ecommerce_items_item_category_2", "STRING"),
        bigquery.SchemaField("ecommerce_items_item_category_3", "STRING"),
        bigquery.SchemaField("ecommerce_items_item_variant", "STRING"),
        bigquery.SchemaField("ecommerce_items_item_list_name", "STRING"),
        bigquery.SchemaField("ecommerce_items_item_list_id", "STRING"),
        bigquery.SchemaField("ecommerce_items_quantity", "INT64"),
        bigquery.SchemaField("ecommerce_items_item_category_4", "STRING")
    ]

    # Create the table if it does not exist
    try:
        client.get_table(temp_table_id)
        print(f"Table {temp_table_id} already exists.")
    except NotFound:
        table = bigquery.Table(temp_table_id, schema=schema)
        client.create_table(table)
        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    # Insert the input data into the temporary table
    instance = {
    "user_id": user_id,
    "client_id": client_id,
    "event": event,
    "page": page,
    "page_previous": page_previous,
    "ecommerce_items_index": ecommerce_items_index,
    "ecommerce_items_item_name": ecommerce_items_item_name,
    "ecommerce_items_item_id": ecommerce_items_item_id,
    "ecommerce_items_price": ecommerce_items_price,
    "ecommerce_items_item_brand": ecommerce_items_item_brand,
    "event_unknown": event_unknown,
    "event_add_to_cart": event_add_to_cart,
    "event_purchase": event_purchase,
    "page_unknown": page_unknown,
    "page_homepage": page_homepage,
    "page_product_page": page_product_page,
    "page_cart": page_cart,
    "page_checkout": page_checkout,
    "page_previous_unknown": page_previous_unknown,
    "page_previous_homepage": page_previous_homepage,
    "page_previous_product_page": page_previous_product_page,
    "page_previous_cart": page_previous_cart,
    "page_previous_checkout": page_previous_checkout,
    "ecommerce_items_item_category": ecommerce_items_item_category,
    "ecommerce_items_item_category_2": ecommerce_items_item_category_2,
    "ecommerce_items_item_category_3": ecommerce_items_item_category_3,
    "ecommerce_items_item_variant": ecommerce_items_item_variant,
    "ecommerce_items_item_list_name": ecommerce_items_item_list_name,
    "ecommerce_items_item_list_id": ecommerce_items_item_list_id,
    "ecommerce_items_quantity": ecommerce_items_quantity,
    "ecommerce_items_item_category_4": ecommerce_items_item_category_4
}

    # Ensure the input data is in the correct format
    input_data = [instance]
    
    errors = client.insert_rows_json(temp_table_id, input_data)
    if errors:
        raise RuntimeError(f"Failed to insert data into temporary table: {errors}")

    # Construct the prediction SQL query
    query = f"""
    SELECT
      *
    FROM
      ML.PREDICT(MODEL `{project_id}.{dataset_id}.{model_id}`, 
      (SELECT * FROM `{temp_table_id}`))
    """

    # Execute the query
    query_job = client.query(query)
    results = query_job.result()

    # Convert the results to a list of dictionaries
    predictions = [dict(row) for row in results]
    print(predictions)

    return predictions[-1]




