import os
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import functions


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
    #new_data_percentage = (new_row_added/ prev_no_rows) * 100
    new_data_percentage = 12
    print("New_data_Percentage",new_data_percentage)

    return new_data_percentage



def export_model_to_gcs(client, project_id, dataset_id, model_id, bucket_name):
    destination_uri = f"gs://{bucket_name}/{model_id}/"
    query = f"""
    EXPORT MODEL `{project_id}.{dataset_id}.{model_id}`
    OPTIONS(uri='{destination_uri}');
    """
    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete
    print(f"Model `{model_id}` exported to `{destination_uri}` successfully.")

def update_cloud_function(project_id, location, function_name, model_path):
    client = functions.CloudFunctionsServiceClient()
    name = f"projects/{project_id}/locations/{location}/functions/{function_name}"

    # Get the existing function configuration
    function = client.get_function(name=name)

    # Update the environment variables
    function.environment_variables["MODEL_PATH"] = model_path

    # Deploy the function with the updated configuration
    operation = client.update_function(function=function)
    response = operation.result()  # Wait for the operation to complete

    print(f"Cloud Function `{function_name}` updated successfully with the new model path.")

def retrain_model(client, project_id, dataset_id, model_id,table_id,count_id,stream_data):
    new_data_percentage = new_count(table_id,count_id,stream_data)
    if new_data_percentage > 10 :
        query = f"""
        CREATE OR REPLACE MODEL `{project_id}.{dataset_id}.{model_id}`
        OPTIONS(
            model_type='logistic_reg'
        ) AS
        SELECT
            user_id,
            client_id,
            churn_label AS label
        FROM
            `{project_id}.{dataset_id}.churn_data`
        """
        
        query_job = client.query(query)
        query_job.result()
        print(f"Model `{model_id}` has been retrained successfully.")

def automate_model_training():
    bq_client = bigquery.Client()
    project_id = "retail-pipeline-beamlytics"
    dataset_id = "Retail_Store"
    model_id = "customer_churn_model"
    bucket_name = "ml_modelling"
    function_name = "Customer_Churn_Automation_Model"
    location = "us-east1"
    count_id = "retail-pipeline-beamlytics.Retail_Store.row_count" 
    table_id = "retail-pipeline-beamlytics.Retail_Store.clean_clickstream_data"
    stream_data="clean_clickstream_data"

    # Retrain the model
    retrain_model(bq_client, project_id, dataset_id, model_id,table_id,count_id,stream_data)

    # Export the trained model to Cloud Storage
    export_model_to_gcs(bq_client, project_id, dataset_id, model_id, bucket_name)

    # Update the Cloud Function with the new model
    #model_path = f"gs://{bucket_name}/{model_id}/"
    #update_cloud_function(project_id, "us-east1", function_name, model_path)


if __name__ == "__main__":
    automate_model_training()

# Cloud Function entry point for prediction
def predict(request):
    # Set up BigQuery client
    client = bigquery.Client()

    # Load the model from Cloud Storage
    bucket_name = "ml_modelling"
    model_path = os.environ.get("MODEL_PATH", f"gs://{bucket_name}/path/to/model.bqml")
    model = client.load_model(model_path)

    # Get the input data from the request
    instance = request.get_json()

    # Make a prediction with the loaded model
    prediction = model.predict(instances=[instance])

    # Return the prediction

    return prediction.to_dataframe().to_dict("records")

automate_model_training()
