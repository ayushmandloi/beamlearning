def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    from googleapiclient.discovery import build
    file = event
    print(f"Processing file: {file['name']}.")
    
    job = "df_gcs_to_bq_ " + " " + file['name']
     #path of the dataflow template on google storage bucket
    template = "gs://<bucket-name>/templates/beamlearning_side_output"
     #user defined parameters to pass to the dataflow pipeline job
    parameters = {
     'gcp_folder': "gs://<bucket-name>/"+file['name'],
     }
     #tempLocation is the path on GCS to store temp files generated during the dataflow job
    environment = {'tempLocation': 'gs://<bucket-name>/tmp', "numWorkers": 2,
     "maxWorkers": 10}

    service = build('dataflow', 'v1b3', cache_discovery=False)
     #below API is used when we want to pass the location of the dataflow job
    request = service.projects().locations().templates().launch(
    projectId="<project-name>",
    gcsPath=template,
    location='us-central1',
    body={
     'jobName': job,
     'parameters': parameters,
     'environment':environment
     },
     )
    response = request.execute()
    job_id = response.get("job").get("id")
    print(job_id)
