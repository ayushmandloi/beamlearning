# beamlearning
This repo contains code for apache beam pipelines. It if focused on running pipelines on Google cloud , both batch and streaming.


## Command to create template for side_input.py file 
```--project=<project-name>
--service_account_credentials_file_path=service_key.json
--runner=Dataflow
--staging_location=gs://<bucket-name>/staging
--temp_location=gs://<bucket-name>/tmp
--template_location=gs://<bucket-name>/templates/beamlearning
--region=us-west1
```

## Command to run side input template online 
```
gcloud dataflow jobs run beam-learb --gcs-location gs://<bucket-name>/templates/beamlearning --parameters gcp_folder=gs://test-global-data/username-password-recovery-code.csv, side_input=gs://<bucket-name>/username.csv
```

## Command to run side output template online 

```
gcloud dataflow jobs run beam-learn_side_output --gcs-location gs://<bucket>/templates/beamlearning_side_output --parameters gcp_folder=gs://<bucket>/username-password-recovery-code.csv
```


## Command to run pubsub to bigquery dataflow job from computer

```
pubsub_to_bigquery.py --project=<project-name> --service_account_credentials_file_path=service_key.json --runner=direct --staging_location=gs://<project-name>/staging --temp_location=gs://<project-name>/tmp --region=us-west1
```

## Service account role required for Dataflow job 

```
BigQuery Data Owner
BigQuery User
Cloud Functions Invoker
Compute Instance Admin (v1)
Dataflow Developer
Dataflow Worker
Pub/Sub Editor
Service Account User
Storage Admin
BigQuery User
```

