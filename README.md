# beamlearning
This repo contains code for apache beam pipelines. It if focused on running pipelines on Google cloud , both batch and streaming.


# Command to create template for side_input.py file 
```--project=<project-name>
--service_account_credentials_file_path=service_key.json
--runner=Dataflow
--staging_location=gs://<bucket-name>/staging
--temp_location=gs://<bucket-name>/tmp
--template_location=gs://<bucket-name>/templates/beamlearning
--region=us-west1
```

