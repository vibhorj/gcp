# python scrap.py --streaming

import os, sys,argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import  PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp import pubsub

parser = argparse.ArgumentParser()
app_params, pipeline_args = parser.parse_known_args(sys.argv)
options = PipelineOptions(flags = pipeline_args)
options.view_as(GoogleCloudOptions).impersonate_service_account = "XYZ_SERVICE_ACCOUNT@PROJECT_ID.iam.gserviceaccount.com"


p = beam.Pipeline(runner=None, options=options,  argv=None)
source = pubsub.ReadFromPubSub(subscription= os.environ.get("SUB"))
stream = p | "read" >> source

p.run().wait_until_finish()


