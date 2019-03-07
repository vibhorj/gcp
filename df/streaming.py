#from __future__ import absolute_import
import sys, os
import json
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

import logging
logging.basicConfig(format='~~~ %(levelname)s: %(message)s', level=logging.INFO)
PROJECT, BUCKET, TOPIC = os.environ.get('PROJECT'), os.environ.get('BUCKET'), os.environ.get('TOPIC')

def run(argv=None):
	def f(e): # e is json (PY2 unicode string)
		payload = json.loads(e)
		logging.info(payload)
		return  payload #return a dic

	options = PipelineOptions(flags = argv)
	p = beam.Pipeline(options=options, runner=None, argv = [])

	p | "message" >> beam.io.gcp.pubsub.ReadFromPubSub(topic=TOPIC, subscription = None) \
	  | "map" >> beam.Map(f) \
	  | "sink_to_bq" >> bigquery.WriteToBigQuery(project = PROJECT, dataset = 'sw', table = 'payload'
	                                      #, schema='hitPayload:STRING'
								)
	result = p.run().wait_until_finish()

if __name__ == '__main__':
	run(sys.argv)

