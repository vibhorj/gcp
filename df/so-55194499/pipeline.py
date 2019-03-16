p = beam.Pipeline(options=options, runner=None, argv=[])

	def f(e): 
		payload = json.loads(e)  # .get('message_payload')
		return payload  # return a dic

	lines = p | beam.io.gcp.pubsub.ReadFromPubSub(topic=TOPIC, subscription=None,
	                                              id_label='message_id', timestamp_attribute='event_time')
	        | beam.Map(f) \
	        | "write_1" >> bigquery.WriteToBigQuery(project=PROJECT, dataset=BQ_DATASET, table=TABLE, schema='message_payload:STRING')
	result = p.run()
