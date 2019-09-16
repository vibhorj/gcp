
def run_pipeline(p=None, known_args=None):
    def add_timestamps(e, timestamp=beam.DoFn.TimestampParam, *args, **kwargs):
        payload = e.data.decode()
        evt_time = e.attributes.get('evt_time')
        row = {'evt_time': evt_time, 'payload': payload}
        return row

    def parse_payload(e, timestamp=beam.DoFn.TimestampParam, *args, **kwargs):
        import json
        element = e.copy()
        payload = json.loads(element.pop('payload'))
        element.update(payload)
        return element

    PUB_SUB_TOPIC = known_args.topic
    PROJECT, BQ_DATASET, BQ_TABLE = known_args.project, known_args.dataset, known_args.table


    row = p | "read_sub" >> beam.io.gcp.pubsub.ReadFromPubSub(topic=PUB_SUB_TOPIC, with_attributes=True) \
            | "add_timestamps" >> beam.Map(add_timestamps)


    row | "write_1_raw_stream_to_bq" >> bigquery.WriteToBigQuery(project=PROJECT,
                                                                 dataset=BQ_DATASET,
                                                                 table=BQ_TABLE + "_test_raw")


    row | "parse" >> beam.Map(parse_payload)  \
        | "write_2_parsed_stream_to_bq_write2" >> bigquery.WriteToBigQuery(project=PROJECT,
                                                                           dataset=BQ_DATASET,
                                                                           table=BQ_TABLE + "_test_parsed")

    p.run().wait_until_finish()


parser = argparse.ArgumentParser()
parser.add_argument('--project')
parser.add_argument('--dataset')
parser.add_argument('--table')
parser.add_argument('--topic')

known_args, unknown_args = parser.parse_known_args(sys.argv)

options = PipelineOptions(flags = unknown_args+["--project",known_args.project])
p = beam.Pipeline(runner = None, options = options,argv = None)
run_pipeline(p, known_args = known_args)

