    id_seed, USER_ID = 2, 141
    
    while True:
        message_id, event_time = str(id_seed), str(datetime.now().isoformat() + 'Z')
        message = "cid={}&message_id={}&evt_time={}".format(USER_ID,message_id,event_time)
        future = publisher.publish(TOPIC_PATH,
                                   json.dumps({"hitPayload":message}).encode(),
                                   message_id=message_id, event_time=event_time)
        future.add_done_callback(callback)
        time.sleep(15)
