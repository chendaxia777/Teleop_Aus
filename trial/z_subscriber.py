import zenoh, time

def listener(sample):
    print(f"Received {sample.kind} ('{sample.key_expr}': '{sample.payload.to_string()}')")

if __name__ == "__main__":
    config = zenoh.Config()
    config.insert_json5("mode", '"client"')
    config.insert_json5("connect/endpoints", '["tcp/10.78.62.83:7447"]')
    with zenoh.open(config) as session:
        sub = session.declare_subscriber('myhome/kitchen/temp', listener)
        time.sleep(60)