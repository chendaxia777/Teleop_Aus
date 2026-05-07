import zenoh, random, time

random.seed()

def read_temp():
    return random.randint(15, 30)

if __name__ == "__main__":
    config = zenoh.Config()
    config.insert_json5("mode", '"client"')
    config.insert_json5("connect/endpoints", '["tcp/192.168.1.118:7447"]')
    
    with zenoh.open(config) as session:
        key = 'myhome/kitchen/temp'
        pub = session.declare_publisher(key)
        while True:
            t = read_temp()
            buf = f"{t}"
            print(f"Putting Data ('{key}': '{buf}')...")
            pub.put(buf)
            time.sleep(1)