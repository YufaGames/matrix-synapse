from datetime import datetime
import datetime, json, os, logging, requests, time
from psycopg2 import connect
from dotenv import load_dotenv

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)

load_dotenv()

def connect_database():
    try:
        conn = connect(host = os.getenv('HOSTNAME'),port = os.getenv('PORT'),database = os.getenv('DB_NAME'),user = os.getenv('DB_USERNAME'),password = os.getenv('PASSWORD'))
        cur = conn.cursor()
        logging.info("Connecting to database")
        return cur, conn
    except Exception as error:
        raise str(error)

def older_time():
    timestamp = (datetime.datetime.now() - datetime.timedelta(minutes=5)).timestamp()
    return timestamp

response = requests.get(
    f"http://localhost:8008/_synapse/admin/v1/rooms?limit=300",
    headers={"Authorization": f"Bearer syt_YWRtaW50ZXN0_DOAULfmwmhqAlAWKEXjB_3CglNX"},
)
rooms = json.loads(response.text)["rooms"]

empty_rooms = [room["room_id"] for room in rooms if room["joined_local_members"] == 0]

logging.info("Empty Rooms: %s",empty_rooms)

for room_id in empty_rooms:
    logging.info(f"purging room {room_id}...")
    response = requests.post(
        f"http://localhost:8008/_synapse/admin/v1/purge_room",
        headers={"Authorization": f"Bearer syt_YWRtaW50ZXN0_DOAULfmwmhqAlAWKEXjB_3CglNX", "Content-Type": "application/json"},
        data=json.dumps({"room_id": room_id}),
    )
    time.sleep(0.5)


response = requests.get(
    f"http://localhost:8008/_synapse/admin/v1/rooms?limit=300",
    headers={"Authorization": f"Bearer syt_YWRtaW50ZXN0_DOAULfmwmhqAlAWKEXjB_3CglNX"},
)
rooms = json.loads(response.text)["rooms"]

large_rooms = [room["room_id"] for room in rooms ]

logging.info("large Rooms: %s",large_rooms)

print(older_time())

if len(large_rooms) > 0:
    dateUntil = older_time()
    dateUntil = dateUntil*1000
    dateUntil = round(dateUntil)

    for room_id in large_rooms:
        logging.info(f"purging history for {room_id}...")
        response = requests.post(
            f"http://localhost:8008/_synapse/admin/v1/purge_history/{room_id}",
            headers={"Authorization": f"Bearer syt_YWRtaW50ZXN0_DOAULfmwmhqAlAWKEXjB_3CglNX", "Content-Type": "application/json"},
            data=json.dumps({"delete_local_events": True, "purge_up_to_ts": dateUntil}),
        )
        time.sleep(0.5)
        print("response: ", response.text)

# res = requests.request("GET",f"http://localhost:8008/_synapse/admin/v1/purge_history_status/MEItlzmSiiJbVWwC",headers={"Authorization": f"Bearer syt_YWRtaW50ZXN0_DOAULfmwmhqAlAWKEXjB_3CglNX", "Content-Type": "application/json"},
# )
# print(res.text)