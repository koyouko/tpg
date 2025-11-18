import argparse
import json
import os
import subprocess
import tempfile
from datetime import datetime

from confluent_kafka import Consumer
import requests


def build_consumer(cluster, env, topic):
    # TODO: replace with your real mappings
    kafka_bootstrap = {
        ("PHY-PROD-CL1", "PROD"): "phy-prod-kafka:9093",
        ("VM-UAT-CL2", "UAT"): "vm-uat-kafka:9093",
    }[(cluster, env)]

    conf = {
        "bootstrap.servers": kafka_bootstrap,
        "group.id": f"kafka-dump-{topic}",
        "auto.offset.reset": "earliest",
        # add SSL/SASL config, certs, keytabs, etc.
        # "security.protocol": "SSL",
        # "ssl.ca.location": "/path/to/ca.pem",
        # "ssl.certificate.location": "/path/to/client.pem",
        # "ssl.key.location": "/path/to/client.key",
    }
    c = Consumer(conf)
    c.subscribe([topic])
    return c


def dump_messages(consumer, topic, out_dir, max_messages=None):
    msg_count = 0
    dump_file = os.path.join(out_dir, f"{topic}.jsonl")
    with open(dump_file, "w", encoding="utf-8") as f:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            record = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": msg.value().decode("utf-8") if msg.value() else None,
                "timestamp": msg.timestamp()[1],
            }
            f.write(json.dumps(record) + "\n")
            msg_count += 1
            if max_messages and msg_count >= max_messages:
                break
    return msg_count, dump_file


def create_metadata(out_dir, args, msg_count):
    meta = {
        "request_id": args.request_id,
        "inc_chg": args.inc_chg,
        "cluster": args.cluster,
        "env": args.env,
        "topic": args.topic,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "created_by": args.requestor,
        "message_count": msg_count,
    }
    meta_file = os.path.join(out_dir, "metadata.json")
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    return meta_file


def tar_and_encrypt(src_dir, request_id, password):
    tar_name = f"{request_id}.tar"
    enc_name = f"{request_id}.tar.enc"

    # Create tar
    subprocess.check_call(["tar", "cf", tar_name, "-C", src_dir, "."])

    # Encrypt with AES-256-CBC
    subprocess.check_call([
        "openssl", "enc", "-aes-256-cbc", "-salt",
        "-in", tar_name, "-out", enc_name,
        "-k", password
    ])

    return os.path.abspath(enc_name)


def upload_to_artifactory(enc_file, env, topic, request_id):
    base_url = os.environ["ARTIFACTORY_BASE_URL"]
    user = os.environ["ARTIFACTORY_USER"]
    password = os.environ["ARTIFACTORY_PASSWORD"]

    repo_path = f"kafka-dump/{env.lower()}/{topic}/{request_id}/{os.path.basename(enc_file)}"
    url = f"{base_url.rstrip('/')}/{repo_path}"

    with open(enc_file, "rb") as f:
        resp = requests.put(url, data=f, auth=(user, password))
    resp.raise_for_status()
    return url


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inc-chg", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--env", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--otp", required=True)
    parser.add_argument("--requestor", required=True)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as workdir:
        consumer = build_consumer(args.cluster, args.env, args.topic)
        try:
            msg_count, dump_file = dump_messages(consumer, args.topic, workdir)
        finally:
            consumer.close()

        create_metadata(workdir, args, msg_count)
        enc_file = tar_and_encrypt(workdir, args.request_id, args.otp)
        art_url = upload_to_artifactory(enc_file, args.env, args.topic, args.request_id)
        print(json.dumps({"status": "OK", "artifactory_url": art_url}))


if __name__ == "__main__":
    main()
