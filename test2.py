from app import buffer, protocol

cluster_meta_data_path = "kafka2.log"
# os.system(f"hexdump {cluster_meta_data_path}")
with open(cluster_meta_data_path, "rb") as fd:
    reader = buffer.ByteReader(fd.read())

while not reader.eof:
    batch = protocol.record.Batch.deserialize(reader)
    # print(f"batch: {batch}")

    # for record in batch.records:
    #     print(f"record: {batch}")
