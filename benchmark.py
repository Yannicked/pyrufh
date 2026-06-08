import time
from pyrufh.server.disk import DiskRufhServer
import tempfile
import os

def run_benchmark():
    with tempfile.TemporaryDirectory() as d:
        server = DiskRufhServer(d)
        # Create 1000 uploads to pad the disk server
        uris = []
        for i in range(1000):
            # Create uploads
            upload, _ = server.create_upload(b"data", complete=True)
            uris.append(upload.uri)

        target_uri = uris[-1]

        start_time = time.time()
        for _ in range(100):
            server.get_upload_info(target_uri)
        end_time = time.time()
        print(f"get_upload_info Baseline: {end_time - start_time:.4f}s for 100 calls")

        start_time = time.time()
        for _ in range(100):
            server._get_upload(target_uri)
        end_time = time.time()
        print(f"_get_upload Baseline: {end_time - start_time:.4f}s for 100 calls")

run_benchmark()
