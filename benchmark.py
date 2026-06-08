import time
import tempfile
import os
from pyrufh.server.disk import DiskRufhServer

def run_benchmark():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create server
        server = DiskRufhServer(storage_dir=tmpdir, cleanup_interval=0)

        print("Creating 1000 uploads...")
        uris = []
        for i in range(1000):
            upload, _ = server.create_upload(b"test data " + str(i).encode())
            uris.append(upload.uri)

        print("Benchmarking _get_upload...")
        # Get the last upload
        target_uri = uris[-1]

        start = time.time()
        for _ in range(100):
            server._get_upload(target_uri)
        end = time.time()

        print(f"Time taken for 100 _get_upload calls: {end - start:.4f} seconds")

        print("Benchmarking get_upload_info...")
        start = time.time()
        for _ in range(100):
            server.get_upload_info(target_uri)
        end = time.time()

        print(f"Time taken for 100 get_upload_info calls: {end - start:.4f} seconds")

if __name__ == "__main__":
    run_benchmark()
