import asyncio

from file_manager import tmp_path
from download_utils.download_files import download_files
from extracao_rfb.rfb_client import ClientRFB


client = ClientRFB()

comp = "2026-02"

result = [client.get_file_url(x) for x in client.list_files(comp)]

output_dir = tmp_path()

chunk_size = 1024 * 1024

asyncio.run(download_files(output_dir, result, 5, chunk_size))
