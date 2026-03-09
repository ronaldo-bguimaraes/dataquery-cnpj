from itertools import count
import os

import asyncio
import aiohttp

from pathlib import Path
from tqdm import tqdm


async def _download_file(
    queue: asyncio.Queue,
    output_dir: str,
    session: aiohttp.ClientSession,
    chunk_size: int,
    counter: count[int],
    total_count: int,
    position: int
):
    while True:
        url = await queue.get()
        _count = next(counter)

        try:
            filename = url.rpartition("/")[2]
            output_file = Path(output_dir, filename)
            temp_file = output_file.with_suffix(".download")

            if output_file.exists():
                raise FileExistsError("Arquivo já existe no disco")

            limit_length = 30
            filename_desc = str(filename).ljust(limit_length)
            if len(filename_desc) > limit_length:
                filename_desc = filename_desc[:limit_length-3] + "..."

            tqdm_desc = f"[{_count}/{total_count}] {filename_desc}"

            async with session.get(url) as resp:
                resp.raise_for_status()

                content_length = resp.content_length

                pbar = tqdm(
                    total=content_length,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    ascii=True,
                    bar_format="{desc} [{bar:40}] {percentage:3.0f}% | {n_fmt}/{total_fmt} | {rate_fmt} | ETA {remaining}",
                    desc=tqdm_desc,
                    position=position,
                    leave=True
                )

                with open(temp_file, "wb") as file, pbar as bar:
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        file.write(chunk)
                        bar.update(len(chunk))

                temp_file.rename(output_file)

        except:
            if temp_file.exists():
                temp_file.unlink()

        finally:
            queue.task_done()


async def download_files(
    output_dir: str,
    urls: list[str],
    concurrency: int = 3,
    chunk_size: int = 4096
):
    os.makedirs(output_dir, exist_ok=True)

    queue = asyncio.Queue()

    counter = count(0)
    total_count = len(urls)

    for url in urls:
        queue.put_nowait(url)

    async with aiohttp.ClientSession() as session:
        workers: list[asyncio.Task] = []
        for i in range(concurrency):
            coro = _download_file(queue, output_dir, session, chunk_size, counter, total_count, i)
            workers.append(asyncio.create_task(coro))

        await queue.join()

        for worker in workers:
            worker.cancel()

        await asyncio.gather(*workers, return_exceptions=True)
