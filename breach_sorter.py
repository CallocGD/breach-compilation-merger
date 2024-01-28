from concurrent.futures import ThreadPoolExecutor, Future
from typing import Union
from pathlib import Path
from threading import Thread
import time
import heapq
import os
import click
from dataclasses import dataclass, field
import re


def read_file(file: str):
    with open(file, "rb") as rb:
        for line in rb:
            if line.strip():
                yield line


# 10MB of memory by default...
DEFAULT_SIZE = 10 * 1024 * 1024


@dataclass
class ExternalSorting:
    """Externally sorts out given files, This does the same thing as
    Cmsort but just a little bit faster..."""

    input: str
    ram_size: int = DEFAULT_SIZE
    view: list[str] = field(default_factory=list, init=False)

    def read_input(self):
        with open(self.input, "rb") as r:
            for line in r:
                if line.strip():  # don't yeild empty lines...
                    yield line

    def dump_set(self, file: str, myset: set[bytes]):
        """Part of phase 1 and 2 for dumping dataset"""
        with open(file, "ab") as w:
            for m in sorted(myset):
                # m should have \n in it already...
                w.write(m)

    def phase_1(self):
        current_mem = 0
        idx = 0
        dataset = set()
        for line in self.read_input():
            if line not in dataset:
                current_mem += len(line)
                dataset.add(line)
                if current_mem >= self.ram_size:
                    idx += 1
                    file = self.input + ("_%i.tmp" % idx)
                    self.view.append(file)
                    self.dump_set(file, dataset)
                    # Cleanup dataset and continue...
                    dataset.clear()
                    # Reset internal memory
                    current_mem = 0
        if dataset:
            # Final dump
            idx += 1
            file = str(self.input) + ("_%i.tmp" % idx)
            self.view.append(file)
            self.dump_set(file, dataset)
            dataset.clear()

    def read_file(self, file: str):
        return read_file(file)

    def heapload(self):
        reader = [self.read_file(f) for f in self.view]
        # TODO Figure out how heapq merge works since we will need to write this in C/C++ or rust code...
        for r in heapq.merge(*reader):
            yield r

    def phase_2(self, output: str):
        """Used to sort everything to the final package"""
        # build the final iterable...
        current_mem = 0
        dataset = set()
        for line in self.heapload():
            if not line in dataset:
                current_mem += len(line)
                dataset.add(line)
                if current_mem >= self.ram_size:
                    self.dump_set(output, dataset)
                    dataset.clear()
                    current_mem = 0
        if dataset:
            self.dump_set(output, dataset)
            dataset.clear()

    def sort(self, output: str):
        """sorts the file itself which has a chance to take a very long time..."""
        self.phase_1()
        self.phase_2(output)
        for f in self.view:
            os.remove(f)


letters = bytearray(b"0123456789abcdefghijklmnopqrstuvwxyz")


def make_re(l: list[str]):
    the_regex = "^"
    for r in l:
        if len(r) != 1:
            the_regex += "[^A-Za-z0-9]"
        elif r in "abcdefghijklmnopqrstuvwxyz":
            the_regex += f"[{r.lower()}|{r.upper()}]"
        else:
            the_regex += r
    return re.compile((the_regex + "[^\n]+$").encode(), re.MULTILINE)


class BreachMerger:
    """Used to take and Merge data to our own breach-complation"""

    def __init__(self, path: str, ram_size=DEFAULT_SIZE) -> None:
        if not path.endswith("data"):
            self.path = Path(path) / "data"
        else:
            self.path = Path(path)
        if not self.path.exists():
            self.path.mkdir()
            self.make_files(self.path)
            time.sleep(3)
        # We can ultilize a threadpool on most of our required operations...
        self.te = ThreadPoolExecutor()
        self.futures: list[Future] = []
        self.done = False
        self.runner = Thread(target=self.run)
        self.runner.start()
        self.ram_size = ram_size

    def reset(self):
        self.runner = Thread(target=self.run)
        self.runner.start()

    def run(self):
        while self.futures or not self.done:
            if self.futures:
                fut = self.futures.pop()
                if not fut.done():
                    self.futures.append(fut)
                else:
                    fut.result()
            # wait for a little bit...
            time.sleep(0.005)

    def join(self):
        self.done = True
        self.runner.join()

    def execute(self, fn, *args, **kwargs):
        self.futures.append(self.te.submit(fn, *args, **kwargs))

    def make_files(self, path: Path):
        for l in letters:
            open(path / chr(l), "wb").write(b"\n")
        open(path / "symbols", "wb").write(b"\n")


    def write_to_file(self, l: list[str], chunk: bytes):
        my_output = str(self.path / Path(*l))
        print(my_output)
        with open(my_output, "ab") as w:
            for line in make_re(l).finditer(chunk):
                w.write(line.group(0) + b"\n")

    def write_chunk_to_all_files(self, chunk: bytes):
        """Writes all the data to each of the chunks iterated over..."""
        stack = [self.path]
        while stack:
            p = stack.pop()
            if p.is_dir():
                stack.extend(p.iterdir())
            else:
                _, _letters_ = p.as_posix().rsplit("data/", 1)
                self.execute(self.write_to_file, _letters_.split("/"), chunk)
        self.join()
        # We're not done yet...
        self.reset()

    def do_external_sorting(self, p: Path):
        ExternalSorting(p).sort(str(p) + ".tmp")
        os.remove(p)
        os.rename(str(p) + ".tmp", p)

    def sort_all_files(self):
        """Writes all the data to each of the chunks iterated over..."""
        stack = [self.path]
        while stack:
            p = stack.pop()
            if p.is_dir():
                stack.extend(p.iterdir())
            else:
                self.execute(self.do_external_sorting, p=p)
        # sort the data...
        self.join()

    def walk_with_file(self, file: str):
        """Takes the file and dumps it into chunks and then transfers everything on over to each of it's
        destinations required..."""
        _next_chunk_part = b""
        with open(file, "rb") as rb:
            while x := rb.read(self.ram_size):
                x = _next_chunk_part + x
                # remove the backend if needed...
                x, _next_chunk_part = x.rsplit(b"\n", 1)
                print("dumping...")
                self.write_chunk_to_all_files(x)

        if _next_chunk_part:
            self.write_chunk_to_all_files(_next_chunk_part)

    def find_location(self, line: bytes):
        """Should always write a new line no matter what..."""
        target = self.path
        for i in line.lower():
            target = target / (chr(i) if i in letters else "symbols")
            if target.is_file() or not target.exists():
                return target

    def merge_combolists(self, combos: Union[list[str], str]):
        """Turns combolists into a breach complation..."""
        if isinstance(combos, str):
            combos = [combos]

        # TODO Hash files same way breach complation shell scripts do it
        # to prevent repeated files from being done...
        for cl in set(combos):
            print(f"[...] Merging {cl}")
            self.walk_with_file(cl)
            print(f"[+] Combolist {cl} complete!")
        print("Sorting files...")
        self.sort_all_files()
        print("[+] Files Were all sorted...")


# TODO ADD Automatic Splitter tool when files become bigger than 300MB

@click.command
@click.argument("combos", nargs=-1, type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--compilation",
    type=click.Path(exists=True),
    help="Loaction of your breach compilation or compilation of many breach otherwise make it in the parent directory you are in",
    default="."
)
@click.option(
    "--ram",
    "-r",
    type=int,
    default=DEFAULT_SIZE,
    help="Size of the Chunks to load Default is Equvilent to 10MB",
)
def cli(combos: list[str],compilation:str, ram: int):
    """An Optimized Version of the breach complation's tools made for sorting the files and merging combolists...
    This will use threading to speedup the sorting and merging of everything."""
    BreachMerger(compilation, ram).merge_combolists(combos)

if __name__ == "__main__":
    cli()


