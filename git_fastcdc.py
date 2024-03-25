import os
import shlex
import sys
from fnmatch import fnmatch
from pathlib import Path
from subprocess import PIPE, run

import click
from fastcdc import fastcdc

read = sys.stdin.buffer.read
buffer = sys.stdout.buffer
write = buffer.write
flush = buffer.flush
tmpfile = Path(".fast_cdc_tmp_file_29310b6")
cdcdir = Path(".cdc")
cdcline = "/.cdc/**/*.cdc binary filter=git_fastcdc"
avg_min = 128 * 1024


def eprint(*args):
    print(*args, file=sys.stderr)


@click.group()
def cli():
    os.chdir(git_toplevel())


def read_pkt_line():
    length_hex = read(4)
    if not length_hex:
        return b""

    length = int(length_hex, 16)
    if length == 0:
        return b""

    res = read(length - 4)
    return res


def read_pkt_line_str():
    return read_pkt_line().decode().strip()


def write_pkt_line(data):
    length = len(data) + 4
    write(f"{length:04x}".encode() + data)
    flush()


def write_pkt_line_str(string):
    write_pkt_line(string.encode())


def flush_pkt():
    write(b"0000")
    flush()


def git_hash_blob(data):
    return (
        run(
            ["git", "hash-object", "-w", "-t", "blob", "--stdin"],
            check=True,
            stdout=PIPE,
            input=data,
        )
        .stdout.strip()
        .decode()
    )


def git_toplevel():
    return (
        run(
            ["git", "rev-parse", "--show-toplevel"],
            check=True,
            stdout=PIPE,
        )
        .stdout.decode()
        .strip()
    )


def git_get_blob(hash):
    return run(
        ["git", "cat-file", "blob", hash],
        check=True,
        stdout=PIPE,
    ).stdout


def git_show(id):
    return run(
        ["git", "show", id],
        check=True,
        stdout=PIPE,
    ).stdout


def git_add_cdc():
    run(["git", "add", ".cdc"], check=True)


def hash_dir(base, hash):
    dir = base / hash[0:2] / hash[2:4]
    dir.mkdir(parents=True, exist_ok=True)
    return dir / hash


def chunk_string(input_string, chunk_size=65516):
    return [
        input_string[i : i + chunk_size]
        for i in range(0, len(input_string), chunk_size)
    ]


def get_avg_size(size):
    box = int(size / 32)
    bits = box.bit_length()
    shift = max(bits - 5, 0)
    return (box >> shift) << shift


def clean(pathname):
    try:
        with tmpfile.open("wb") as f:
            while pkg := read_pkt_line():
                f.write(pkg)
        size = tmpfile.stat().st_size
        avg_size = max(avg_min, get_avg_size(size))
        write_pkt_line_str("status=success\n")
        flush_pkt()
        with tmpfile.open("rb") as f:
            for cdc in fastcdc(str(tmpfile), avg_size=avg_size):
                f.seek(cdc.offset)
                data = f.read(cdc.length)
                hash = git_hash_blob(data)
                path = hash_dir(cdcdir, hash).with_suffix(".cdc")
                with path.open("w") as w:
                    w.write(hash)
                write_pkt_line_str(f"{path.name}\n")
        flush_pkt()
        flush_pkt()
    finally:
        tmpfile.unlink()


def smudge_cdc(pathname, blob):
    while read_pkt_line():
        pass
    write_pkt_line_str("status=success\n")
    flush_pkt()
    write_pkt_line_str(pathname.name)
    flush_pkt()
    flush_pkt()


def clean_cdc(pathname):
    hash = read_pkt_line_str()
    assert read_pkt_line_str() == ""
    write_pkt_line_str("status=success\n")
    flush_pkt()
    blob = git_get_blob(hash)
    for chunk in chunk_string(blob):
        write_pkt_line(chunk)
    flush_pkt()
    flush_pkt()


def smudge(pathname, blob):
    lines = []
    while pkg := read_pkt_line():
        lines.extend(pkg.splitlines())
    write_pkt_line_str("status=success\n")
    flush_pkt()
    for line in lines:
        line = line.strip()
        if line:
            blob = git_get_blob(line)
            for chunk in chunk_string(blob):
                write_pkt_line(chunk)
    flush_pkt()
    flush_pkt()


def cat():
    pkgs = []
    append = pkgs.append
    while pkg := read_pkt_line():
        append(pkg)
    write_pkt_line_str("status=success\n")
    flush_pkt()
    for pkg in pkgs:
        write_pkt_line(pkg)
    flush_pkt()
    flush_pkt()


@cli.command()
def process():
    """Called by git to do fastcdc."""
    assert read_pkt_line_str() == "git-filter-client"
    assert read_pkt_line_str() == "version=2"
    write_pkt_line_str("git-filter-server")
    write_pkt_line_str("version=2")
    flush_pkt()
    assert read_pkt_line_str() == ""
    capability = set()
    while line := read_pkt_line_str():
        key, _, cap = line.partition("=")
        assert key == "capability"
        capability.add(cap)
    assert {"clean", "smudge"}.issubset(capability)
    write_pkt_line_str("capability=clean")
    write_pkt_line_str("capability=smudge")
    flush_pkt()
    while line := read_pkt_line_str():
        key, _, command = line.partition("=")
        assert key == "command"
        key, _, pathname = read_pkt_line_str().partition("=")
        pathname = Path(pathname)
        assert key == "pathname"
        blob = None
        ref = None
        treeish = None
        while line := read_pkt_line_str():
            key, _, value = line.partition("=")
            if key == "treeish":
                treeish = value
            elif key == "ref":
                ref = value
            elif key == "blob":
                blob = value
            else:
                RuntimeError("Unknown argument")
        if command == "clean":
            if str(pathname).startswith(".cdc/"):
                if pathname.suffix == ".cdc":
                    clean_cdc(pathname)
                else:
                    cat()
            else:
                clean(pathname)
        elif command == "smudge":
            if str(pathname).startswith(".cdc/"):
                smudge_cdc(pathname, blob)
            else:
                smudge(pathname, blob)
    cdc = Path(".cdc")
    if cdc.exists():
        git_add_cdc()


def remove_empty_dirs(path):
    path = Path(path)

    for subpath in path.iterdir():
        if subpath.is_dir():
            remove_empty_dirs(subpath)

    if not any(path.iterdir()):
        path.rmdir()


def read_blobs(entry, blobs):
    for blob in git_show(f"HEAD:{entry}").decode().splitlines():
        if fnmatch(blob, "*.cdc"):
            blobs.add(blob)


def prune_blobs(blobs):
    for file in Path(".").glob(".cdc/**/*.cdc"):
        if file.name not in blobs:
            file.unlink()


@cli.command()
def prune():
    """Prune fastcdc objects."""
    file = Path(".gitattributes")
    file_list = os.listdir(".")
    blobs = set()
    if file.exists():
        with file.open("r", encoding="UTF-8") as f:
            for line in f:
                if "filter=git_fastcdc" in line and cdcline not in line:
                    match = shlex.split(line)[0]
                    for entry in file_list:
                        if fnmatch(entry, match):
                            read_blobs(entry, blobs)
    prune_blobs(blobs)
    cdc = Path(".cdc")
    if cdc.exists():
        remove_empty_dirs(cdc)
        git_add_cdc()


@cli.command()
def install():
    """Install fastcdc in the current repository."""

    do_remove()
    run(
        [
            "git",
            "config",
            "--local",
            "filter.git_fastcdc.process",
            "git-fastcdc process",
        ],
        check=True,
    )
    run(
        [
            "git",
            "config",
            "--local",
            "filter.git_fastcdc.required",
            "true",
        ],
        check=True,
    )
    file = Path(".gitattributes")
    file.touch()
    with file.open("r", encoding="UTF-8") as f:
        data = f.read().strip()
    with file.open("w", encoding="UTF-8") as f:
        if data:
            f.write(f"{data}\n")
        f.write(f"{cdcline}\n")


def do_remove():
    run(
        [
            "git",
            "config",
            "--local",
            "--unset",
            "filter.git_fastcdc.process",
        ],
    )
    run(
        [
            "git",
            "config",
            "--local",
            "--unset",
            "filter.git_fastcdc.required",
        ],
    )
    file = Path(".gitattributes")
    file.touch()
    with file.open("r", encoding="UTF-8") as f:
        data = f.read()
    with file.open("w", encoding="UTF-8") as f:
        for line in data.splitlines():
            if cdcline not in line:
                f.write(f"{line}\n")


@cli.command()
def remove():
    """Remove fastcdc from the current repository."""
    do_remove()
