import sys
from pathlib import Path
from subprocess import PIPE, run

import click
from fastcdc import fastcdc

read = sys.stdin.buffer.read
buffer = sys.stdout.buffer
write = buffer.write
flush = buffer.flush
tmpfile1 = Path(".fast_cdc_tmp_file_29310b6")
tmpfile2 = Path(".fast_cdc_tmp_file_0c2a0b9")
cdcdir = Path(".cdc")


def eprint(*args):
    print(*args, file=sys.stderr)


@click.group()
def cli():
    pass


def read_pkt_line():
    length_hex = read(4)
    if not length_hex:
        return b""

    length = int(length_hex, 16)
    if length == 0:
        return b""

    return read(length - 4)


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


def git_hash_blob(file):
    return run(
        ["git", "hash-object", "-w", "-t", "blob", file],
        check=True,
        stdout=PIPE,
        encoding="UTF-8",
    ).stdout.strip()


def git_get_blob(hash):
    return run(
        ["git", "cat-file", "blob", hash],
        check=True,
        stdout=PIPE,
    ).stdout


def hash_dir(base, hash):
    dir = base / hash[0:2] / hash[2:4]
    dir.mkdir(parents=True, exist_ok=True)
    return dir / hash


def chunk_string(input_string, chunk_size=65516):
    return [
        input_string[i : i + chunk_size]
        for i in range(0, len(input_string), chunk_size)
    ]


def clean(pathname):
    try:
        with tmpfile1.open("wb") as f:
            while pkg := read_pkt_line():
                f.write(pkg)
        write_pkt_line_str("status=success\n")
        flush_pkt()
        with tmpfile1.open("rb") as f:
            for cdc in fastcdc(str(tmpfile1), avg_size=256 * 1024):
                f.seek(cdc.offset)
                with tmpfile2.open("wb") as w:
                    w.write(f.read(cdc.length))
                path = hash_dir(cdcdir, git_hash_blob(tmpfile2))
                with tmpfile2.open("w") as w:
                    w.write(path.name)
                tmpfile2.rename(path)
                write_pkt_line_str(f"{path.name}\n")
        flush_pkt()
        flush_pkt()
    finally:
        tmpfile1.unlink()


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


@cli.command()
def process():
    """Called by git to do fastcdc."""
    assert read_pkt_line_str() == "git-filter-client"
    assert read_pkt_line_str() == "version=2"
    write_pkt_line_str("git-filter-server")
    write_pkt_line_str("version=2")
    flush_pkt()
    # write_pkt_line_str("capability=clean\ncapability=smudge\n")
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
        if command == "smudge":
            key, _, blob = read_pkt_line_str().partition("=")
            assert key == "blob"
        assert read_pkt_line_str() == ""
        if command == "clean":
            if str(pathname).startswith(".cdc/"):
                clean_cdc(pathname)
            else:
                clean(pathname)
        elif command == "smudge":
            if str(pathname).startswith(".cdc/"):
                smudge_cdc(pathname, blob)
            else:
                smudge(pathname, blob)


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


@cli.command()
def remove():
    """Remove fastcdc from the current repository."""
    do_remove()
