import os
import shlex
import sys
from fnmatch import fnmatch
from io import BytesIO
from pathlib import Path
from subprocess import DEVNULL, PIPE, CalledProcessError, run

import click
from fastcdc import fastcdc

read = sys.stdin.buffer.read
buffer = sys.stdout.buffer
write = buffer.write
flush = buffer.flush
tmpfile = Path(".fast_cdc_tmp_file_29310b6")
cdcbranch = "git-fastcdc"
cdcdir = Path(".cdc")
cdcattr = "/.gitattributes text -binary -filter"
gitempty = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
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
    return read_pkt_line().decode(encoding="UTF-8").strip()


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
        .stdout.decode(encoding="UTF-8")
        .strip()
    )


def git_mktree(tree):
    return run(
        ["git", "mktree"],
        stdout=PIPE,
        input=tree,
        encoding="UTF-8",
        check=True,
    ).stdout.strip()


def git_toplevel():
    return run(
        ["git", "rev-parse", "--show-toplevel"],
        encoding="UTF-8",
        stdout=PIPE,
        check=True,
    ).stdout.strip()


def git_ls_files():
    return run(
        ["git", "ls-files"],
        check=True,
        encoding="UTF-8",
        stdout=PIPE,
    ).stdout.strip()


def git_ls_tree(rev):
    return run(
        ["git", "ls-tree", rev],
        check=True,
        encoding="UTF-8",
        stdout=PIPE,
    ).stdout.strip()


def git_get_blob(hash):
    return run(
        ["git", "cat-file", "blob", hash],
        check=True,
        stdout=PIPE,
    ).stdout


def git_config_ondisk():
    return run(
        ["git", "config", "--local", "--get", "fastcdc.ondisk"],
        stdout=PIPE,
    ).stdout


def git_show(rev):
    return run(
        ["git", "show", rev],
        check=True,
        stderr=DEVNULL,
        stdout=PIPE,
    ).stdout.strip()


def git_rev_parse(rev):
    return run(
        ["git", "rev-parse", rev],
        check=True,
        stderr=DEVNULL,
        encoding="UTF-8",
        stdout=PIPE,
    ).stdout.strip()


def git_add(*args):
    run(["git", "add"] + list(args), check=True)


def git_branch(branch, commit, force=False):
    if force:
        run(["git", "branch", "-f", branch, commit], check=True)
    else:
        run(["git", "branch", branch, commit], check=True)


def git_commit_tree(hash, *args):
    return run(
        ["git", "commit-tree", hash] + list(args),
        stdout=PIPE,
        encoding="UTF-8",
        check=True,
    ).stdout.strip()


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


def clean(pathname, cdcs):
    io = BytesIO()
    while pkg := read_pkt_line():
        io.write(pkg)
    io.seek(0)
    size = io.getbuffer().nbytes
    avg_size = max(avg_min, get_avg_size(size))
    write_pkt_line_str("status=success\n")
    flush_pkt()
    buffer = io.getbuffer()
    for cdc in fastcdc(io, avg_size=avg_size):
        data = buffer[cdc.offset : cdc.offset + cdc.length]
        hash = git_hash_blob(data)
        cdcs.add(hash)
        write_pkt_line_str(f"{hash}.cdc\n")
    flush_pkt()
    flush_pkt()


def clean_ondisk(pathname, cdcs):
    try:
        with tmpfile.open("wb") as f:
            while pkg := read_pkt_line():
                f.write(pkg)
            f.seek(0)
        size = tmpfile.stat().st_size
        avg_size = max(avg_min, get_avg_size(size))
        write_pkt_line_str("status=success\n")
        flush_pkt()
        with tmpfile.open("rb") as f:
            for cdc in fastcdc(str(tmpfile), avg_size=avg_size):
                f.seek(cdc.offset)
                data = f.read(cdc.length)
                hash = git_hash_blob(data)
                cdcs.add(hash)
                write_pkt_line_str(f"{hash}.cdc\n")
            flush_pkt()
            flush_pkt()

    finally:
        tmpfile.unlink()


def smudge(pathname, blob):
    lines = []
    while pkg := read_pkt_line_str():
        lines.extend(pkg.splitlines())
    write_pkt_line_str("status=success\n")
    flush_pkt()
    for line in lines:
        line = line.strip()
        hash = Path(line).stem
        if line:
            blob = git_get_blob(hash)
            for chunk in chunk_string(blob):
                write_pkt_line(chunk)
    flush_pkt()
    flush_pkt()


_ondisk = None


def ondisk():
    global _ondisk
    if _ondisk is None:
        _ondisk = git_config_ondisk().strip() == b"true"
    return _ondisk


def read_cdcs():
    cdcs = set()
    try:
        git_rev_parse(cdcbranch)
    except CalledProcessError:
        return cdcs
    for line in git_ls_tree(cdcbranch).splitlines():
        _, _, rest = line.partition(" blob ")
        hash, _, _ = rest.partition("\t")
        cdcs.add(hash.strip())
    return cdcs


def write_cdcs(cdcs):
    commit = None
    if not cdcs:
        old_tree = git_rev_parse(f"{cdcbranch}^{{tree}}")
        hash = gitempty
    else:
        tree = []
        append = tree.append
        for cdc in cdcs:
            append(f"100644 blob {cdc}\t{cdc}.cdc")
        tree = "\n".join(tree)
        hash = git_mktree(tree)
    parent = None
    try:
        parent = git_rev_parse(cdcbranch)
    except CalledProcessError:
        pass
    if not parent:
        if not commit:
            commit = git_commit_tree(hash, "-m", "cdc")
        git_branch(cdcbranch, commit)
    else:
        old_tree = git_rev_parse(f"{cdcbranch}^{{tree}}")
        if old_tree != hash:
            if not commit:
                commit = git_commit_tree(hash, "-m", "cdc", "-p", parent)
            git_branch(cdcbranch, commit, force=True)


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
    new = False
    cdcs = read_cdcs()
    old_cdcs = set(cdcs)
    while line := read_pkt_line_str():
        key, _, command = line.partition("=")
        assert key == "command"
        key, _, pathname = read_pkt_line_str().partition("=")
        pathname = Path(pathname)
        assert key == "pathname"
        blob = None
        while line := read_pkt_line_str():
            key, _, value = line.partition("=")
            if key == "blob":
                blob = value
        if command == "clean":
            if ondisk():
                new = clean_ondisk(pathname, cdcs) or new
            else:
                new = clean(pathname, cdcs) or new
        elif command == "smudge":
            smudge(pathname, blob)
    if old_cdcs != cdcs:
        write_cdcs(cdcs)


def read_blobs(entry, cdcs):
    if not Path(entry).exists():
        return
    git_add(entry)
    for blob in git_show(f":{entry}").decode(encoding="UTF-8").splitlines():
        if fnmatch(blob, "*.cdc"):
            cdcs.add(Path(blob).stem)


@cli.command()
def prune():
    """Prune fastcdc objects."""
    file = Path(".gitattributes")
    file_list = []
    for entry in git_ls_files().splitlines():
        entry = entry.strip()
        if ".gitattributes" not in entry:
            file_list.append(entry)
    old_cdcs = read_cdcs()
    cdcs = set()
    if file.exists():
        with file.open("r", encoding="UTF-8") as f:
            for line in f:
                if "filter=git_fastcdc" in line:
                    match = shlex.split(line)[0]
                    for entry in file_list:
                        if fnmatch(entry, match):
                            read_blobs(entry, cdcs)
    if old_cdcs != cdcs:
        write_cdcs(cdcs)


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
        f.write(f"{cdcattr}\n")


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
            if cdcattr not in line:
                f.write(f"{line}\n")


@cli.command()
def remove():
    """Remove fastcdc from the current repository."""
    do_remove()
