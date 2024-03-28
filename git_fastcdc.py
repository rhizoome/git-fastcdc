import os
import shlex
import sys
from fnmatch import fnmatch
from io import BytesIO
from pathlib import Path
from subprocess import DEVNULL, PIPE, CalledProcessError, run

import click
from fastcdc import fastcdc
from tqdm import tqdm

read = sys.stdin.buffer.read
buffer = sys.stdout.buffer
write = buffer.write
flush = buffer.flush
tmpfile = Path(".fast_cdc_tmp_file_29310b6")
cdcbranch = "git-fastcdc"
cdcdir = Path(".cdc")
cdcattr = "/.gitattributes text -binary -filter"
cdcignore = "/.gitignore text -binary -filter"
gitempty = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
avg_min = 256 * 1024


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


def git_rev_list(rev, limit=None):
    if limit:
        n = ["-n", str(limit)]
    else:
        n = []
    return run(
        ["git", "rev-list"] + n + [rev],
        stdout=PIPE,
        encoding="UTF-8",
        check=True,
    ).stdout.strip()


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


def chunk_seq(input_string, chunk_size=65516):
    return [
        input_string[i : i + chunk_size]
        for i in range(0, len(input_string), chunk_size)
    ]


def get_avg_size(size):
    box = int(size / 16)
    bits = box.bit_length()
    shift = max(bits - 4, 0)
    avg_size = (box >> shift) << shift
    avg_size = max(avg_min, avg_size)
    return avg_size


def make_hint(pathname):
    hint = f"{pathname.parent}_{pathname.stem}"
    hint = hint.replace("/", "")
    hint = hint.replace("-", "_")
    hint = hint.strip(".")
    hint = hint.strip("_")
    hint = hint.strip(".")
    hint = hint.strip("_")
    return hint


def clean(pathname, cdcs, base_hints):
    io = BytesIO()
    while pkg := read_pkt_line():
        io.write(pkg)
    io.seek(0)
    size = io.getbuffer().nbytes
    avg_size = get_avg_size(size)
    write_pkt_line_str("status=success\n")
    flush_pkt()
    buffer = io.getbuffer()
    for cdc in fastcdc(io, avg_size=avg_size):
        data = buffer[cdc.offset : cdc.offset + cdc.length]
        hash = git_hash_blob(data)
        cdcs.add(hash)
        base_hints[hash] = make_hint(pathname)
        write_pkt_line_str(f"{hash}.cdc\n")
    flush_pkt()
    flush_pkt()


def clean_ondisk(pathname, cdcs, base_hints):
    try:
        with tmpfile.open("wb") as f:
            while pkg := read_pkt_line():
                f.write(pkg)
            f.seek(0)
        size = tmpfile.stat().st_size
        avg_size = get_avg_size(size)
        write_pkt_line_str("status=success\n")
        flush_pkt()
        with tmpfile.open("rb") as f:
            for cdc in fastcdc(str(tmpfile), avg_size=avg_size):
                f.seek(cdc.offset)
                data = f.read(cdc.length)
                hash = git_hash_blob(data)
                cdcs.add(hash)
                base_hints[hash] = make_hint(pathname)
                write_pkt_line_str(f"{hash}.cdc\n")
            flush_pkt()
            flush_pkt()

    finally:
        tmpfile.unlink()


def smudge(pathname):
    pkgs = []
    while pkg := read_pkt_line():
        pkgs.append(pkg)
    data = b"".join(pkgs).decode(encoding="UTF-8")
    write_pkt_line_str("status=success\n")
    flush_pkt()
    for line in data.splitlines():
        line = line.strip()
        hash = Path(line).stem
        if line:
            blob = git_get_blob(hash)
            for chunk in chunk_seq(blob):
                write_pkt_line(chunk)
    flush_pkt()
    flush_pkt()


_ondisk = None


def ondisk():
    global _ondisk
    if _ondisk is None:
        _ondisk = git_config_ondisk().strip() == b"true"
    return _ondisk


def read_recent():
    cdcs = set()
    try:
        git_rev_parse(cdcbranch)
    except CalledProcessError:
        return cdcs
    for rev in git_rev_list(cdcbranch, limit=10).splitlines():
        for line in git_ls_tree(rev).splitlines():
            _, _, rest = line.partition(" blob ")
            hash, _, _ = rest.partition("\t")
            cdcs.add(hash)
    return cdcs


def read_cdcs():
    base_hints = {}
    cdcs = set()
    try:
        git_rev_parse(cdcbranch)
    except CalledProcessError:
        return cdcs, base_hints
    for rev in tqdm(git_rev_list(cdcbranch).splitlines(), delay=10):
        for line in git_ls_tree(rev).splitlines():
            _, _, rest = line.partition(" blob ")
            hash, _, rest = rest.partition("\t")
            hint, _, _ = rest.rpartition("-")
            if hint:
                base_hints[hash] = hint
            cdcs.add(hash)
    return cdcs, base_hints


def write_cdcs(cdcs, base_hints):
    commit = None
    parent = None
    try:
        parent = git_rev_parse(cdcbranch)
    except CalledProcessError:
        pass
    if not cdcs:
        old_tree = git_rev_parse(f"{cdcbranch}^{{tree}}")
        hash = gitempty
    else:
        tree = []
        append = tree.append
        for chunk in chunk_seq(list(cdcs), chunk_size=1000):
            for cdc in chunk:
                hint = base_hints.get(cdc)
                if hint:
                    append(f"100644 blob {cdc}\t{hint}-{cdc}.cdc")
                else:
                    append(f"100644 blob {cdc}\t{cdc}.cdc")
        attrs = git_hash_blob(b"*.cdc binary")
        append(f"100644 blob {attrs}\t.gitattributes")
        tree = "\n".join(tree)
        hash = git_mktree(tree)
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
    cdcs = set()
    cdcs_recent = read_recent()
    base_hints = {}
    write = False
    while line := read_pkt_line_str():
        key, _, command = line.partition("=")
        assert key == "command"
        key, _, pathname = read_pkt_line_str().partition("=")
        pathname = Path(pathname)
        assert key == "pathname"
        while line := read_pkt_line_str():
            pass
            # key, _, value = line.partition("=")
        if command == "clean":
            write = True
            if ondisk():
                clean_ondisk(pathname, cdcs, base_hints)
            else:
                clean(pathname, cdcs, base_hints)
        elif command == "smudge":
            smudge(pathname)
    if write:
        to_write = cdcs - cdcs_recent
        if to_write:
            write_cdcs(to_write, base_hints)


def read_blobs(entry, cdcs, base_hints):
    entry = Path(entry)
    if not entry.exists():
        return
    for blob in git_show(f":{entry}").decode(encoding="UTF-8").splitlines():
        if fnmatch(blob, "*.cdc"):
            hash = Path(blob).stem
            base_hints[hash] = make_hint(entry)
            if len(hash):
                cdcs.add(hash)


@cli.command()
def update():
    """Update fastcdc objects-index from current files."""
    file = Path(".gitattributes")
    if not file.exists():
        return
    file_list = git_ls_files().splitlines()
    cdcs_log, base_hints = read_cdcs()
    cdcs = set()
    check_files = set()
    with file.open("r", encoding="UTF-8") as f:
        for line in f:
            if "filter=git_fastcdc" in line:
                match = shlex.split(line)[0]
                for entry in file_list:
                    if fnmatch(entry, match):
                        check_files.add(entry)
    for entry in tqdm(list(check_files), delay=10):
        read_blobs(entry, cdcs, base_hints)
    to_write = cdcs - cdcs_log
    if to_write:
        write_cdcs(to_write, base_hints)


@cli.command()
def useful_config():
    """Set useful config on the repository. no auto gc and no loose compression."""

    run(
        [
            "git",
            "config",
            "--local",
            "gc.auto",
            "0",
        ],
        check=True,
    )
    run(
        [
            "git",
            "config",
            "--local",
            "core.looseCompression",
            "0",
        ],
        check=True,
    )


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
        f.write(f"{cdcignore}\n")


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
            if cdcattr not in line and cdcignore not in line:
                f.write(f"{line}\n")


@cli.command()
def remove():
    """Remove fastcdc from the current repository."""
    do_remove()
