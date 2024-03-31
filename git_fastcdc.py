import os
import shlex
import sys
import time
from fnmatch import fnmatch
from io import BytesIO
from pathlib import Path
from subprocess import DEVNULL, PIPE, CalledProcessError, Popen, run
from tempfile import NamedTemporaryFile

import click
from fastcdc import fastcdc  # type: ignore
from tqdm import tqdm

read = sys.stdin.buffer.read
buffer = sys.stdout.buffer
write = buffer.write
flush = buffer.flush
cdcbranch = "git-fastcdc"
cdcattr = "/.gitattributes text -binary -filter"
cdcignore = "/.gitignore text -binary -filter"
avg_min = 256 * 1024
pkt_size = 65516

# Helpers


def eprint(*args):
    print(*args, file=sys.stderr)


def chunk_seq(input_string, chunk_size):
    return [
        input_string[i : i + chunk_size]
        for i in range(0, len(input_string), chunk_size)
    ]


def make_hint(pathname):
    hint = f"{pathname.parent}_{pathname.stem}"
    hint = hint.replace("/", "")
    hint = hint.replace("-", "_")
    hint = hint.strip(".")
    hint = hint.strip("_")
    hint = hint.strip(".")
    hint = hint.strip("_")
    return hint


_ondisk: bool | None = None


def ondisk():
    global _ondisk
    if _ondisk is None:
        _ondisk = git_config_ondisk().strip() == b"true"
    return _ondisk


# git pkt-line


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
    return read_pkt_line().decode("UTF-8").strip()


def write_pkt_line(data):
    length = len(data) + 4
    write(f"{length:04x}".encode("UTF-8") + data)
    flush()


def write_pkt_line_str(string):
    write_pkt_line(string.encode("UTF-8"))


def flush_pkt():
    write(b"0000")
    flush()


# git cat-file batch

_batch: Popen | None = None
_batch_cleanup_times = (0.001, 0.01, 0.1, 0.2, 0.4)


def git_cat_batch():
    global _batch
    if _batch is None:
        _batch = Popen(["git", "cat-file", "--batch"], stdout=PIPE, stdin=PIPE)
    return _batch


def git_cat_get(entry, stdin, stdout):
    stdin.write(f"{entry}\n".encode("UTF-8"))
    stdin.flush()
    line = stdout.readline().decode("UTF-8").strip()
    _, _, data_len = line.rpartition(" ")
    data = stdout.read(int(data_len))
    stdout.readline()
    return data


def git_cat_yield(entry, stdin, stdout, chunk_size):
    stdin.write(f"{entry}\n".encode("UTF-8"))
    stdin.flush()
    line = stdout.readline().decode("UTF-8").strip()
    _, _, data_len = line.rpartition(" ")
    data_len = int(data_len)
    while data_len > 0:
        if data_len >= chunk_size:
            yield stdout.read(chunk_size)
            data_len -= chunk_size
        else:
            yield stdout.read(data_len)
            data_len = 0
    stdout.readline()


def batch_cleanup():
    global _batch
    if _batch:
        proc = _batch
        proc.stdin and proc.stdin.close()
        proc.stdout and proc.stdout.close()
        for sleep_time in _batch_cleanup_times:
            if proc.poll() is not None:
                break
            time.sleep(sleep_time)
        if proc.poll() is None:
            proc.terminate()
            time.sleep(1)
            if proc.poll() is None:
                eprint("error: needed to kill subprocess()")
                proc.kill()
        _batch = None


# git cli helpers


def git_hash_blob(data):
    return (
        run(
            ["git", "hash-object", "-w", "-t", "blob", "--stdin"],
            check=True,
            stdout=PIPE,
            input=data,
        )
        .stdout.decode("UTF-8")
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


def git_config_ondisk():
    return run(
        ["git", "config", "--local", "--get", "fastcdc.ondisk"],
        stdout=PIPE,
    ).stdout


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


# clean


def get_avg_size(size):
    box = int(size / 16)
    bits = box.bit_length()
    shift = max(bits - 4, 0)
    avg_size = (box >> shift) << shift
    avg_size = max(avg_min, avg_size)
    return avg_size


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
    with NamedTemporaryFile(
        "r+b", dir=".", prefix=".fast_cdc_tmp_file_", suffix=".tmp"
    ) as f:
        while pkg := read_pkt_line():
            f.write(pkg)
        f.flush()
        size = f.tell()
        avg_size = get_avg_size(size)
        write_pkt_line_str("status=success\n")
        flush_pkt()
        for cdc in fastcdc(str(f.name), avg_size=avg_size):
            f.seek(cdc.offset)
            data = f.read(cdc.length)
            hash = git_hash_blob(data)
            cdcs.add(hash)
            base_hints[hash] = make_hint(pathname)
            write_pkt_line_str(f"{hash}.cdc\n")
        flush_pkt()
        flush_pkt()


# smudge


def smudge(pathname):
    pkgs = []
    while pkg := read_pkt_line():
        pkgs.append(pkg)
    data = b"".join(pkgs).decode("UTF-8")
    write_pkt_line_str("status=success\n")
    flush_pkt()
    proc = git_cat_batch()
    stdin = proc.stdin
    stdout = proc.stdout
    for line in data.splitlines():
        line = line.strip()
        hash = Path(line).stem
        if line:
            for chunk in git_cat_yield(hash, stdin, stdout, pkt_size):
                write_pkt_line(chunk)
    flush_pkt()
    flush_pkt()


# reading state history


def read_trees(branch, rev_limit=None):
    try:
        git_rev_parse(branch)
    except CalledProcessError:
        return
    proc = git_cat_batch()
    stdin = proc.stdin
    stdout = proc.stdout
    for rev in git_rev_list(branch, limit=rev_limit).splitlines():
        yield git_cat_get(f"{rev}^{{tree}}", stdin, stdout)


def parse_git_tree(binary_tree):
    entries = []
    i = 0
    while i < len(binary_tree):
        space_index = binary_tree.index(b" ", i)
        mode = binary_tree[i:space_index].decode("ascii")

        null_index = binary_tree.index(b"\0", space_index)
        filename = binary_tree[space_index + 1 : null_index].decode("UTF-8")

        sha1 = binary_tree[null_index + 1 : null_index + 21]
        sha1_hex = sha1.hex()

        entries.append((mode, filename, sha1_hex))
        i = null_index + 21

    return entries


def read_recent():
    cdcs = set()
    for tree in read_trees(cdcbranch, rev_limit=10):
        for _, filename, hash in parse_git_tree(tree):
            _, _, ext = filename.rpartition(".")
            if ext == "cdc":
                cdcs.add(hash)
    return cdcs


def read_cdcs():
    base_hints: dict[str, str] = {}
    cdcs: set[str] = set()
    try:
        git_rev_parse(cdcbranch)
    except CalledProcessError:
        return cdcs, base_hints
    for tree in tqdm(read_trees(cdcbranch), desc="read revions", delay=2):
        for _, filename, hash in parse_git_tree(tree):
            rest, _, ext = filename.rpartition(".")
            hint, _, _ = filename.rpartition("-")
            if ext == "cdc":
                if hint:
                    base_hints[hash] = hint
                cdcs.add(hash)
    return cdcs, base_hints


# reading current state


def read_blobs(entry, stdin, stdout, cdcs, base_hints):
    entry = Path(entry)
    data = git_cat_get(f":{entry}", stdin, stdout)
    try:
        data = data.decode("UTF-8")
    except UnicodeDecodeError:
        # No data for us, we wrote UTF-8
        return
    for blob in data.splitlines():
        if fnmatch(blob, "*.cdc"):
            hash = Path(blob).stem
            base_hints[hash] = make_hint(entry)
            if len(hash) == 40:
                cdcs.add(hash)


# writing state history


def write_cdcs(cdcs, base_hints, no_progress=True):
    trees = []
    for chunk in chunk_seq(list(cdcs), chunk_size=1500):
        tree: list[str] = []
        append = tree.append
        for cdc in chunk:
            hint = base_hints.get(cdc)
            if hint:
                append(f"100644 blob {cdc}\t{hint}-{cdc}.cdc")
            else:
                append(f"100644 blob {cdc}\t{cdc}.cdc")
        attrs = git_hash_blob(b"*.cdc binary")
        append(f"100644 blob {attrs}\t.gitattributes")
        trees.append("\n".join(tree))

    commit = None
    try:
        commit = git_rev_parse(cdcbranch)
    except CalledProcessError:
        pass
    force = commit is not None
    for tree_str in tqdm(trees, desc="write trees", delay=2, disable=no_progress):
        hash = git_mktree(tree_str)
        if not commit:
            commit = git_commit_tree(hash, "-m", "cdc")
        else:
            commit = git_commit_tree(hash, "-m", "cdc", "-p", commit)

    git_branch(cdcbranch, commit, force=force)


# Cli


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


@click.group()
def cli():
    os.chdir(git_toplevel())


# cli actions


@cli.command()
def process():
    """Called by git to do fastcdc."""
    try:
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
        cdcs: set[str] = set()
        cdcs_recent = read_recent()
        base_hints: dict[str, str] = {}
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
    finally:
        batch_cleanup()


@cli.command()
def update():
    """Update fastcdc objects-index from current files."""
    file = Path(".gitattributes")
    if not file.exists():
        return
    file_list = git_ls_files().splitlines()
    cdcs_log, base_hints = read_cdcs()
    cdcs: set[str] = set()
    check_files = set()
    with file.open("r", encoding="UTF-8") as f:
        for line in f:
            if "filter=git_fastcdc" in line:
                match = shlex.split(line)[0]
                for entry in file_list:
                    if fnmatch(entry, match):
                        check_files.add(entry)
    proc = None
    try:
        proc = git_cat_batch()
        stdin = proc.stdin
        stdout = proc.stdout
        for entry in tqdm(list(check_files), desc="read files", delay=2):
            read_blobs(entry, stdin, stdout, cdcs, base_hints)
    finally:
        batch_cleanup()
    to_write = cdcs - cdcs_log
    if to_write:
        write_cdcs(to_write, base_hints, no_progress=False)


# cli config


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


@cli.command()
def remove():
    """Remove fastcdc from the current repository."""
    do_remove()


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


@cli.group()
def delta():
    """Enable/disable delta-compression."""
    pass


@delta.command(name="enable")
def enable_delta():
    """Enable delta-compression."""
    run(
        [
            "git",
            "config",
            "--unset",
            "core.bigFileThreshold",
        ],
    )


@delta.command()
def disable_delta(name="disable"):
    """Disable delta-compression."""
    run(
        [
            "git",
            "config",
            "--local",
            "core.bigFileThreshold",
            "200k",
        ],
        check=True,
    )


@cli.group(name="ondisk")
def ondisk_cli():
    """Enable/disable ondisk-fastdcd."""
    pass


@ondisk_cli.command(name="enable")
def enable_ondisk():
    """Enable ondisk-fastcdc."""
    run(
        [
            "git",
            "config",
            "--local",
            "fastcdc.ondisk",
            "true",
        ],
        check=True,
    )


@ondisk_cli.command(name="disable")
def disable_ondisk():
    """Disable ondisk-fastcdc."""
    run(
        [
            "git",
            "config",
            "--unset",
            "fastcdc.ondisk",
        ],
    )
