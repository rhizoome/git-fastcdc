# git-fastcdc

Split certain files using content-defined-chunking for faster deduplication. It
has a similar use-case to git-lfs, but blobs are in-repository. git-fastcdc
mitigates some of the speed penalties. For most use-cases you are probably
better off with git-lfs. If you have a focus on archival and deduplication, git-
fastcdc might right for you.

## Enable

```bash
git fastcdc install
```

## Config

Edit .gitattributes:

```
*.wav binary filter=git_fastcdc
/.gitattributes text -binary -filter
/.gitignore text -binary -filter
```

By default git-fastcdc runs in-memory. Switch to on-disk:

```bash
git config --local fastcdc.ondisk true
```

If you have a pure git-fastcdc repository, you probably want to disable delta-compression 
to benefit from the speedups through fastcdc.

```bash
git fastcdc delta disable
```

Which will set `core.bigFileThreshold` to `200k` which isn't exect science. It
means most of the history- and meta-data is delta-compressed while most of the
cdc-blobs aren't.

## Results

For my repository - 800GB of music collection:

- Without git-fastcdc delta-compression took over 5 hours (actually it took all
  night)
- With git-fastcdc delta-compression takes about 2 minutes
- With git-fastcdc the repostiory got slightly smaller: about 1%

So much faster repack, with the same delta-compression.

Methodology: I took one state of my repostory from 2 years ago and one state
from today. A lot of meta-data has changed in those two states, because I am
constantly fixing these using beaTunes. In both tests I created two commits
and did `reapck -a -d -f` at the end.

## How

It will split files on filtering when you add them. The split files go into
the `git-fastcdc` branch. You need to push this branch to remotes too!

You will see the actual data in the files in the working copy, in `*.wav` in the
example above. But actually the blobs of these files are just a list of chunks.
