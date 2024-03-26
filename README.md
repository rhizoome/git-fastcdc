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

## How

It will split files on filtering when you add them. The split files go into
the `git-fastcdc` branch. You need to push this branch to remotes too!

You will see the actual data in the files in the working copy, in `*.wav` in the
example above. But actually the blobs of these files are just a list of chunks.
