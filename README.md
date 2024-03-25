# git-fastcdc

Split certain files using content defined chunking for faster deduplication.

## Enable

```bash
git fastcdc install
```

## Config

Edit .gitattributes:

```
*.wav binary filter=git_fastcdc
/.cdc/** binary filter=git_fastcdc
```

## How

It will split files on filtering when you add them. The split files got into
`.cdc`. The files in `.cdc` are filtered too, so they don't use up much space.

You will see the actual data in the files in the working copy, in `*.wav` in the
example above. But actually the blobs of these files are just a list of chunks.
The blobs stored in git are actually in `.cdc`, but since these are not
interesting, another filter will remove them from the working copy.
