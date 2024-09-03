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

## Repository Status: Personal

This repository hosts a project that is actively maintained but primarily
intended for my personal use. It is public for transparency, sharing ideas, and
as a resource for others who might find the methodologies or implementations
useful. Please consider the following:

- **Status change**: Should there be significant interest in this project, I am
  open to changing its status to accommodate broader collaboration and
  development.
- **Personal Project**: This is a personal project, and while it is actively
  maintained, it is tailored to my specific needs and use cases.
- **Limited Support**: Given the personal nature of this project, support and
  responses to issues or pull requests might be limited. I encourage open
  collaboration but may prioritize changes that align with my personal use.
- **Viewing and Forking Encouraged**: You are welcome to view, fork, or use the
  code in your own projects. However, this project is provided as-is, with no
  guarantees of regular updates or adaptations for broader use.
- **Contribution Guidelines**: While contributions are appreciated, they should
  be relevant and beneficial to the project’s ongoing development. Please review
  any provided contribution guidelines before making pull requests.

Feel free to explore the code, and utilize it under the terms of the license
attached to this repository!
