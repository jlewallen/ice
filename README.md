# Ice

I have lots of large media files that rarely change and I'd like to keep for archival purposes. Things like raw video and pictures straight from cameras. After these have been through some kind of post-production they tend to just sit around and collect digital dust. The idea behind Ice is to upload these to Amazon Glacier for storage.

I evaluated a few other tools (bakthat, for example) and couldn't find the combination of features that I'd like. The main premise is that the files this tool is uploading will never change, and so it's possible to take advantage of this immutability and simplify some of the backup. No need to do rotations, etc...  I also wanted something that allowed finer grained control of grouping, aggregation because the data itself will likely have very specific access patterns which can be important when using Glacier.

## Features

1. Archival data is stored in Glacier.
1. Meta data is stored in S3.
1. All data is encrypted client-side using Blowfish.


## How It Works

Basically, archives are generated using various strategies. My media has some logical groupings defined by the directory structure and so those are used to create archives that would mirror any future access pattern.

## Also...

I actually haven't used it yet :) I've still got some money left in my Tarsnap account and I'm waiting until that runs out before I switch over.
