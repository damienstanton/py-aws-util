# py-aws-util
This software is distrubted under the MIT license. It will not be actively developed further.

A module for data scientists to more easily manage S3 and EMR cluster resources on AWS

### Environment

- Python 3.7.5 and 3.8.1 are tested. Requires a virtualenv for development & testing.

### Tests

- Run `run_tests.sh` in a venv that has `pytest` installed.

### Sub-modules

- [cluster_manager](#cluster-manager)
- [s3_manager](#s3-manager)
- [s3_transform](#s3-transform)

## Cluster Manager

Abstractions for operating EMR clusters and their steps from Python

```py
# Import and instantiate the manager
from cluster_manager import ClusterManager

cmgr = ClusterManager("my_emr_logpath", "my_ec2_key")

# Launch a job with a given name
my_cluster_id = cmgr.launch_cluster("my_job_name")

# Now my_cluster_id can be used in logging/monitoring.
# This cluster will run until terminated via:
cmgr.terminate_cluster(my_cluster_id)

# One may instead want to run a cluster with a given
# job, which will automatically self-terminate once
# the job has completed. The job is constructed by
# passing in the Java/Scala jarfile to be run by
# EMR, along with the jarfile arguments:
cmgr.launch_cluster_with_jobs("path_to_spark.jar", [("my_class", ["arg0", "arg1"])])

# If a cluster is already running, one can attach jobs
# in a similar way, returning a list of step IDs:
my_steps = cmgr.run_steps(
    "path_to_spark.jar",
    "my_cluster_id",
    [("my_class", ["arg0", "arg1"])]
)

# Cluster job state can be managed in both a blocking
# and non-blocking fashion:

# report_step blocks until the step completes, returning a
# completion state of COMPLETED, CANCELLED, FAILED,
# or INTERRUPTED
final_job_state = cmgr.report_step("my_step_id", "my_cluster_id")


# Alternatively, non-blocking watch_step can examine
# the job state and execute a callback depending on
# state changes, periodically printing the current state value.
# Valid states are one of PENDING, CANCEL_PENDING, RUNNING,
# COMPLETED, CANCELLED, FAILED, and INTERRUPTED
cmgr.watch_step(
    "my_step_id",
    "my_cluster_id",
    my_callback # void, with no parameters
    my_callback_arg # the state to watch
)

# Step status can be determined at any time via:
current_state, err = cmgr.step_status("my_step_id", "my_cluster_id")
# If errors occur, the second tuple value contains
# the error logs, automatically fetched from S3 and gunzipped.
# The error string is formatted to be human-readable in an
# ipython or jupyter notebook

# Similarly, cluster state can be examined, where the second
# tuple value contains the cluster's termination cause (if applicable)
cluster_state, reason = cmgr.cluster_status("my_cluster_id")
```

## S3 Manager

A wrapper around boto3 that includes an in-memory string buffer and error handling

```py
from s3_manager import Session

s3mgr = Session()

# Load a file from an S3 path into memory
my_file_str = s3mgr.get("s3://some-bucket/some_thing")

# List the contents of an S3 path, returning a fully-qualified
# path for each file in the bucket
all_files = s3mgr.list("s3://some-bucket")

# Passing a single file to delete deletes that file from the bucket
s3mgr.delete("s3://some-bucket/some_thing")
# while passing a directory uses a single session to recursively
# delete all objects in that S3 path
s3mgr.delete("s3://some-bucket")

# Move and copy ops work as expected
s3mgr.move("s3://source/thing", "s3://dest2/thing") # thing only in dest
s3mgr.copy("s3://source/thing", "s3://dest2/thing") # thing in both source & dest

# Paths can be checked for validity
path_is_good = s3mgr.path_exists("s3://some-bucket/some_thing")

# There is one module-level function that can be used independently
# from s3_manager, parse_path. This chops a given S3 path into a
# dictionary containing the full path, the bucket, and the key.
path_details = ("s3://some-bucket/some_thing")
```

## S3 Transform

Simple `pandas` <-> S3 transformation functions built on top of the S3 manager class

```py
# Not worth going into details, functions do
# what one would expect
from s3_transform import (
    read_csv_to_df,    
    read_json_to_df,
    write_df_to_json
    write_df_to_csv,
)
```
