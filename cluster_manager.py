"""
A module that controls the creation and shutdown of EMR clusters
"""
import os
import re
from gzip import decompress
from time import sleep
from typing import Callable
from multiprocessing import Process, Pipe
import boto3


class ClusterManager:
    """
    Methods for launching/terminating EMR clusters
    """

    def __init__(self, log_uri: str = None, name: str = None):
        """ Constructor for a ClusterManager instance"""
        self.log_uri = log_uri
        self._tx_poll, self._rx_poll = Pipe()
        self._poll_time = 60
        self._client = boto3.client("emr", region_name="us-east-1")
        self.instance_config = {
            "InstanceGroups": [
                {
                    "InstanceCount": 2,
                    "Market": "SPOT",
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [
                            {
                                "VolumeSpecification": {
                                    "SizeInGB": 32,
                                    "VolumeType": "gp2",
                                },
                                "VolumesPerInstance": 4,
                            }
                        ]
                    },
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.2xlarge",
                    "Name": "Core - 2",
                },
                {
                    "InstanceCount": 1,
                    "Market": "SPOT",
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [
                            {
                                "VolumeSpecification": {
                                    "SizeInGB": 32,
                                    "VolumeType": "gp2",
                                },
                                "VolumesPerInstance": 4,
                            }
                        ]
                    },
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.2xlarge",
                    "Name": "Master - 1",
                },
                {
                    "InstanceCount": 32,
                    "Market": "SPOT",
                    "EbsConfiguration": {
                        "EbsBlockDeviceConfigs": [
                            {
                                "VolumeSpecification": {
                                    "SizeInGB": 32,
                                    "VolumeType": "gp2",
                                },
                                "VolumesPerInstance": 4,
                            }
                        ]
                    },
                    "InstanceRole": "TASK",
                    "InstanceType": "m5.2xlarge",
                    "Name": "Task - 3",
                },
            ],
            "Ec2KeyName": name,
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False,
            "Ec2SubnetId": "subnet-e5e7c6ca",
            "EmrManagedMasterSecurityGroup": "sg-f6b581be",
            "EmrManagedSlaveSecurityGroup": "sg-fdc1f5b5",
        }

    def launch_cluster(self, name: str) -> str:
        """ Launches a new cluster, returning the (jobflow) cluster's ID """
        self.instance_config["KeepJobFlowAliveWhenNoSteps"] = True
        response = self._client.run_job_flow(
            Name=name,
            AutoScalingRole="EMR_AutoScaling_DefaultRole",
            Applications=[
                {"Name": "Hadoop"},
                {"Name": "Hive"},
                {"Name": "Pig"},
                {"Name": "Hue"},
                {"Name": "Spark"},
            ],
            EbsRootVolumeSize=10,
            ScaleDownBehavior="TERMINATE_AT_TASK_COMPLETION",
            Configurations=[
                {
                    "Classification": "spark",
                    "Properties": {"maximizeResourceAllocation": "true"},
                }
            ],
            LogUri=self.log_uri,
            ReleaseLabel="emr-5.27.0",
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            Instances=self.instance_config,
            VisibleToAllUsers=True,
        )

        res = ""
        try:
            res = response["JobFlowId"]
        except KeyError:
            print("-> JobFlowId key missing from {}".format(response))
        return res

    def launch_cluster_with_jobs(
        self,
        assembly_path: str,
        class_and_args: [(str, [str])],
        cluster_name: str = None,
    ) -> dict:
        """
        Launches a new cluster with the given EMR step, returning
        a dictionary where the key is the cluster ID and the value
        is the list of step IDs associated with the cluster.
        """
        res = dict()
        steps = []
        for pair in class_and_args:
            arg_list = [
                "spark-submit",
                "--conf",
                "spark.driver.maxResultSize=4g",
                "--packages",
                "org.apache.spark:spark-avro_2.11:2.4.4",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                "--class",
                pair[0],
                assembly_path,
            ]

            arg_list.extend(str(arg) for arg in pair[1])
            steps.append(
                {
                    "Name": os.path.splitext(pair[0])[1][1:],
                    "ActionOnFailure": "TERMINATE_CLUSTER",
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": arg_list},
                }
            )

        self.instance_config["KeepJobFlowAliveWhenNoSteps"] = False
        cluster_response = self._client.run_job_flow(
            Name="{}-m2x".format(cluster_name),
            AutoScalingRole="EMR_AutoScaling_DefaultRole",
            Applications=[
                {"Name": "Hadoop"},
                {"Name": "Hive"},
                {"Name": "Pig"},
                {"Name": "Hue"},
                {"Name": "Spark"},
            ],
            EbsRootVolumeSize=10,
            ScaleDownBehavior="TERMINATE_AT_TASK_COMPLETION",
            Configurations=[
                {
                    "Classification": "spark",
                    "Properties": {"maximizeResourceAllocation": "true"},
                }
            ],
            LogUri=self.log_uri,
            ReleaseLabel="emr-5.27.0",
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            Instances=self.instance_config,
            Steps=steps,
            VisibleToAllUsers=True,
        )

        cluster_id = ""
        try:
            cluster_id = cluster_response["JobFlowId"]
            res[cluster_id] = []
        except KeyError:
            print("-> JobFlowId key missing from {}".format(cluster_response))
        step_response = self._client.list_steps(ClusterId=cluster_id)
        for step in step_response["Steps"]:
            res[cluster_id].append(step["Id"])

        return res

    def terminate_cluster(self, cluster_id: str):
        """ Terminates a given cluster """
        self._client.terminate_job_flows(JobFlowIds=[cluster_id])
        print("-> Sent termination command for cluster: {}".format(cluster_id))

    def _poll(self, step_id: str, cluster_id: str):
        """ Polls the client for the current step state"""
        shutdown_states = ["COMPLETED", "CANCELLED", "FAILED", "INTERRUPTED"]
        while self.step_status(step_id, cluster_id)[0]["state"] not in shutdown_states:
            self._tx_poll.send(self.step_status(step_id, cluster_id)[0]["state"])
            sleep(self._poll_time)
        print(
            'Step {} has shutdown. See cmgr.step_status("{}", "{}")'.format(
                step_id, step_id, cluster_id
            )
        )
        self._tx_poll.close()

    def _listener(self, callback: Callable[[], None] = None, callback_arg: str = None):
        """ Listens for poll updates """
        while self._rx_poll.recv() != callback_arg:
            print(
                "-> Step in state {}, waiting for {}".format(
                    self._rx_poll.recv(), callback_arg
                )
            )
        callback()

    def report_step(self, step_id: str, cluster_id: str) -> str:
        """
        Polls the describe step API until the step reaches a shutdown state,
        then returns the final state (similar to "spark_status_on_completion")
        """
        shutdown_states = ["COMPLETED", "CANCELLED", "FAILED", "INTERRUPTED"]
        while self.step_status(step_id, cluster_id)[0]["state"] not in shutdown_states:
            sleep(self._poll_time)
        return self.step_status(step_id, cluster_id)[0]["state"]

    def watch_step(
        self,
        step_id: str,
        cluster_id: str,
        callback: Callable[[], None] = None,
        callback_arg: str = None,
    ):
        """
        Polls the given step for status, and depending on whether a callback func/arg
        are provided, runs the callback when the state matches the provided state.
        When the step is in one of a known set of shutdown states, the poll thread
        is shutdown.
        """
        proc = Process(target=self._poll, args=(step_id, cluster_id))
        proc.start()
        print(
            "-> Watching step {}, will execute your callback when state is {}".format(
                step_id, callback_arg
            )
        )
        print("-> Step status will be updated every {} seconds".format(self._poll_time))
        if callback is None:
            print("-> Step {} state: {}".format(step_id, self._rx_poll.recv()))
        else:
            valid_states = [
                "PENDING",
                "CANCEL_PENDING",
                "RUNNING",
                "COMPLETED",
                "CANCELLED",
                "FAILED",
                "INTERRUPTED",
            ]
            if callback_arg not in valid_states:
                print("-> Callback argument must be one of {}".format(valid_states))
                return
        listener = Process(target=self._listener, args=(callback, callback_arg))
        listener.start()

    def step_status(self, step_id: str, cluster_id: str) -> (dict, str):
        """
        Returns a simplified status dictionary of the given step and
        cluster ID. If the step ended in a failure, the stderr is
        automatically fetched from S3 and returned as the second
        element of the return tuple.
        """
        err = None
        response = self._client.describe_step(ClusterId=cluster_id, StepId=step_id)
        output = {
            "name": response["Step"]["Name"],
            "parameters": response["Step"]["Config"]["Args"],
            "failure_mode": response["Step"]["ActionOnFailure"],
            "state": response["Step"]["Status"]["State"],
        }

        if "FailureDetails" in response["Step"]["Status"]:
            try:
                reason = response["Step"]["Status"]["FailureDetails"]["Reason"]
            except KeyError:
                reason = "Unknown"
            output["failure_details"] = {
                "reason": reason,
                "logfile": response["Step"]["Status"]["FailureDetails"]["LogFile"],
            }
            logfile_path = output["failure_details"]["logfile"]
            logfile_re = re.match(r"s3:\/\/.*?\/", logfile_path).group()
            logfile_bucket = logfile_re.split("s3://")[1].strip("/")
            logfile_key = logfile_path.split(logfile_re)[1]
            if not logfile_key.endswith("stderr.gz"):
                logfile_key += "stderr.gz"
            s3_client = boto3.client("s3")

            try:
                data = s3_client.get_object(Bucket=logfile_bucket, Key=logfile_key)[
                    "Body"
                ]
                err = decompress(data.read()).decode("unicode-escape")
            except s3_client.exceptions.NoSuchKey:
                print(
                    'Log for step {step} not found. It likely has not yet been written.\nPlease try to check status again in a few minutes.\nReminder: Step ID is "{step}", Cluster ID is "{cluster}"'.format(
                        step=step_id, cluster=cluster_id
                    )
                )
        return output, err

    def cluster_status(self, cluster_id: str) -> (dict, str):
        """
        Returns a simplified status dictionary of the given cluster ID.
        If the cluster is in a terminated state, the reason is returned
        as the second element of the return tuple.
        """
        terminated_states = ["TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS"]
        termination_cause = None
        response = self._client.describe_cluster(ClusterId=cluster_id)
        steps_response = self._client.list_steps(ClusterId=cluster_id)
        steps = steps_response["Steps"]
        steps_list = []
        for s in steps:
            steps_list.append(s)
        output = {
            "name": response["Cluster"]["Name"],
            "state": response["Cluster"]["Status"]["State"],
            "steps": steps_list,
        }
        if output["state"] in terminated_states:
            termination_cause = response["Cluster"]["Status"]["StateChangeReason"][
                "Message"
            ]
        return output, termination_cause

    def run_steps(
        self,
        assembly_path: str,
        cluster_id: str,
        class_and_args: [(str, [str])],
        terminate_on_failure=True,
    ) -> [str]:
        """
        Run steps on a cluster which is running and waiting
        """
        if not isinstance(class_and_args, list):
            print(
                "Steps must be provided as a list.\nFor a single job, pass [job_tuple]"
            )
            return []
        steps = []
        step_names = []
        failure_op = "TERMINATE_CLUSTER"
        if not terminate_on_failure:
            failure_op = "CONTINUE"
        for pair in class_and_args:
            arg_list = [
                "spark-submit",
                "--packages",
                "org.apache.spark:spark-avro_2.11:2.4.4",
                "--deploy-mode",
                "cluster",
                "--master",
                "yarn",
                "--class",
                pair[0],
                assembly_path,
            ]
            arg_list.extend(str(arg) for arg in pair[1])
            steps.append(
                {
                    "Name": os.path.splitext(pair[0])[1][1:],
                    "ActionOnFailure": failure_op,
                    "HadoopJarStep": {"Jar": "command-runner.jar", "Args": arg_list},
                }
            )
        response = self._client.add_job_flow_steps(JobFlowId=cluster_id, Steps=steps)
        step_names.extend(response["StepIds"])

        return step_names
