import json

import yaml

from qtoolkit.io.slurm import SlurmIO

slurm_io = SlurmIO()

mylist = []

return_code = 0
stdout = b"JobId=270 JobName=submit.script UserId=matgenix-dwa(1001) GroupId=matgenix-dwa(1002) MCS_label=N/A Priority=4294901497 Nice=0 Account=(null) QOS=normal JobState=COMPLETED Reason=None Dependency=(null) Requeue=1 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0 RunTime=00:05:00 TimeLimit=UNLIMITED TimeMin=N/A SubmitTime=2023-10-11T11:08:17 EligibleTime=2023-10-11T11:08:17 AccrueTime=2023-10-11T11:08:17 StartTime=2023-10-11T11:08:17 EndTime=2023-10-11T11:13:17 Deadline=N/A SuspendTime=None SecsPreSuspend=0 LastSchedEval=2023-10-11T11:08:17 Scheduler=Main Partition=main AllocNode:Sid=matgenixdb:2556938 ReqNodeList=(null) ExcNodeList=(null) NodeList=matgenixdb BatchHost=matgenixdb NumNodes=1 NumCPUs=1 NumTasks=1 CPUs/Task=1 ReqB:S:C:T=0:0:*:* TRES=cpu=1,mem=96G,node=1,billing=1 Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=* MinCPUsNode=1 MinMemoryNode=0 MinTmpDiskNode=0 Features=(null) DelayBoot=00:00:00 OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null) Command=/home/matgenix-dwa/software/qtoolkit/tests/test_data/io/slurm/submit.script WorkDir=/home/matgenix-dwa/software/qtoolkit/tests/test_data/io/slurm StdErr=/home/matgenix-dwa/software/qtoolkit/tests/test_data/io/slurm/slurm-270.out StdIn=/dev/null StdOut=/home/matgenix-dwa/software/qtoolkit/tests/test_data/io/slurm/slurm-270.out Power= \n"
stderr = b""
job = slurm_io.parse_job_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_job_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "job_ref": json.dumps(job.as_dict()),
}
mylist.append(a)


with open("parse_job_output_inout.yaml", "w") as f:
    yaml.dump(mylist, f, sort_keys=False)
