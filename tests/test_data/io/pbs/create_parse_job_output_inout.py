import json

import yaml

from qtoolkit.io.pbs import PBSIO

pbs_io = PBSIO()

mylist = []

# First case: successful job parsing
return_code = 0
stdout = b"""Job Id: 14
    Job_Name = myscript_1
    Job_Owner = testu@f41a0fbae027
    resources_used.cpupercent = 0
    resources_used.cput = 00:00:00
    resources_used.mem = 0kb
    resources_used.ncpus = 1
    resources_used.vmem = 0kb
    resources_used.walltime = 00:00:00
    job_state = R
    queue = workq
    server = f41a0fbae027
    Checkpoint = u
    ctime = Sun Dec 29 20:13:12 2024
    Error_Path = f41a0fbae027:/home/testu/myscript_1.e14
    exec_host = f41a0fbae027/0
    exec_vnode = (f41a0fbae027:ncpus=1)
    Hold_Types = n
    Join_Path = n
    Keep_Files = n
    Mail_Points = a
    mtime = Sun Dec 29 20:13:14 2024
    Output_Path = f41a0fbae027:/home/testu/myscript_1.o14
    Priority = 0
    qtime = Sun Dec 29 20:13:12 2024
    Rerunable = True
    Resource_List.ncpus = 1
    Resource_List.nodect = 1
    Resource_List.nodes = 1:ppn=1
    Resource_List.place = scatter
    Resource_List.select = 1:ncpus=1
    Resource_List.walltime = 01:00:00
    stime = Sun Dec 29 20:13:12 2024
    session_id = 1534
    Shell_Path_List = /bin/bash
    jobdir = /home/testu
    substate = 42
    Variable_List = PBS_O_HOME=/home/testu,PBS_O_LANG=C.UTF-8,
    PBS_O_LOGNAME=testu,
    PBS_O_PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bi
    n:/usr/games:/usr/local/games:/snap/bin:/opt/pbs/bin,
    PBS_O_SHELL=/bin/bash,PBS_O_WORKDIR=/home/testu,PBS_O_SYSTEM=Linux,
    PBS_O_QUEUE=workq,PBS_O_HOST=f41a0fbae027
    comment = Job run at Sun Dec 29 at 20:13 on (f41a0fbae027:ncpus=1)
    etime = Sun Dec 29 20:13:12 2024
    run_count = 1
    Submit_arguments = test_submit.sh
    project = _pbs_project_default
    Submit_Host = f41a0fbae027
"""
stderr = b""
job = pbs_io.parse_job_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_job_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "job_ref": json.dumps(job.as_dict()),
}
mylist.append(a)


# Second case: empty stdout and stderr
return_code = 0
stdout = b""
stderr = b""
job = pbs_io.parse_job_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_job_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "job_ref": json.dumps(job.as_dict() if job is not None else None),
}
mylist.append(a)

with open("parse_job_output_inout.yaml", "w") as f:
    yaml.dump(mylist, f, sort_keys=False)
