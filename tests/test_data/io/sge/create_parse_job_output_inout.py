import json

import yaml

from qtoolkit.core.exceptions import OutputParsingError
from qtoolkit.io.sge import SGEIO

sge_io = SGEIO()

mylist = []

# First case: successful job parsing
return_code = 0
stdout = b"""<?xml version='1.0'?>
<job_info>
    <job_list state="running">
        <JB_job_number>270</JB_job_number>
        <JB_name>submit.script</JB_name>
        <JB_owner>matgenix-dwa</JB_owner>
        <JB_group>matgenix-dwa</JB_group>
        <JB_account>(null)</JB_account>
        <state>r</state>
        <priority>0</priority>
        <JAT_start_time>2023-10-11T11:08:17</JAT_start_time>
        <queue_name>main.q</queue_name>
        <slots>1</slots>
        <tasks>1</tasks>
        <hard resource_list.h_rt>00:05:00</hard resource_list.h_rt>
        <hard resource_list.mem_free>96G</hard resource_list.mem_free>
    </job_list>
</job_info>
"""
stderr = b""
job = sge_io.parse_job_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_job_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "job_ref": json.dumps(job.as_dict()),
}
mylist.append(a)

# Second case: job parsing with invalid fields
return_code = 0
stdout = b"""<?xml version='1.0'?>
<job_info>
    <job_list state="running">
        <JB_job_number>270</JB_job_number>
        <JB_name>submit.script</JB_name>
        <JB_owner>matgenix-dwa</JB_owner>
        <JB_group>matgenix-dwa</JB_group>
        <JB_account>(null)</JB_account>
        <state>r</state>
        <priority>0</priority>
        <JAT_start_time>2023-10-11T11:08:17</JAT_start_time>
        <queue_name>main.q</queue_name>
        <slots>a</slots>
        <tasks>1</tasks>
        <hard resource_list.h_rt>a</hard resource_list.h_rt>
        <hard resource_list.mem_free>96G</hard resource_list.mem_free>
    </job_list>
</job_info>
"""
stderr = b""
try:
    job = sge_io.parse_job_output(exit_code=return_code, stdout=stdout, stderr=stderr)
    job_dict = job.as_dict()
except OutputParsingError as e:
    job_dict = {"error": str(e)}
a = {
    "parse_job_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "job_ref": json.dumps(job_dict),
}
mylist.append(a)

# Third case: empty stdout and stderr
return_code = 0
stdout = b""
stderr = b""
job = sge_io.parse_job_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_job_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "job_ref": json.dumps(job.as_dict() if job is not None else None),
}
mylist.append(a)

with open("parse_job_output_inout.yaml", "w") as f:
    yaml.dump(mylist, f, sort_keys=False)
