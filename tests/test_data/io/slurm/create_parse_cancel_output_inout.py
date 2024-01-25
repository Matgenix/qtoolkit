import json

import yaml

from qtoolkit.io.slurm import SlurmIO

slurm_io = SlurmIO()

mylist = []

return_code = 0
stdout = b""
stderr = b"scancel: Terminating job 267\n"

cr = slurm_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)


return_code = 1
stdout = b""
stderr = b"scancel: error: No job identification provided\n"

cr = slurm_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)


return_code = 210
stdout = b""
stderr = b"scancel: error: Kill job error on job id 1: Access/permission denied\n"

cr = slurm_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)


return_code = 1
stdout = b""
stderr = b"scancel: error: Invalid job id a\n"

cr = slurm_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)


return_code = 0
stdout = b""
stderr = b"scancel: Terminating job 269\nscancel: error: Kill job error on job id 269: Job/step already completing or completed\n"

cr = slurm_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)


return_code = 0
stdout = b""
stderr = b"scancel: Terminating job 2675\nscancel: error: Kill job error on job id 2675: Invalid job id specified\n"

cr = slurm_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)


with open("parse_cancel_output_inout.yaml", "w") as f:
    yaml.dump(mylist, f, sort_keys=False)
