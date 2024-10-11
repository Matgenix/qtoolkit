import json

import yaml

from qtoolkit.io.sge import SGEIO

sge_io = SGEIO()

mylist = []

# First case: successful termination
return_code = 0
stdout = b""
stderr = b"qdel: job 267 deleted\n"

cr = sge_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)

# Second case: no job identification provided
return_code = 1
stdout = b""
stderr = b"qdel: No job id specified\n"

cr = sge_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)

# Third case: access/permission denied
return_code = 210
stdout = b""
stderr = b"qdel: job 1 access denied\n"

cr = sge_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)

# Fourth case: invalid job id
return_code = 1
stdout = b""
stderr = b"qdel: Invalid job id a\n"

cr = sge_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)

# Fifth case: job already completed
return_code = 0
stdout = b""
stderr = b"qdel: job 269 deleted\nqdel: job 269 already completed\n"

cr = sge_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)

# Sixth case: invalid job id specified
return_code = 0
stdout = b""
stderr = b"qdel: job 2675 deleted\nqdel: Invalid job id specified\n"

cr = sge_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)

with open("parse_cancel_output_inout.yaml", "w") as f:
    yaml.dump(mylist, f, sort_keys=False)
