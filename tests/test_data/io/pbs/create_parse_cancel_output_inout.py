import json

import yaml

from qtoolkit.io.pbs import PBSIO

pbs_io = PBSIO()

mylist = []

# First case: successful termination
return_code = 0
stdout = b""
stderr = b""

cr = pbs_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

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
stderr = b"""usage:
    qdel [-W force|suppress_email=X] [-x] job_identifier...
    qdel --version
"""

cr = pbs_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

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
stderr = b"qdel: Unauthorized Request  210\n"

cr = pbs_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

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
stderr = b"qdel: illegally formed job identifier: a\n"

cr = pbs_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)

# Fifth case: job already completed
return_code = 1
stdout = b""
stderr = b"qdel: Job has finished 8\n"

cr = pbs_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)

# Sixth case: unkwnown job id
return_code = 1
stdout = b""
stderr = b"qdel: Unknown Job Id 120\n"

cr = pbs_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)

with open("parse_cancel_output_inout.yaml", "w") as f:
    yaml.dump(mylist, f, sort_keys=False)
