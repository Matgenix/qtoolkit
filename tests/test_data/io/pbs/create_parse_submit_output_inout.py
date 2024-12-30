import json

import yaml

from qtoolkit.io.pbs import PBSIO

pbs_io = PBSIO()

mylist = []

# First case: invalid queue specified
return_code = 1
stdout = b""
stderr = b"qsub: Unknown queue\n"

sr = pbs_io.parse_submit_output(
    exit_code=return_code, stdout=stdout.decode(), stderr=stderr.decode()
)

a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

# Second case: successful submission
return_code = 0
stdout = b"24\n"
stderr = b""
sr = pbs_io.parse_submit_output(
    exit_code=return_code, stdout=stdout.decode(), stderr=stderr.decode()
)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)


with open("parse_submit_output_inout.yaml", "w") as f:
    yaml.dump(mylist, f, sort_keys=False)
