import json

import yaml

from qtoolkit.io.sge import SGEIO

sge_io = SGEIO()

mylist = []

# First case: invalid queue specified
return_code = 1
stdout = b""
stderr = (
    b"qsub: Invalid queue specified: abcd\n"
    b"qsub: Job submission failed: Invalid queue name specified\n"
)

sr = sge_io.parse_submit_output(
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
stdout = b'Your job 24 ("submit.script") has been submitted\n'
stderr = b""
sr = sge_io.parse_submit_output(
    exit_code=return_code, stdout=stdout.decode(), stderr=stderr.decode()
)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

# Third case: another successful submission
return_code = 0
stdout = b'Your job 15 ("submit.script") has been submitted\n'
stderr = b""
sr = sge_io.parse_submit_output(
    exit_code=return_code, stdout=stdout.decode(), stderr=stderr.decode()
)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

# Fourth case: successful job allocation
return_code = 0
stdout = b'Your job 10 ("submit.script") has been submitted\n'
stderr = b""
sr = sge_io.parse_submit_output(
    exit_code=return_code, stdout=stdout.decode(), stderr=stderr.decode()
)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

# Fifth case: another successful job allocation
return_code = 0
stdout = b'Your job 124 ("submit.script") has been submitted\n'
stderr = b""
sr = sge_io.parse_submit_output(
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
