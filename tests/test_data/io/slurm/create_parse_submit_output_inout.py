import json

import yaml

from qtoolkit.io.slurm import SlurmIO

slurm_io = SlurmIO()

mylist = []

return_code = 1
stdout = b""
stderr = (
    b"sbatch: error: invalid partition specified: abcd\n"
    b"sbatch: error: Batch job submission failed: Invalid partition name specified\n"
)

sr = slurm_io.parse_submit_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

return_code = 0
stdout = b"Submitted batch job 24\n"
stderr = b""
sr = slurm_io.parse_submit_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

return_code = 0
stdout = b"submitted batch job 15\n"
stderr = b""
sr = slurm_io.parse_submit_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

return_code = 0
stdout = b"Granted job allocation 10\n"
stderr = b""
sr = slurm_io.parse_submit_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

return_code = 0
stdout = b"granted job allocation 124\n"
stderr = b""
sr = slurm_io.parse_submit_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

return_code = 0
stdout = b"sbatch: Submitted batch job 24\n"
stderr = b""
sr = slurm_io.parse_submit_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

return_code = 0
stdout = b"sbatch: submitted batch job 15\n"
stderr = b""
sr = slurm_io.parse_submit_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

return_code = 0
stdout = b"salloc: Granted job allocation 10\n"
stderr = b""
sr = slurm_io.parse_submit_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)

return_code = 0
stdout = b"salloc: granted job allocation 124\n"
stderr = b""
sr = slurm_io.parse_submit_output(exit_code=return_code, stdout=stdout, stderr=stderr)
a = {
    "parse_submit_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "submission_result_ref": json.dumps(sr.as_dict()),
}
mylist.append(a)


with open("parse_submit_output_inout.yaml", "w") as f:
    yaml.dump(mylist, f, sort_keys=False)
