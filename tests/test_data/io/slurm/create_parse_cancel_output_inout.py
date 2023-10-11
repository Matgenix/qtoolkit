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

cr = slurm_io.parse_cancel_output(exit_code=return_code, stdout=stdout, stderr=stderr)

a = {
    "parse_cancel_kwargs": json.dumps(
        {"exit_code": return_code, "stdout": stdout.decode(), "stderr": stderr.decode()}
    ),
    "cancel_result_ref": json.dumps(cr.as_dict()),
}
mylist.append(a)


with open("parse_cancel_output_inout.yaml", "w") as f:
    yaml.dump(mylist, f)
