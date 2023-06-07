import pytest

try:
    import monty
except ModuleNotFoundError:
    monty = None


from qtoolkit.io.shell import ShellIO


class TestShellIO:
    @pytest.mark.skipif(monty is None, reason="monty is not installed")
    def test_msonable(self, test_utils):
        shell_io = ShellIO()
        assert test_utils.is_msonable(shell_io)

    def test_get_submit_cmd(self):
        shell_io = ShellIO(blocking=True)
        submit_cmd = shell_io.get_submit_cmd(script_file="myscript.sh")
        assert submit_cmd == "bash myscript.sh > stdout 2> stderr"
        shell_io = ShellIO(blocking=False)
        submit_cmd = shell_io.get_submit_cmd(script_file="myscript.sh")
        assert submit_cmd == "nohup bash myscript.sh > stdout 2> stderr & echo $!"
