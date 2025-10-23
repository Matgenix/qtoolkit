class TestLocalHost:
    def test_execute(self, tmp_dir):
        import os

        from qtoolkit.host.local import LocalHost

        host = LocalHost()
        host.execute(["touch", "somefile"])

        assert os.path.exists(os.path.join(tmp_dir, "somefile"))

    def test_mkdir(self, tmp_dir):
        import os

        from qtoolkit.host.local import LocalHost

        host = LocalHost()

        mydir = os.path.join(tmp_dir, "somedir")
        assert host.mkdir(mydir) is True

        assert host.mkdir(mydir, exist_ok=False) is False
        assert host.mkdir(mydir, exist_ok=True) is True

        assert os.path.exists(mydir)
        assert os.path.isdir(mydir)

        assert (
            host.mkdir(os.path.join(tmp_dir, "somedir2", "subdir"), recursive=False)
            is False
        )
        assert not os.path.exists(os.path.join(tmp_dir, "somedir2", "subdir"))
