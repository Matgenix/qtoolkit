# Changelog

## [v0.1.7](https://github.com/Matgenix/qtoolkit/tree/v0.1.7) (2026-01-07)

[Full Changelog](https://github.com/Matgenix/qtoolkit/compare/v0.1.6...v0.1.7)

**Closed issues:**

- qverbatim flag not being written out [\#56](https://github.com/Matgenix/qtoolkit/issues/56)
- gpus not part of slurm template [\#54](https://github.com/Matgenix/qtoolkit/issues/54)

**Merged pull requests:**

- Bump codecov/codecov-action from 3 to 5 [\#69](https://github.com/Matgenix/qtoolkit/pull/69) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/checkout from 3 to 6 [\#68](https://github.com/Matgenix/qtoolkit/pull/68) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/setup-python from 4 to 6 [\#67](https://github.com/Matgenix/qtoolkit/pull/67) ([dependabot[bot]](https://github.com/apps/dependabot))
- update release workflow [\#62](https://github.com/Matgenix/qtoolkit/pull/62) ([gpetretto](https://github.com/gpetretto))
- Documentation and docstrings [\#61](https://github.com/Matgenix/qtoolkit/pull/61) ([gpetretto](https://github.com/gpetretto))
- Fixing maxchars for macos + update of github workflow [\#60](https://github.com/Matgenix/qtoolkit/pull/60) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Option to run integration tests on selected containers [\#58](https://github.com/Matgenix/qtoolkit/pull/58) ([gpetretto](https://github.com/gpetretto))
- Raise if qstat command fails in PBS [\#57](https://github.com/Matgenix/qtoolkit/pull/57) ([gpetretto](https://github.com/gpetretto))
- Integration tests + various fixes [\#55](https://github.com/Matgenix/qtoolkit/pull/55) ([davidwaroquiers](https://github.com/davidwaroquiers))

## [v0.1.6](https://github.com/Matgenix/qtoolkit/tree/v0.1.6) (2025-01-20)

[Full Changelog](https://github.com/Matgenix/qtoolkit/compare/v0.1.5...v0.1.6)

**Closed issues:**

- Potential issue for jobs list in SGE [\#50](https://github.com/Matgenix/qtoolkit/issues/50)
- Confusing SLURM template variables [\#48](https://github.com/Matgenix/qtoolkit/issues/48)
- `process_placement` unused by `slurm.py`? [\#47](https://github.com/Matgenix/qtoolkit/issues/47)

**Merged pull requests:**

- Sanitization of job name and tests for PBS [\#53](https://github.com/Matgenix/qtoolkit/pull/53) ([gpetretto](https://github.com/gpetretto))
- fix parse\_jobs\_list\_output parsing issues with SGE [\#52](https://github.com/Matgenix/qtoolkit/pull/52) ([QuantumChemist](https://github.com/QuantumChemist))
- improve message for missing keys [\#49](https://github.com/Matgenix/qtoolkit/pull/49) ([gpetretto](https://github.com/gpetretto))
- Implementation of SGE interface [\#43](https://github.com/Matgenix/qtoolkit/pull/43) ([QuantumChemist](https://github.com/QuantumChemist))
- Same `ruff` linting as `jf-remote` [\#42](https://github.com/Matgenix/qtoolkit/pull/42) ([janosh](https://github.com/janosh))

## [v0.1.5](https://github.com/Matgenix/qtoolkit/tree/v0.1.5) (2024-08-09)

[Full Changelog](https://github.com/Matgenix/qtoolkit/compare/v0.1.4...v0.1.5)

**Merged pull requests:**

- Fix codecov upload in CI [\#46](https://github.com/Matgenix/qtoolkit/pull/46) ([ml-evs](https://github.com/ml-evs))
- Add simple test for QResources -\> slurm submission script  [\#45](https://github.com/Matgenix/qtoolkit/pull/45) ([ml-evs](https://github.com/ml-evs))
- Fix `mem_per_cpu` in `SlurmIO` not being passed as snake\_case [\#44](https://github.com/Matgenix/qtoolkit/pull/44) ([janosh](https://github.com/janosh))
- Correctly handle `float` in `(Slurm|PBS)IO._convert_time_to_str` [\#41](https://github.com/Matgenix/qtoolkit/pull/41) ([janosh](https://github.com/janosh))

## [v0.1.4](https://github.com/Matgenix/qtoolkit/tree/v0.1.4) (2024-03-19)

[Full Changelog](https://github.com/Matgenix/qtoolkit/compare/v0.1.3...v0.1.4)

**Closed issues:**

- Issue with SLURM RD state  [\#37](https://github.com/Matgenix/qtoolkit/issues/37)

**Merged pull requests:**

- Slurm and QResources updates [\#38](https://github.com/Matgenix/qtoolkit/pull/38) ([gpetretto](https://github.com/gpetretto))

## [v0.1.3](https://github.com/Matgenix/qtoolkit/tree/v0.1.3) (2024-02-05)

[Full Changelog](https://github.com/Matgenix/qtoolkit/compare/v0.1.2...v0.1.3)

**Closed issues:**

- Release workflow does not update changelog [\#35](https://github.com/Matgenix/qtoolkit/issues/35)
- Missing bits of documentation [\#34](https://github.com/Matgenix/qtoolkit/issues/34)

**Merged pull requests:**

- Update docs and release workflow [\#36](https://github.com/Matgenix/qtoolkit/pull/36) ([ml-evs](https://github.com/ml-evs))

## [v0.1.2](https://github.com/Matgenix/qtoolkit/tree/v0.1.2) (2024-02-05)

[Full Changelog](https://github.com/Matgenix/qtoolkit/compare/v0.1.1...v0.1.2)

**Closed issues:**

- Logo [\#21](https://github.com/Matgenix/qtoolkit/issues/21)

**Merged pull requests:**

- Add PyPI release workflow [\#33](https://github.com/Matgenix/qtoolkit/pull/33) ([ml-evs](https://github.com/ml-evs))
- Fix bug due to poor naming of QResources attribute [\#32](https://github.com/Matgenix/qtoolkit/pull/32) ([gpetretto](https://github.com/gpetretto))
- Fix README badge and warning [\#31](https://github.com/Matgenix/qtoolkit/pull/31) ([ml-evs](https://github.com/ml-evs))
- Better deserialization handling for QTKEnum [\#30](https://github.com/Matgenix/qtoolkit/pull/30) ([gpetretto](https://github.com/gpetretto))
- Unit tests [\#29](https://github.com/Matgenix/qtoolkit/pull/29) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Fix tests. Setup codecov [\#28](https://github.com/Matgenix/qtoolkit/pull/28) ([davidwaroquiers](https://github.com/davidwaroquiers))

## [v0.1.1](https://github.com/Matgenix/qtoolkit/tree/v0.1.1) (2023-10-09)

[Full Changelog](https://github.com/Matgenix/qtoolkit/compare/v0.1.0...v0.1.1)

**Closed issues:**

- Automated releases through GH actions [\#17](https://github.com/Matgenix/qtoolkit/issues/17)

**Merged pull requests:**

- Add CI docs build and deploy [\#27](https://github.com/Matgenix/qtoolkit/pull/27) ([ml-evs](https://github.com/ml-evs))
- Add logos [\#26](https://github.com/Matgenix/qtoolkit/pull/26) ([ml-evs](https://github.com/ml-evs))

## [v0.1.0](https://github.com/Matgenix/qtoolkit/tree/v0.1.0) (2023-10-06)

[Full Changelog](https://github.com/Matgenix/qtoolkit/compare/3658f911689f65f7a0caf8728de48c3b1e2d1f90...v0.1.0)

**Closed issues:**

- Problem in parse\_jobs\_list\_output in class ShellIO [\#22](https://github.com/Matgenix/qtoolkit/issues/22)
- `number_of_tasks` missing in slurm template [\#18](https://github.com/Matgenix/qtoolkit/issues/18)

**Merged pull requests:**

- Install build during build step [\#25](https://github.com/Matgenix/qtoolkit/pull/25) ([ml-evs](https://github.com/ml-evs))
- Preparing public release [\#24](https://github.com/Matgenix/qtoolkit/pull/24) ([ml-evs](https://github.com/ml-evs))
- Fix problem in parse\_jobs\_list\_output in class ShellIO [\#23](https://github.com/Matgenix/qtoolkit/pull/23) ([FabiPi3](https://github.com/FabiPi3))
- Updates for SlurmIO and ShellIO [\#20](https://github.com/Matgenix/qtoolkit/pull/20) ([gpetretto](https://github.com/gpetretto))
- Fix name for `ntasks` in slurm template [\#19](https://github.com/Matgenix/qtoolkit/pull/19) ([ml-evs](https://github.com/ml-evs))
- Go Live version [\#16](https://github.com/Matgenix/qtoolkit/pull/16) ([davidwaroquiers](https://github.com/davidwaroquiers))
- update shell io [\#15](https://github.com/Matgenix/qtoolkit/pull/15) ([gpetretto](https://github.com/gpetretto))
- Modify slurm template [\#13](https://github.com/Matgenix/qtoolkit/pull/13) ([gpetretto](https://github.com/gpetretto))
- Adding a shell io object [\#12](https://github.com/Matgenix/qtoolkit/pull/12) ([gpetretto](https://github.com/gpetretto))
- Documentation layout [\#11](https://github.com/Matgenix/qtoolkit/pull/11) ([davidwaroquiers](https://github.com/davidwaroquiers))
- split io and manager [\#7](https://github.com/Matgenix/qtoolkit/pull/7) ([gpetretto](https://github.com/gpetretto))
- WIP Remote host [\#5](https://github.com/Matgenix/qtoolkit/pull/5) ([davidwaroquiers](https://github.com/davidwaroquiers))
- typing [\#4](https://github.com/Matgenix/qtoolkit/pull/4) ([gpetretto](https://github.com/gpetretto))
- Commented test on documentation in testing.yaml. [\#3](https://github.com/Matgenix/qtoolkit/pull/3) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Fixed state mapping from slurm COMPLETED to standard QState DONE. [\#2](https://github.com/Matgenix/qtoolkit/pull/2) ([davidwaroquiers](https://github.com/davidwaroquiers))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
