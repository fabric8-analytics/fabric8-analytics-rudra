"""EMR Deployments."""

from rudra.deployments.emr_scripts.maven_emr import MavenEMR
from rudra.deployments.emr_scripts.npm_emr import NpmEMR
from rudra.deployments.emr_scripts.pypi_emr import PyPiEMR

__all__ = ['MavenEMR', 'NpmEMR', 'PyPiEMR']

# make all linters happy
assert MavenEMR is not None
assert NpmEMR is not None
assert PyPiEMR is not None
