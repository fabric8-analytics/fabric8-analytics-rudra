"""EMR Deployments."""

from rudra.deployments.emrs.maven_emr import MavenEMR
from rudra.deployments.emrs.npm_emr import NpmEMR
from rudra.deployments.emrs.pypi_emr import PyPiEMR

__all__ = [MavenEMR, NpmEMR, PyPiEMR]
