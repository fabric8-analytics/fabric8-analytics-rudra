"""Mercator: implementation of dependencies finder."""

from lxml import objectify


class SimpleMercator:
    """SimpleMercator Implementation."""

    def __init__(self, content):
        """Initialize the SimpleMercator object."""
        con = content.encode() if isinstance(content, str) else content
        if not con:
            raise ValueError("Empty Content Error")
        self.root = objectify.fromstring(con.strip())

    def get_dependencies(self):
        """Get the list dependencies."""
        result = list()
        try:
            for dp in getattr(self.root.dependencies, 'dependency', list()):
                result.append(self.Dependency(dp))
        except AttributeError:
            pass  # dependencies does not exist in pom
        return result

    def __iter__(self):
        """Return the iterator of dictionaries."""
        return iter(self.get_dependencies())

    class Dependency:
        """Dependency class Implementation."""

        def __init__(self, dep):
            """Initialize Dependency object."""
            if not isinstance(dep, objectify.ObjectifiedElement):
                raise ValueError

            self.artifact_id = self.group_id = self.scope = None

            try:
                self.artifact_id = dep.artifactId
                self.group_id = dep.groupId
                self.scope = getattr(dep, 'scope', 'compile')
            except AttributeError:
                pass  # artifactId, groupId does not exist in pom

        def __iter__(self):
            """Iterate over attributes."""
            return iter(self.__dict__.items())
