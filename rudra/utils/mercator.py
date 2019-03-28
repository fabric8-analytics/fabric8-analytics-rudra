"""Mercator: implementation of dependencies finder."""

from lxml import objectify
import re


class SimpleMercator:
    """SimpleMercator Implementation."""

    def __init__(self, content):
        """Initialize the SimpleMercator object."""
        con = content.encode() if isinstance(content, str) else content
        if not con:
            raise ValueError("Empty Content Error")
        try:
            self.root = objectify.fromstring(con.strip())
        except Exception:
            # Try to get the information from invalid pom
            self.root = objectify.fromstring(self.handle_corrupt_pom(con))

        if not hasattr(self, 'root'):
            raise ValueError("Unable to read pom file.")

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

    @staticmethod
    def handle_corrupt_pom(content):
        """Try to find the dependencies in corrupt/invalid pom."""
        con = "<p><dependencies>{}</dependencies></p>"
        dependencies_pattern = re.compile(r'<dependencies>(.*?)</dependencies>', flags=re.DOTALL)
        dependency_pattern = re.compile(r'<dependency>(.*?)</dependency>', flags=re.DOTALL)
        base_pattern = '<{tag}>(.*?)</{tag}>'

        # can not run regex on bytes like object
        content = content.decode() if not isinstance(content, str) else content
        # remove dependencyManagement
        content = re.sub(r'<dependencyManagement>(.*?)</dependencyManagement>',
                         '', content, flags=re.DOTALL)

        # Construct dependency
        con_dependency_obj_list = list()
        con_dependency_obj_pattern = """
                <dependency>
                    <groupId>{g}</groupId>
                    <artifactId>{a}</artifactId>
                    <scope>{s}</scope>
                </dependency>
        """

        for dependencies in dependencies_pattern.findall(content):
            for dependency in dependency_pattern.findall(dependencies):
                try:
                    aid = re.findall(base_pattern.format(tag='artifactId'),
                                     dependency, flags=re.DOTALL)[0]
                    gid = re.findall(base_pattern.format(tag='groupId'),
                                     dependency, flags=re.DOTALL)[0]
                    scope = re.findall(base_pattern.format(tag='scope'),
                                       dependency, flags=re.DOTALL) or 'compile'
                    con_dependency_obj_list.append(
                        con_dependency_obj_pattern.format(g=gid, a=aid, s=scope))
                except IndexError:
                    continue
        return con.format(''.join(con_dependency_obj_list))
